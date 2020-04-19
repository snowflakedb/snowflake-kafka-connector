package com.snowflake.kafka.connector.records;

import com.snowflake.kafka.connector.internal.SnowflakeErrors;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.JsonNode;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.ObjectMapper;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.node.JsonNodeFactory;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.node.ArrayNode;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.DataException;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Map;
import java.util.TimeZone;

public class SnowflakeRecordContent
{

  private static ObjectMapper MAPPER = new ObjectMapper();
  public static int NON_AVRO_SCHEMA = -1;
  private final JsonNode[] content;
  private final byte[] brokenData;
  private int schemaID;
  private boolean isBroken;

  public static final SimpleDateFormat ISO_DATE_FORMAT= new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
  public static final SimpleDateFormat TIME_FORMAT= new SimpleDateFormat("HH:mm:ss.SSSZ");
  static{
    ISO_DATE_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"));;
  }


  /**
   * constructor for null value
   */
  public SnowflakeRecordContent()
  {
    content = new JsonNode[1];
    content[0] = MAPPER.createObjectNode();
    brokenData = null;
  }

  /**
   * constructor for native json converter
   * @param schema schema of the object
   * @param data object produced by native avro/json converters
   */
  public SnowflakeRecordContent(Schema schema, Object data)
  {
    this.content = new JsonNode[1];
    this.schemaID = NON_AVRO_SCHEMA;
    try
    {
      this.content[0] = convertToJson(schema, data);
    } catch (DataException e)
    {
      this.isBroken = true;
      this.brokenData = data.toString().getBytes();
      return;
    }
    this.isBroken = false;
    this.brokenData = null;
  }

  /**
   * Convert this object, in the org.apache.kafka.connect.data format, into a JSON object, returning both the schema
   * and the converted object.
   * @param schema schema of the object
   * @param logicalValue object to be converted
   * @return a JsonNode of the object
   */
  private static JsonNode convertToJson(Schema schema, Object logicalValue) {
    if (logicalValue == null) {
      if (schema == null) // Any schema is valid and we don't have a default, so treat this as an optional schema
        return null;
      if (schema.defaultValue() != null)
        return convertToJson(schema, schema.defaultValue());
      if (schema.isOptional())
        return JsonNodeFactory.instance.nullNode();
      throw new DataException("Conversion error: null value for field that is required and has no default value");
    }

    Object value = logicalValue;
    try {
      final Schema.Type schemaType;
      if (schema == null) {
        schemaType = ConnectSchema.schemaType(value.getClass());
        if (schemaType == null)
          throw new DataException("Java class " + value.getClass() + " does not have corresponding schema type.");
      } else {
        schemaType = schema.type();
      }
      switch (schemaType) {
        case INT8:
          return JsonNodeFactory.instance.numberNode((Byte) value);
        case INT16:
          return JsonNodeFactory.instance.numberNode((Short) value);
        case INT32:
          if (schema != null && Date.LOGICAL_NAME.equals(schema.name())) {
            return JsonNodeFactory.instance.textNode(ISO_DATE_FORMAT.format((java.util.Date) value));
          }
          if (schema != null && Time.LOGICAL_NAME.equals(schema.name())) {
            return JsonNodeFactory.instance.textNode(TIME_FORMAT.format((java.util.Date) value));
          }
          return JsonNodeFactory.instance.numberNode((Integer) value);
        case INT64:
          String schemaName = schema.name();
          if(Timestamp.LOGICAL_NAME.equals(schemaName)){
            return JsonNodeFactory.instance.numberNode(Timestamp.fromLogical(schema, (java.util.Date) value));
          }
          return JsonNodeFactory.instance.numberNode((Long) value);
        case FLOAT32:
          return JsonNodeFactory.instance.numberNode((Float) value);
        case FLOAT64:
          return JsonNodeFactory.instance.numberNode((Double) value);
        case BOOLEAN:
          return JsonNodeFactory.instance.booleanNode((Boolean) value);
        case STRING:
          CharSequence charSeq = (CharSequence) value;
          return JsonNodeFactory.instance.textNode(charSeq.toString());
        case BYTES:
          if (Decimal.LOGICAL_NAME.equals(schema.name())) {
            return JsonNodeFactory.instance.numberNode((BigDecimal) value);
          }

          byte[] valueArr = null;
          if (value instanceof byte[])
            valueArr = (byte[]) value;
          else if (value instanceof ByteBuffer)
            valueArr = ((ByteBuffer) value).array();

          if (valueArr == null)
            throw new DataException("Invalid type for bytes type: " + value.getClass());

          return JsonNodeFactory.instance.binaryNode(valueArr);

        case ARRAY: {
          Collection collection = (Collection) value;
          ArrayNode list = JsonNodeFactory.instance.arrayNode();
          for (Object elem : collection) {
            Schema valueSchema = schema == null ? null : schema.valueSchema();
            JsonNode fieldValue = convertToJson(valueSchema, elem);
            list.add(fieldValue);
          }
          return list;
        }
        case MAP: {
          Map<?, ?> map = (Map<?, ?>) value;
          // If true, using string keys and JSON object; if false, using non-string keys and Array-encoding
          boolean objectMode;
          if (schema == null) {
            objectMode = true;
            for (Map.Entry<?, ?> entry : map.entrySet()) {
              if (!(entry.getKey() instanceof String)) {
                objectMode = false;
                break;
              }
            }
          } else {
            objectMode = schema.keySchema().type() == Schema.Type.STRING;
          }
          ObjectNode obj = null;
          ArrayNode list = null;
          if (objectMode)
            obj = JsonNodeFactory.instance.objectNode();
          else
            list = JsonNodeFactory.instance.arrayNode();
          for (Map.Entry<?, ?> entry : map.entrySet()) {
            Schema keySchema = schema == null ? null : schema.keySchema();
            Schema valueSchema = schema == null ? null : schema.valueSchema();
            JsonNode mapKey = convertToJson(keySchema, entry.getKey());
            JsonNode mapValue = convertToJson(valueSchema, entry.getValue());

            if (objectMode)
              obj.set(mapKey.asText(), mapValue);
            else
              list.add(JsonNodeFactory.instance.arrayNode().add(mapKey).add(mapValue));
          }
          return objectMode ? obj : list;
        }
        case STRUCT: {
          Struct struct = (Struct) value;
          if (struct.schema() != schema)
            throw new DataException("Mismatching schema.");
          ObjectNode obj = JsonNodeFactory.instance.objectNode();
          for (Field field : schema.fields()) {
            obj.set(field.name(), convertToJson(field.schema(), struct.get(field)));
          }
          return obj;
        }
      }

      throw new DataException("Couldn't convert " + value + " to JSON.");
    } catch (ClassCastException e) {
      throw new DataException("Invalid type for " + schema.type() + ": " + value.getClass());
    }
  }


  /**
   * constructor for json converter
   * @param data json node
   */
  SnowflakeRecordContent(JsonNode data)
  {
    this.content = new JsonNode[1];
    this.content[0] = data;
    this.isBroken = false;
    this.schemaID = NON_AVRO_SCHEMA;
    this.brokenData = null;
  }

  /**
   * constructor for avro converter without schema registry
   * @param data json node array
   */
  SnowflakeRecordContent(JsonNode[] data)
  {
    this.content = data;
    this.isBroken = false;
    this.schemaID = NON_AVRO_SCHEMA;
    this.brokenData = null;
  }

  /**
   * constructor for broken record
   * @param data broken record
   */
  SnowflakeRecordContent(byte[] data)
  {
    this.brokenData = data;
    this.isBroken = true;
    this.schemaID = NON_AVRO_SCHEMA;
    this.content = null;
  }

  /**
   * constructor for avro converter
   * @param data json node
   * @param schemaID schema id
   */
  SnowflakeRecordContent(JsonNode data, int schemaID)
  {
    this(data);
    this.schemaID = schemaID;
  }

  /**
   *
   * @return true is record is broken
   */
  public boolean isBroken()
  {
    return this.isBroken;
  }

  /**
   *
   * @return bytes array represents broken data
   */
  public byte[] getBrokenData()
  {
    if(!isBroken)
    {
      throw SnowflakeErrors.ERROR_5011.getException();
    }
    assert this.brokenData != null;
    return this.brokenData.clone();
  }

  /**
   * @return schema id, -1 if not available
   */
  int getSchemaID()
  {
    return schemaID;
  }

  public JsonNode[] getData()
  {
    if(isBroken)
    {
      throw SnowflakeErrors.ERROR_5012.getException();
    }
    assert content != null;
    return content.clone();
  }
}
