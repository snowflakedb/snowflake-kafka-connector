/*
 * Copyright (c) 2026 Snowflake Computing Inc. All rights reserved.
 */

package com.snowflake.kafka.connector.internal.schemaevolution;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Streams;
import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.internal.SnowflakeErrors;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Resolves table schema from Kafka SinkRecord. Supports both schema-ful (Avro/Protobuf) and
 * schema-less (JSON) records.
 *
 * <p>Includes the convertToJson logic (originally from RecordService) for type inference.
 */
public class TableSchemaResolver {

  private static final Logger LOGGER = LoggerFactory.getLogger(TableSchemaResolver.class);

  private final ColumnTypeMapper columnTypeMapper;

  // ---- Constants from RecordService ----

  private static final ConcurrentHashMap<Class<?>, Optional<Schema.Type>> SCHEMA_TYPE_CACHE =
      new ConcurrentHashMap<>();

  private static final ThreadLocal<SimpleDateFormat> ISO_DATE_TIME_FORMAT =
      ThreadLocal.withInitial(
          () -> {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
            sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
            return sdf;
          });

  private static final ThreadLocal<SimpleDateFormat> TIME_FORMAT =
      ThreadLocal.withInitial(() -> new SimpleDateFormat("HH:mm:ss.SSSZ"));

  private static final ThreadLocal<SimpleDateFormat> TIME_FORMAT_STREAMING =
      ThreadLocal.withInitial(() -> new SimpleDateFormat("HH:mm:ss.SSSXXX"));

  private static final int MAX_SNOWFLAKE_NUMBER_PRECISION = 38;

  public TableSchemaResolver(ColumnTypeMapper columnTypeMapper) {
    this.columnTypeMapper = columnTypeMapper;
  }

  public TableSchemaResolver() {
    this(new SnowflakeColumnTypeMapper());
  }

  /**
   * With the list of columns, collect their data types from either the schema or the data itself
   *
   * @param record the sink record that contains the schema and actual data
   * @param columnsToInclude the names of the columns to include in the schema
   * @return a Map object where the key is column name and value is ColumnInfos
   */
  public TableSchema resolveTableSchemaFromRecord(
      SinkRecord record, List<String> columnsToInclude) {
    if (columnsToInclude == null || columnsToInclude.isEmpty()) {
      return new TableSchema(ImmutableMap.of());
    }

    Set<String> columnNamesSet = new HashSet<>(columnsToInclude);

    if (hasSchema(record)) {
      return getTableSchemaFromRecordSchema(record, columnNamesSet);
    } else {
      return getTableSchemaFromJson(record, columnNamesSet);
    }
  }

  private boolean hasSchema(SinkRecord record) {
    return record.valueSchema() != null
        && record.valueSchema().fields() != null
        && !record.valueSchema().fields().isEmpty();
  }

  private TableSchema getTableSchemaFromRecordSchema(
      SinkRecord record, Set<String> columnNamesSet) {
    JsonNode recordNode = convertToJson(record.valueSchema(), record.value(), true);
    Map<String, ColumnInfos> schemaMap = getFullSchemaMapFromRecord(record);
    Map<Boolean, List<ColumnValuePair>> columnsWitValue =
        Streams.stream(recordNode.fields())
            .map(ColumnValuePair::from)
            .filter(pair -> columnNamesSet.contains(pair.getQuotedColumnName()))
            .collect(
                Collectors.partitioningBy((pair -> schemaMap.containsKey(pair.getColumnName()))));

    List<ColumnValuePair> notFoundFieldsInSchema = columnsWitValue.get(false);
    List<ColumnValuePair> foundFieldsInSchema = columnsWitValue.get(true);

    if (!notFoundFieldsInSchema.isEmpty()) {
      throw SnowflakeErrors.ERROR_5022.getException(
          "Columns not found in schema: "
              + notFoundFieldsInSchema.stream()
                  .map(ColumnValuePair::getColumnName)
                  .collect(Collectors.toList())
              + ", schemaMap: "
              + schemaMap);
    }
    Map<String, ColumnInfos> columnsInferredFromSchema =
        foundFieldsInSchema.stream()
            .map(
                pair ->
                    Maps.immutableEntry(
                        Utils.quoteNameIfNeeded(pair.getQuotedColumnName()),
                        schemaMap.get(pair.getColumnName())))
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey, Map.Entry::getValue, (oldValue, newValue) -> newValue));
    return new TableSchema(columnsInferredFromSchema);
  }

  private TableSchema getTableSchemaFromJson(SinkRecord record, Set<String> columnNamesSet) {
    JsonNode recordNode = convertToJson(record.valueSchema(), record.value(), true);
    Map<String, ColumnInfos> columnsInferredFromJson =
        Streams.stream(recordNode.fields())
            .map(ColumnValuePair::from)
            .filter(pair -> columnNamesSet.contains(pair.getQuotedColumnName()))
            .map(
                pair ->
                    Maps.immutableEntry(
                        pair.getQuotedColumnName(),
                        new ColumnInfos(inferDataTypeFromJsonObject(pair.getJsonNode()))))
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey, Map.Entry::getValue, (oldValue, newValue) -> newValue));
    return new TableSchema(columnsInferredFromJson);
  }

  /**
   * Given a SinkRecord, get the schema information from it
   *
   * @param record the sink record that contains the schema and actual data
   * @return a Map object where the key is column name and value is ColumnInfos
   */
  private Map<String, ColumnInfos> getFullSchemaMapFromRecord(SinkRecord record) {
    Map<String, ColumnInfos> schemaMap = new HashMap<>();
    Schema schema = record.valueSchema();
    if (schema != null && schema.fields() != null) {
      for (Field field : schema.fields()) {
        String columnType =
            columnTypeMapper.mapToColumnType(field.schema().type(), field.schema().name());
        LOGGER.info(
            "Got the data type for field:{}, schemaName:{}, schemaDoc: {} kafkaType:{},"
                + " columnType:{}",
            field.name(),
            field.schema().name(),
            field.schema().doc(),
            field.schema().type(),
            columnType);

        schemaMap.put(field.name(), new ColumnInfos(columnType, field.schema().doc()));
      }
    }
    return schemaMap;
  }

  /** Try to infer the data type from the data */
  private String inferDataTypeFromJsonObject(JsonNode value) {
    Schema.Type schemaType = columnTypeMapper.mapJsonNodeTypeToKafkaType(value);
    if (schemaType == null) {
      // only when the type of the value is unrecognizable for JAVA
      throw SnowflakeErrors.ERROR_5021.getException("class: " + value.getClass());
    }
    // Passing null to schemaName when there is no schema information
    return columnTypeMapper.mapToColumnType(schemaType);
  }

  // ---- convertToJson ----

  /**
   * Convert this object, in the org.apache.kafka.connect.data format, into a JSON object, returning
   * the converted object.
   *
   * @param schema schema of the object
   * @param logicalValue object to be converted
   * @param isStreaming indicates whether this is part of snowpipe streaming
   * @return a JsonNode of the object
   */
  static JsonNode convertToJson(Schema schema, Object logicalValue, boolean isStreaming) {
    if (logicalValue == null) {
      if (schema
          == null) // Any schema is valid and we don't have a default, so treat this as an optional
        // schema
        return null;
      if (schema.defaultValue() != null)
        return convertToJson(schema, schema.defaultValue(), isStreaming);
      if (schema.isOptional()) return JsonNodeFactory.instance.nullNode();
      throw SnowflakeErrors.ERROR_5015.getException(
          "Conversion error: null value for field that is required and has no default value");
    }

    Object value = logicalValue;
    try {
      final Schema.Type schemaType;
      if (schema == null) {
        Optional<Schema.Type> cachedType =
            SCHEMA_TYPE_CACHE.computeIfAbsent(
                value.getClass(), clazz -> Optional.ofNullable(ConnectSchema.schemaType(clazz)));
        if (cachedType.isPresent()) {
          schemaType = cachedType.get();
        } else {
          if (value instanceof java.util.Date) {
            schema = Timestamp.SCHEMA;
            schemaType = Schema.Type.INT64;
          } else {
            throw SnowflakeErrors.ERROR_5015.getException(
                "Java class " + value.getClass() + " does not have corresponding schema type.");
          }
        }
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
            return JsonNodeFactory.instance.textNode(
                ISO_DATE_TIME_FORMAT.get().format((java.util.Date) value));
          }
          if (schema != null && Time.LOGICAL_NAME.equals(schema.name())) {
            ThreadLocal<SimpleDateFormat> format =
                isStreaming ? TIME_FORMAT_STREAMING : TIME_FORMAT;
            return JsonNodeFactory.instance.textNode(format.get().format((java.util.Date) value));
          }
          return JsonNodeFactory.instance.numberNode((Integer) value);
        case INT64:
          if (schema != null && Timestamp.LOGICAL_NAME.equals(schema.name())) {
            // Snowflake does not support inserting numbers into timestamp columns, but it does
            // support strings
            return JsonNodeFactory.instance.textNode(
                String.valueOf(Timestamp.fromLogical(schema, (java.util.Date) value)));
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
          if (schema != null && Decimal.LOGICAL_NAME.equals(schema.name())) {
            BigDecimal bigDecimalValue = (BigDecimal) value;
            if (bigDecimalValue.precision() > MAX_SNOWFLAKE_NUMBER_PRECISION) {
              // in order to prevent losing precision, convert this value to text
              return JsonNodeFactory.instance.textNode(bigDecimalValue.toString());
            }
            return JsonNodeFactory.instance.numberNode(bigDecimalValue);
          }

          byte[] valueArr = null;
          if (value instanceof byte[]) valueArr = (byte[]) value;
          else if (value instanceof ByteBuffer) {
            ByteBuffer byteBufferValue = (ByteBuffer) value;
            if (byteBufferValue.hasArray()) valueArr = ((ByteBuffer) value).array();
            else {
              // If the byte buffer is read only, make a copy of the buffer then access the byte
              // array.
              ByteBuffer clone = ByteBuffer.allocate(byteBufferValue.capacity());
              byteBufferValue.rewind();
              clone.put(byteBufferValue);
              byteBufferValue.rewind();
              clone.flip();
              valueArr = clone.array();
            }
          }

          if (valueArr == null)
            throw SnowflakeErrors.ERROR_5015.getException(
                "Invalid type for bytes type: " + value.getClass());

          return JsonNodeFactory.instance.binaryNode(valueArr);

        case ARRAY:
          {
            Collection collection = (Collection) value;
            ArrayNode list = JsonNodeFactory.instance.arrayNode();
            for (Object elem : collection) {
              Schema valueSchema = schema == null ? null : schema.valueSchema();
              JsonNode fieldValue = convertToJson(valueSchema, elem, isStreaming);
              list.add(fieldValue);
            }
            return list;
          }
        case MAP:
          {
            Map<?, ?> map = (Map<?, ?>) value;
            // If true, using string keys and JSON object; if false, using non-string keys and
            // Array-encoding
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
              objectMode =
                  (schema.keySchema() != null && schema.keySchema().type() == Schema.Type.STRING);
            }
            ObjectNode obj = null;
            ArrayNode list = null;
            if (objectMode) obj = JsonNodeFactory.instance.objectNode();
            else list = JsonNodeFactory.instance.arrayNode();
            for (Map.Entry<?, ?> entry : map.entrySet()) {
              Schema keySchema = schema == null ? null : schema.keySchema();
              Schema valueSchema = schema == null ? null : schema.valueSchema();
              JsonNode mapKey = convertToJson(keySchema, entry.getKey(), isStreaming);
              JsonNode mapValue = convertToJson(valueSchema, entry.getValue(), isStreaming);

              if (objectMode) obj.set(mapKey.asText(), mapValue);
              else list.add(JsonNodeFactory.instance.arrayNode().add(mapKey).add(mapValue));
            }
            return objectMode ? obj : list;
          }
        case STRUCT:
          {
            Struct struct = (Struct) value;
            if (struct.schema() != schema)
              throw SnowflakeErrors.ERROR_5015.getException("Mismatching schema.");
            ObjectNode obj = JsonNodeFactory.instance.objectNode();
            for (Field field : schema.fields()) {
              obj.set(field.name(), convertToJson(field.schema(), struct.get(field), isStreaming));
            }
            return obj;
          }
      }

      throw SnowflakeErrors.ERROR_5015.getException("Couldn't convert " + value + " to JSON.");
    } catch (ClassCastException e) {
      throw SnowflakeErrors.ERROR_5015.getException(
          "Invalid type for " + schema.type() + ": " + value.getClass());
    }
  }

  // ---- ColumnValuePair ----

  private static class ColumnValuePair {
    private final String columnName;
    private final String quotedColumnName;
    private final JsonNode jsonNode;

    public static ColumnValuePair from(Map.Entry<String, JsonNode> field) {
      return new ColumnValuePair(field.getKey(), field.getValue());
    }

    private ColumnValuePair(String columnName, JsonNode jsonNode) {
      this.columnName = columnName;
      this.quotedColumnName = Utils.quoteNameIfNeeded(columnName);
      this.jsonNode = jsonNode;
    }

    public String getColumnName() {
      return columnName;
    }

    public String getQuotedColumnName() {
      return quotedColumnName;
    }

    public JsonNode getJsonNode() {
      return jsonNode;
    }
  }
}
