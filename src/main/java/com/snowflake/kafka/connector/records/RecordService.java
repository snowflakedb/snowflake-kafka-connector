/*
 * Copyright (c) 2019 Snowflake Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.snowflake.kafka.connector.records;

import com.snowflake.kafka.connector.internal.Logging;
import com.snowflake.kafka.connector.internal.SnowflakeErrors;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.TimeZone;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.JsonNode;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.ObjectMapper;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.node.ArrayNode;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.node.JsonNodeFactory;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.sink.SinkRecord;

public class RecordService extends Logging {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  // deleted private to use these values in test
  static final String OFFSET = "offset";
  static final String TOPIC = "topic";
  static final String PARTITION = "partition";
  static final String KEY = "key";
  static final String CONTENT = "content";
  static final String META = "meta";
  static final String SCHEMA_ID = "schema_id";
  private static final String KEY_SCHEMA_ID = "key_schema_id";
  static final String HEADERS = "headers";

  public static final SimpleDateFormat ISO_DATE_FORMAT =
      new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
  public static final SimpleDateFormat TIME_FORMAT = new SimpleDateFormat("HH:mm:ss.SSSZ");
  static final int MAX_SNOWFLAKE_NUMBER_PRECISION = 38;

  static {
    ISO_DATE_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"));
  }

  // This class is designed to work with empty metadata config map
  private SnowflakeMetadataConfig metadataConfig = new SnowflakeMetadataConfig();

  /**
   * process records output JSON format: { "meta": { "offset": 123, "topic": "topic name",
   * "partition": 123, "key":"key name" } "content": "record content" }
   *
   * <p>create a JsonRecordService instance
   */
  public RecordService() {}

  public void setMetadataConfig(SnowflakeMetadataConfig metadataConfigIn) {
    metadataConfig = metadataConfigIn;
  }

  /**
   * process given SinkRecord, only support snowflake converters
   *
   * @param record SinkRecord
   * @return a record string, already to output
   */
  public String processRecord(SinkRecord record) {
    if (record.value() == null || record.valueSchema() == null) {
      throw SnowflakeErrors.ERROR_5016.getException();
    }
    if (!record.valueSchema().name().equals(SnowflakeJsonSchema.NAME)) {
      throw SnowflakeErrors.ERROR_0009.getException();
    }
    if (!(record.value() instanceof SnowflakeRecordContent)) {
      throw SnowflakeErrors.ERROR_0010.getException(
          "Input record should be SnowflakeRecordContent object");
    }

    SnowflakeRecordContent valueContent = (SnowflakeRecordContent) record.value();

    ObjectNode meta = MAPPER.createObjectNode();
    if (metadataConfig.topicFlag) {
      meta.put(TOPIC, record.topic());
    }
    if (metadataConfig.offsetAndPartitionFlag) {
      meta.put(OFFSET, record.kafkaOffset());
      meta.put(PARTITION, record.kafkaPartition());
    }

    // ignore if no timestamp
    if (record.timestampType() != TimestampType.NO_TIMESTAMP_TYPE
        && metadataConfig.createtimeFlag) {
      meta.put(record.timestampType().name, record.timestamp());
    }

    // include schema id if using avro with schema registry
    if (valueContent.getSchemaID() != SnowflakeRecordContent.NON_AVRO_SCHEMA) {
      meta.put(SCHEMA_ID, valueContent.getSchemaID());
    }

    putKey(record, meta);

    if (!record.headers().isEmpty()) {
      meta.set(HEADERS, parseHeaders(record.headers()));
    }

    StringBuilder buffer = new StringBuilder();
    for (JsonNode node : valueContent.getData()) {
      ObjectNode data = MAPPER.createObjectNode();
      data.set(CONTENT, node);
      if (metadataConfig.allFlag) {
        data.set(META, meta);
      }
      buffer.append(data.toString());
    }
    return buffer.toString();
  }

  void putKey(SinkRecord record, ObjectNode meta) {
    if (record.key() == null) {
      return;
    }

    if (record.keySchema().toString().equals(Schema.STRING_SCHEMA.toString())) {
      meta.put(KEY, record.key().toString());
    } else if (SnowflakeJsonSchema.NAME.equals(record.keySchema().name())) {
      if (!(record.key() instanceof SnowflakeRecordContent)) {
        throw SnowflakeErrors.ERROR_0010.getException(
            "Input record key should be SnowflakeRecordContent object if key schema is"
                + " SNOWFLAKE_JSON_SCHEMA");
      }

      SnowflakeRecordContent keyContent = (SnowflakeRecordContent) record.key();

      JsonNode[] keyData = keyContent.getData();
      if (keyData.length == 1) {
        meta.set(KEY, keyData[0]);
      } else {
        ArrayNode keyNode = MAPPER.createArrayNode();
        keyNode.addAll(Arrays.asList(keyData));
        meta.set(KEY, keyNode);
      }

      if (keyContent.getSchemaID() != SnowflakeRecordContent.NON_AVRO_SCHEMA) {
        meta.put(KEY_SCHEMA_ID, keyContent.getSchemaID());
      }
    } else {
      throw SnowflakeErrors.ERROR_0010.getException(
          "Unsupported Key format, please implement either String Key Converter or Snowflake"
              + " Converters");
    }
  }

  static JsonNode parseHeaders(Headers headers) {
    ObjectNode result = MAPPER.createObjectNode();
    for (Header header : headers) {
      result.set(header.key(), convertToJson(header.schema(), header.value()));
    }
    return result;
  }

  /**
   * Convert this object, in the org.apache.kafka.connect.data format, into a JSON object, returning
   * the converted object.
   *
   * @param schema schema of the object
   * @param logicalValue object to be converted
   * @return a JsonNode of the object
   */
  public static JsonNode convertToJson(Schema schema, Object logicalValue) {
    if (logicalValue == null) {
      if (schema
          == null) // Any schema is valid and we don't have a default, so treat this as an optional
        // schema
        return null;
      if (schema.defaultValue() != null) return convertToJson(schema, schema.defaultValue());
      if (schema.isOptional()) return JsonNodeFactory.instance.nullNode();
      throw SnowflakeErrors.ERROR_5015.getException(
          "Conversion error: null value for field that is required and has no default value");
    }

    Object value = logicalValue;
    try {
      final Schema.Type schemaType;
      if (schema == null) {
        Schema.Type primitiveType = ConnectSchema.schemaType(value.getClass());
        if (primitiveType != null) {
          schemaType = primitiveType;
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
                ISO_DATE_FORMAT.format((java.util.Date) value));
          }
          if (schema != null && Time.LOGICAL_NAME.equals(schema.name())) {
            return JsonNodeFactory.instance.textNode(TIME_FORMAT.format((java.util.Date) value));
          }
          return JsonNodeFactory.instance.numberNode((Integer) value);
        case INT64:
          if (schema != null && Timestamp.LOGICAL_NAME.equals(schema.name())) {
            return JsonNodeFactory.instance.numberNode(
                Timestamp.fromLogical(schema, (java.util.Date) value));
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
              JsonNode fieldValue = convertToJson(valueSchema, elem);
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
              JsonNode mapKey = convertToJson(keySchema, entry.getKey());
              JsonNode mapValue = convertToJson(valueSchema, entry.getValue());

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
              obj.set(field.name(), convertToJson(field.schema(), struct.get(field)));
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
}
