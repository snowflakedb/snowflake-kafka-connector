package com.snowflake.kafka.connector.records;

import com.snowflake.kafka.connector.internal.KCLogger;
import com.snowflake.kafka.connector.internal.SnowflakeErrors;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.header.Headers;

public final class KafkaRecordConverter {

  private static final KCLogger LOGGER = new KCLogger(KafkaRecordConverter.class.getName());

  private static final int MAX_SNOWFLAKE_NUMBER_PRECISION = 38;

  private static final DateTimeFormatter ISO_DATE_TIME_FORMAT =
      DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").withZone(ZoneOffset.UTC);

  private static final DateTimeFormatter TIME_FORMAT =
      DateTimeFormatter.ofPattern("HH:mm:ss.SSSXXX").withZone(ZoneId.systemDefault());

  private KafkaRecordConverter() {}

  /**
   * Converts a Kafka Connect value with its schema directly to a Map suitable for Snowflake
   * streaming ingest.
   */
  public static Map<String, Object> convertToMap(Schema schema, Object value) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "Converting record to map. Schema: {}, valueType: {}",
          schema != null ? schema.type() : "null",
          value != null ? value.getClass().getSimpleName() : "null");
    }

    if (value == null) {
      return new HashMap<>();
    }

    if (value instanceof Map) {
      return convertMapToMap((Map<?, ?>) value, schema);
    }

    if (value instanceof Struct) {
      return convertStructToMap((Struct) value);
    }

    throw SnowflakeErrors.ERROR_5015.getException(
        "Cannot schematize record. Record value must be a Map or Struct. Consider using kafka"
            + " HoistField transformer to wrap the value of the record.");
  }

  public static Map<String, String> convertHeaders(Headers headers) {
    Map<String, String> result = new HashMap<>();
    if (headers == null) {
      LOGGER.trace("Headers is null, returning empty map");
      return result;
    }
    for (Header header : headers) {
      Object headerValue = convertValue(header.schema(), header.value());
      result.put(header.key(), headerValue == null ? null : String.valueOf(headerValue));
    }
    return result;
  }

  public static Object convertKey(Schema keySchema, Object key) {
    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace(
          "Converting key. Schema: {}, keyType: {}",
          keySchema != null ? keySchema.type() : "null",
          key != null ? key.getClass().getSimpleName() : "null");
    }
    if (key == null) {
      LOGGER.trace("Key is null, returning null");
      return null;
    }
    return convertValue(keySchema, key);
  }

  private static Map<String, Object> convertStructToMap(Struct struct) {
    Map<String, Object> result = new HashMap<>();
    Schema schema = struct.schema();
    for (Field field : schema.fields()) {
      if (LOGGER.isTraceEnabled()) {

        LOGGER.trace(
            "Converting struct field: {}, schema: {}",
            field.name(),
            field.schema() != null ? field.schema().type() : "null");
      }
      Object fieldValue = convertValue(field.schema(), struct.get(field));
      result.put(field.name(), fieldValue);
    }
    return result;
  }

  private static Map<String, Object> convertMapToMap(Map<?, ?> map, Schema schema) {
    Map<String, Object> result = new LinkedHashMap<>();

    Schema valueSchema = schema != null ? schema.valueSchema() : null;

    for (Map.Entry<?, ?> entry : map.entrySet()) {
      String key = String.valueOf(entry.getKey());

      Object convertedValue;
      if (entry.getValue() instanceof Map) {
        convertedValue = convertMapToMap((Map<?, ?>) entry.getValue(), valueSchema);
      } else if (entry.getValue() instanceof Struct) {
        convertedValue = convertStructToMap((Struct) entry.getValue());
      } else {
        convertedValue = convertValue(valueSchema, entry.getValue());
      }
      result.put(key, convertedValue);
    }
    return result;
  }

  static Object convertValue(Schema schema, Object value) {
    if (value == null) {
      if (schema == null) {
        LOGGER.trace("Value is null with no schema, returning null");
        return null;
      }
      if (schema.defaultValue() != null) {
        return convertValue(schema, schema.defaultValue());
      }
      if (schema.isOptional()) {
        LOGGER.trace("Value is null for optional field, returning null");
        return null;
      }
      throw SnowflakeErrors.ERROR_5015.getException(
          "Conversion error: null value for field that is required and has no default value");
    }

    final Schema.Type schemaType = getSchemaType(schema, value);
    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace(
          "Converting value of type {} with schemaType {}",
          value.getClass().getSimpleName(),
          schemaType);
    }

    // Validate that value type matches schema type when schema is present
    if (schema != null) {
      validateValueType(schema.type(), value);
    }

    switch (schemaType) {
      case INT8:
      case INT16:
      case BOOLEAN:
        LOGGER.trace("Passthrough for primitive type: {}", schemaType);
        return value;
      case INT32:
        return convertInt32(schema, value);
      case INT64:
        return convertInt64(schema, value);
      case FLOAT32:
        return handleFloatSpecialValues((Float) value);
      case FLOAT64:
        return handleDoubleSpecialValues((Double) value);
      case STRING:
        LOGGER.trace("Converting to String");
        return value.toString();
      case BYTES:
        return convertBytes(schema, value);
      case ARRAY:
        return convertArray(schema, value);
      case MAP:
        return convertMapValue(schema, value);
      case STRUCT:
        return convertStructToMap((Struct) value);
      default:
        throw SnowflakeErrors.ERROR_5015.getException("Couldn't convert " + value + " to Object.");
    }
  }

  private static void validateValueType(Schema.Type schemaType, Object value) {
    boolean valid;
    switch (schemaType) {
      case INT8:
        valid = value instanceof Byte;
        break;
      case INT16:
        valid = value instanceof Short;
        break;
      case INT32:
        valid = value instanceof Integer || value instanceof java.util.Date;
        break;
      case INT64:
        valid = value instanceof Long || value instanceof java.util.Date;
        break;
      case FLOAT32:
        valid = value instanceof Float;
        break;
      case FLOAT64:
        valid = value instanceof Double;
        break;
      case BOOLEAN:
        valid = value instanceof Boolean;
        break;
      case STRING:
        valid = value instanceof String;
        break;
      case BYTES:
        valid =
            value instanceof byte[] || value instanceof ByteBuffer || value instanceof BigDecimal;
        break;
      case ARRAY:
        valid = value instanceof Collection;
        break;
      case MAP:
        valid = value instanceof Map;
        break;
      case STRUCT:
        valid = value instanceof Struct;
        break;
      default:
        valid = false;
    }
    if (!valid) {
      throw SnowflakeErrors.ERROR_5015.getException(
          "Type mismatch: expected " + schemaType + " but got " + value.getClass().getName());
    }
  }

  private static Schema.Type getSchemaType(Schema schema, Object value) {
    if (schema != null) {
      return schema.type();
    }

    LOGGER.trace(
        "No schema provided, inferring type from value class: {}", value.getClass().getName());

    // Handle collections and maps before checking primitive schema types
    // ConnectSchema.schemaType() only matches exact classes, not subclasses
    if (value instanceof Map) {
      return Schema.Type.MAP;
    }
    if (value instanceof Collection) {
      return Schema.Type.ARRAY;
    }

    Schema.Type primitiveType = ConnectSchema.schemaType(value.getClass());
    if (primitiveType != null) {
      return primitiveType;
    }

    if (value instanceof java.util.Date) {
      return Schema.Type.INT64;
    }

    throw SnowflakeErrors.ERROR_5015.getException(
        "Java class " + value.getClass() + " does not have corresponding schema type.");
  }

  private static Object convertInt32(Schema schema, Object value) {
    if (schema != null && Date.LOGICAL_NAME.equals(schema.name())) {
      LOGGER.trace("Converting INT32 Date logical type to ISO format");
      return ISO_DATE_TIME_FORMAT.format(((java.util.Date) value).toInstant());
    }
    if (schema != null && Time.LOGICAL_NAME.equals(schema.name())) {
      LOGGER.trace("Converting INT32 Time logical type to time format");
      return TIME_FORMAT.format(((java.util.Date) value).toInstant());
    }
    LOGGER.trace("Passthrough for INT32 value");
    return value;
  }

  private static Object convertInt64(Schema schema, Object value) {
    if (schema != null && Timestamp.LOGICAL_NAME.equals(schema.name())) {
      LOGGER.trace("Converting INT64 Timestamp logical type to string");
      return String.valueOf(Timestamp.fromLogical(schema, (java.util.Date) value));
    }
    LOGGER.trace("Passthrough for INT64 value");
    return value;
  }

  private static Object convertBytes(Schema schema, Object value) {
    if (schema != null && Decimal.LOGICAL_NAME.equals(schema.name())) {
      BigDecimal bigDecimalValue = (BigDecimal) value;
      if (bigDecimalValue.precision() > MAX_SNOWFLAKE_NUMBER_PRECISION) {
        if (LOGGER.isTraceEnabled()) {
          LOGGER.trace(
              "Converting Decimal with precision {} (exceeds max {}) to string",
              bigDecimalValue.precision(),
              MAX_SNOWFLAKE_NUMBER_PRECISION);
        }
        return bigDecimalValue.toString();
      }
      return bigDecimalValue;
    }

    LOGGER.trace("Converting bytes to Base64 string");
    byte[] bytes = toByteArray(value);
    return Base64.getEncoder().encodeToString(bytes);
  }

  private static byte[] toByteArray(Object value) {
    if (value instanceof byte[]) {
      return (byte[]) value;
    }
    if (value instanceof ByteBuffer) {
      ByteBuffer byteBuffer = (ByteBuffer) value;
      if (byteBuffer.hasArray()) {
        return byteBuffer.array();
      }
      ByteBuffer clone = ByteBuffer.allocate(byteBuffer.capacity());
      byteBuffer.rewind();
      clone.put(byteBuffer);
      byteBuffer.rewind();
      clone.flip();
      return clone.array();
    }
    throw SnowflakeErrors.ERROR_5015.getException(
        "Invalid type for bytes type: " + value.getClass());
  }

  private static List<Object> convertArray(Schema schema, Object value) {
    Collection<?> collection = (Collection<?>) value;
    List<Object> result = new ArrayList<>(collection.size());
    Schema elementSchema = schema != null ? schema.valueSchema() : null;
    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace(
          "Array element schema: {}", elementSchema != null ? elementSchema.type() : "null");
    }
    for (Object elem : collection) {
      result.add(convertValue(elementSchema, elem));
    }
    return result;
  }

  private static Object convertMapValue(Schema schema, Object value) {
    Map<?, ?> map = (Map<?, ?>) value;
    boolean useObjectMode = shouldUseObjectMode(schema, map);

    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace(
          "Converting nested Map with {} entries, useObjectMode: {}", map.size(), useObjectMode);
    }

    if (useObjectMode) {
      Map<String, Object> result = new LinkedHashMap<>();
      Schema valueSchema = schema != null ? schema.valueSchema() : null;
      for (Map.Entry<?, ?> entry : map.entrySet()) {
        String key = String.valueOf(entry.getKey());
        result.put(key, convertValue(valueSchema, entry.getValue()));
      }
      return result;
    } else {
      // Non-string keys: use array encoding [[key, value], [key, value], ...]
      List<List<Object>> result = new ArrayList<>();
      Schema keySchema = schema != null ? schema.keySchema() : null;
      Schema valueSchema = schema != null ? schema.valueSchema() : null;
      if (LOGGER.isTraceEnabled()) {
        LOGGER.trace(
            "Array mode key schema: {}, value schema: {}",
            keySchema != null ? keySchema.type() : "null",
            valueSchema != null ? valueSchema.type() : "null");
      }
      for (Map.Entry<?, ?> entry : map.entrySet()) {
        List<Object> pair = new ArrayList<>(2);
        pair.add(convertValue(keySchema, entry.getKey()));
        pair.add(convertValue(valueSchema, entry.getValue()));
        result.add(pair);
      }
      return result;
    }
  }

  private static boolean shouldUseObjectMode(Schema schema, Map<?, ?> map) {
    if (schema != null) {
      return schema.keySchema() != null && schema.keySchema().type() == Schema.Type.STRING;
    }
    // For schemaless, check if all keys are strings
    for (Object key : map.keySet()) {
      if (!(key instanceof String)) {
        return false;
      }
    }
    return true;
  }

  private static Object handleFloatSpecialValues(Float value) {
    if (Float.isNaN(value)) {
      return "NaN";
    }
    if (Float.isInfinite(value)) {
      return value > 0 ? "Inf" : "-Inf";
    }
    return value;
  }

  private static Object handleDoubleSpecialValues(Double value) {
    if (Double.isNaN(value)) {
      return "NaN";
    }
    if (Double.isInfinite(value)) {
      return value > 0 ? "Inf" : "-Inf";
    }
    return value;
  }
}
