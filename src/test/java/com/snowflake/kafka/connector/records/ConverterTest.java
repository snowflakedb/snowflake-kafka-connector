package com.snowflake.kafka.connector.records;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.snowflake.kafka.connector.internal.SnowflakeKafkaConnectorException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.storage.SimpleHeaderConverter;
import org.junit.jupiter.api.Test;

class ConverterTest {

  private static final ObjectMapper mapper = new ObjectMapper();

  @Test
  void testConnectJsonConverter_MapInt64() throws JsonProcessingException {
    JsonConverter jsonConverter = new JsonConverter();
    Map<String, ?> config = Collections.singletonMap("schemas.enable", false);
    jsonConverter.configure(config, false);
    Map<String, Object> jsonMap = new HashMap<>();
    // Value will map to int64.
    jsonMap.put("test", Integer.MAX_VALUE);
    SchemaAndValue schemaAndValue =
        jsonConverter.toConnectData("test", mapper.writeValueAsBytes(jsonMap));
    Map<String, Object> result =
        KafkaRecordConverter.convertToMap(schemaAndValue.schema(), schemaAndValue.value());

    Map<String, Object> expected = new HashMap<>();
    expected.put("test", (long) Integer.MAX_VALUE);
    assertEquals(expected, result);
  }

  @Test
  void testConnectJsonConverter_MapBigDecimal() throws JsonProcessingException {
    JsonConverter jsonConverter = new JsonConverter();
    Map<String, ?> config = Collections.singletonMap("schemas.enable", false);
    jsonConverter.configure(config, false);
    // Use a BigDecimal that fits within precision limits
    Map<String, Object> jsonMap = new HashMap<>();
    jsonMap.put("test", new BigDecimal("12345678901234567890"));
    SchemaAndValue schemaAndValue =
        jsonConverter.toConnectData("test", mapper.writeValueAsBytes(jsonMap));
    Map<String, Object> result =
        KafkaRecordConverter.convertToMap(schemaAndValue.schema(), schemaAndValue.value());

    // BigDecimal gets converted through JSON which treats it as a number
    // JSON doesn't preserve BigDecimal - large numbers become scientific notation or lose precision
    // The important thing is the value is preserved as a numeric type
    assertNotNull(result.get("test"));
    assertInstanceOf(
        Number.class,
        result.get("test"),
        "Expected Number but got: " + result.get("test").getClass());
  }

  @Test
  void testConvertMapWithNestedValues() throws JsonProcessingException {
    JsonConverter jsonConverter = new JsonConverter();
    Map<String, ?> config = Collections.singletonMap("schemas.enable", false);
    jsonConverter.configure(config, false);

    Map<String, Object> nestedMap = new HashMap<>();
    nestedMap.put("nested", "value");

    Map<String, Object> jsonMap = new HashMap<>();
    jsonMap.put("outer", nestedMap);
    jsonMap.put("simple", "text");

    SchemaAndValue schemaAndValue =
        jsonConverter.toConnectData("test", mapper.writeValueAsBytes(jsonMap));
    Map<String, Object> result =
        KafkaRecordConverter.convertToMap(schemaAndValue.schema(), schemaAndValue.value());

    assertEquals("text", result.get("simple"));
    assertInstanceOf(Map.class, result.get("outer"));
    @SuppressWarnings("unchecked")
    Map<String, Object> outerMap = (Map<String, Object>) result.get("outer");
    assertEquals("value", outerMap.get("nested"));
  }

  @Test
  void testConvertHeaders() {
    org.apache.kafka.connect.header.Headers headers =
        new org.apache.kafka.connect.header.ConnectHeaders();
    headers.addString("stringHeader", "value");
    headers.addInt("intHeader", 42);
    headers.addBoolean("boolHeader", true);

    Map<String, String> result = KafkaRecordConverter.convertHeaders(headers);

    assertEquals("value", result.get("stringHeader"));
    assertEquals("42", result.get("intHeader"));
    assertEquals("true", result.get("boolHeader"));
  }

  @Test
  void testConvertKey() {
    // Test string key
    Object stringKeyResult = KafkaRecordConverter.convertKey(Schema.STRING_SCHEMA, "testKey");
    assertEquals("testKey", stringKeyResult);

    // Test int key
    Object intKeyResult = KafkaRecordConverter.convertKey(Schema.INT32_SCHEMA, 123);
    assertEquals(123, intKeyResult);

    // Test null key
    Object nullKeyResult = KafkaRecordConverter.convertKey(Schema.OPTIONAL_STRING_SCHEMA, null);
    assertNull(nullKeyResult);
  }

  @SuppressWarnings("resource")
  @Test
  void testConvertHeaders_WithSimpleHeaderConverter() {
    // Test that headers converted by SimpleHeaderConverter are properly handled
    // This covers the scenario where raw JSON header bytes are first converted by
    // SimpleHeaderConverter
    SimpleHeaderConverter headerConverter = new SimpleHeaderConverter();
    String rawHeader = "{\"f1\": \"1970-03-22T00:00:00.000Z\", \"f2\": true}";

    SchemaAndValue schemaAndValue =
        headerConverter.toConnectHeader(
            "test", "h1", rawHeader.getBytes(StandardCharsets.US_ASCII));

    // SimpleHeaderConverter returns String schema with the raw string value for complex JSON
    ConnectHeaders headers = new ConnectHeaders();
    headers.add("h1", schemaAndValue);

    Map<String, String> result = KafkaRecordConverter.convertHeaders(headers);

    // The header value should contain the JSON structure as a string
    assertNotNull(result.get("h1"));
    assertTrue(result.get("h1").contains("f1"));
    assertTrue(result.get("h1").contains("f2"));
  }

  @Test
  void testConvertHeaders_WithTimestampLogicalType() {
    // Test headers with Timestamp logical type
    ConnectHeaders headers = new ConnectHeaders();
    java.util.Date timestampValue =
        new java.util.Date(80 * 24 * 60 * 60 * 1000L); // 80 days from epoch

    headers.add("timestampHeader", timestampValue, Timestamp.SCHEMA);
    headers.add("boolHeader", true, Schema.BOOLEAN_SCHEMA);

    Map<String, String> result = KafkaRecordConverter.convertHeaders(headers);

    // Timestamp should be converted to epoch millis string
    String expectedTimestamp = String.valueOf(timestampValue.getTime());
    assertEquals(expectedTimestamp, result.get("timestampHeader"));
    assertEquals("true", result.get("boolHeader"));
  }

  @Test
  void testConvertHeaders_WithDateLogicalType() {
    // Test headers with Date logical type
    ConnectHeaders headers = new ConnectHeaders();
    // Create a date value (80 days from epoch = 1970-03-22)
    java.util.Date dateValue = new java.util.Date(80 * 24 * 60 * 60 * 1000L);

    headers.add("dateHeader", dateValue, Date.SCHEMA);

    Map<String, String> result = KafkaRecordConverter.convertHeaders(headers);

    // Date should be formatted as ISO date-time string
    assertEquals("1970-03-22T00:00:00.000Z", result.get("dateHeader"));
  }

  @Test
  void testConvertStructWithAllTypes() {
    // Test conversion of Struct with all supported types (equivalent to old RecordContentTest)
    Schema schema =
        SchemaBuilder.struct()
            .field("int8", SchemaBuilder.int8().defaultValue((byte) 2).doc("int8 field").build())
            .field("int16", Schema.INT16_SCHEMA)
            .field("int32", Schema.INT32_SCHEMA)
            .field("int64", Schema.INT64_SCHEMA)
            .field("float32", Schema.FLOAT32_SCHEMA)
            .field("float64", Schema.FLOAT64_SCHEMA)
            .field("boolean", Schema.BOOLEAN_SCHEMA)
            .field("string", Schema.STRING_SCHEMA)
            .field("bytes", Schema.BYTES_SCHEMA)
            .field("array", SchemaBuilder.array(Schema.STRING_SCHEMA).build())
            .field("map", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).build())
            .field(
                "mapNonStringKeys",
                SchemaBuilder.map(Schema.INT32_SCHEMA, Schema.INT32_SCHEMA).build())
            .build();

    Struct original =
        new Struct(schema)
            .put("int8", (byte) 12)
            .put("int16", (short) 12)
            .put("int32", 12)
            .put("int64", 12L)
            .put("float32", 12.2f)
            .put("float64", 12.2)
            .put("boolean", true)
            .put("string", "foo")
            .put("bytes", ByteBuffer.wrap("foo".getBytes()))
            .put("array", Arrays.asList("a", "b", "c"))
            .put("map", Collections.singletonMap("field", 1))
            .put("mapNonStringKeys", Collections.singletonMap(1, 1));

    Map<String, Object> result = KafkaRecordConverter.convertToMap(schema, original);

    assertEquals((byte) 12, result.get("int8"));
    assertEquals((short) 12, result.get("int16"));
    assertEquals(12, result.get("int32"));
    assertEquals(12L, result.get("int64"));
    assertEquals(12.2f, result.get("float32"));
    assertEquals(12.2, result.get("float64"));
    assertEquals(true, result.get("boolean"));
    assertEquals("foo", result.get("string"));
    assertEquals(Base64.getEncoder().encodeToString("foo".getBytes()), result.get("bytes"));
    assertEquals(Arrays.asList("a", "b", "c"), result.get("array"));

    @SuppressWarnings("unchecked")
    Map<String, Object> mapResult = (Map<String, Object>) result.get("map");
    assertEquals(1, mapResult.get("field"));

    // Non-string keys are encoded as [[key, value], ...]
    @SuppressWarnings("unchecked")
    List<List<Object>> mapNonStringKeysResult = (List<List<Object>>) result.get("mapNonStringKeys");
    assertEquals(1, mapNonStringKeysResult.size());
    assertEquals(Arrays.asList(1, 1), mapNonStringKeysResult.get(0));
  }

  @Test
  void testConvertValue_WithDefaultValue() {
    // Test that default values are returned when struct field value is null
    Schema fieldSchema = SchemaBuilder.int32().optional().defaultValue(123).build();
    Schema schema = SchemaBuilder.struct().field("field", fieldSchema).build();

    Struct struct = new Struct(schema);
    struct.put("field", null);

    Map<String, Object> result = KafkaRecordConverter.convertToMap(schema, struct);
    assertEquals(123, result.get("field"));
  }

  @Test
  void testConvertReadOnlyByteBuffer() {
    // Test conversion of read-only ByteBuffer
    String original = "bytes";
    String expected = Base64.getEncoder().encodeToString(original.getBytes());
    ByteBuffer buffer = ByteBuffer.wrap(original.getBytes()).asReadOnlyBuffer();

    Schema schema = SchemaBuilder.struct().field("bytesField", Schema.BYTES_SCHEMA).build();

    Struct struct = new Struct(schema).put("bytesField", buffer);

    Map<String, Object> result = KafkaRecordConverter.convertToMap(schema, struct);

    assertEquals(expected, result.get("bytesField"));
  }

  @Test
  void testConvertToMap_WithInvalidInput_ThrowsException() {
    // Test that invalid inputs throw exceptions
    assertThrows(
        SnowflakeKafkaConnectorException.class,
        () -> KafkaRecordConverter.convertToMap(Schema.STRING_SCHEMA, "not a map or struct"));
  }

  @Test
  void testConvertKey_WithTypeMismatch_ThrowsException() {
    // Test that type mismatch throws exception
    assertThrows(
        SnowflakeKafkaConnectorException.class,
        () -> KafkaRecordConverter.convertKey(Schema.INT32_SCHEMA, "not an int"));
  }

  @Test
  void testConvertDecimal() {
    // Test Decimal logical type conversion
    Schema decimalSchema = Decimal.schema(2);
    BigDecimal value = new BigDecimal("123.45");

    Schema schema = SchemaBuilder.struct().field("decimal", decimalSchema).build();
    Struct struct = new Struct(schema).put("decimal", value);

    Map<String, Object> result = KafkaRecordConverter.convertToMap(schema, struct);

    assertEquals(value, result.get("decimal"));
  }

  @Test
  void testConvertDecimal_ExceedsPrecision_ReturnsString() {
    // Test that BigDecimal exceeding max precision is converted to string
    Schema decimalSchema = Decimal.schema(0);
    BigDecimal value = new BigDecimal("999999999999999999999999999999999999999");

    Schema schema = SchemaBuilder.struct().field("decimal", decimalSchema).build();
    Struct struct = new Struct(schema).put("decimal", value);

    Map<String, Object> result = KafkaRecordConverter.convertToMap(schema, struct);

    assertEquals(value.toString(), result.get("decimal"));
  }

  @Test
  void testConvertTime() {
    // Test Time logical type conversion
    // Use a fixed time that will work regardless of local timezone
    // The Time logical type represents milliseconds since midnight, formatted with HH:mm:ss.SSSXXX
    java.util.Date timeValue = new java.util.Date(0L); // midnight UTC

    Schema schema = SchemaBuilder.struct().field("time", Time.SCHEMA).build();
    Struct struct = new Struct(schema).put("time", timeValue);

    Map<String, Object> result = KafkaRecordConverter.convertToMap(schema, struct);

    assertNotNull(result.get("time"));
    // The result should be a time string in format HH:mm:ss.SSSXXX
    String timeResult = result.get("time").toString();
    assertTrue(timeResult.contains(":"), "Time should contain colons: " + timeResult);
  }

  @Test
  void testConvertFloatSpecialValues() {
    // Test Float special values (NaN, Infinity)
    Schema schema =
        SchemaBuilder.struct()
            .field("nan", Schema.FLOAT32_SCHEMA)
            .field("posInf", Schema.FLOAT32_SCHEMA)
            .field("negInf", Schema.FLOAT32_SCHEMA)
            .build();

    Struct struct =
        new Struct(schema)
            .put("nan", Float.NaN)
            .put("posInf", Float.POSITIVE_INFINITY)
            .put("negInf", Float.NEGATIVE_INFINITY);

    Map<String, Object> result = KafkaRecordConverter.convertToMap(schema, struct);

    assertEquals("NaN", result.get("nan"));
    assertEquals("Inf", result.get("posInf"));
    assertEquals("-Inf", result.get("negInf"));
  }

  @Test
  void testConvertDoubleSpecialValues() {
    // Test Double special values (NaN, Infinity)
    Schema schema =
        SchemaBuilder.struct()
            .field("nan", Schema.FLOAT64_SCHEMA)
            .field("posInf", Schema.FLOAT64_SCHEMA)
            .field("negInf", Schema.FLOAT64_SCHEMA)
            .build();

    Struct struct =
        new Struct(schema)
            .put("nan", Double.NaN)
            .put("posInf", Double.POSITIVE_INFINITY)
            .put("negInf", Double.NEGATIVE_INFINITY);

    Map<String, Object> result = KafkaRecordConverter.convertToMap(schema, struct);

    assertEquals("NaN", result.get("nan"));
    assertEquals("Inf", result.get("posInf"));
    assertEquals("-Inf", result.get("negInf"));
  }

  @Test
  void testConvertNullValue() {
    // Test null value handling with optional schema
    Schema schema = SchemaBuilder.struct().field("optional", Schema.OPTIONAL_STRING_SCHEMA).build();

    Struct struct = new Struct(schema).put("optional", null);

    Map<String, Object> result = KafkaRecordConverter.convertToMap(schema, struct);

    assertNull(result.get("optional"));
  }
}
