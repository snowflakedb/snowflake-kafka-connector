package com.snowflake.kafka.connector.records;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.snowflake.kafka.connector.builder.SinkRecordBuilder;
import com.snowflake.kafka.connector.internal.SnowflakeErrors;
import com.snowflake.kafka.connector.internal.SnowflakeKafkaConnectorException;
import com.snowflake.kafka.connector.internal.TestUtils;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class RecordContentTest {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final String TOPIC = "test";
  private static final int PARTITION = 0;

  @Test
  public void test() throws IOException {
    JsonNode data = OBJECT_MAPPER.readTree("{\"name\":123}");
    // json
    SnowflakeRecordContent content = new SnowflakeRecordContent(data);
    assertFalse(content.isBroken());
    assertEquals(SnowflakeRecordContent.NON_AVRO_SCHEMA, content.getSchemaID());
    assertEquals(1, content.getData().length);
    assertEquals(data.asText(), content.getData()[0].asText());
    assertTrue(TestUtils.assertError(SnowflakeErrors.ERROR_5011, content::getBrokenData));

    // avro
    int schemaID = 123;
    content = new SnowflakeRecordContent(data, schemaID);
    assertFalse(content.isBroken());
    assertEquals(schemaID, content.getSchemaID());
    assertEquals(1, content.getData().length);
    assertEquals(data.asText(), content.getData()[0].asText());
    assertTrue(TestUtils.assertError(SnowflakeErrors.ERROR_5011, content::getBrokenData));

    // avro without schema registry
    JsonNode[] data1 = new JsonNode[1];
    data1[0] = data;
    content = new SnowflakeRecordContent(data1);
    assertFalse(content.isBroken());
    assertEquals(SnowflakeRecordContent.NON_AVRO_SCHEMA, content.getSchemaID());
    assertEquals(1, content.getData().length);
    assertEquals(data.asText(), content.getData()[0].asText());
    assertTrue(TestUtils.assertError(SnowflakeErrors.ERROR_5011, content::getBrokenData));

    // broken record
    byte[] brokenData = "123".getBytes(StandardCharsets.UTF_8);
    content = new SnowflakeRecordContent(brokenData);
    assertTrue(content.isBroken());
    assertEquals(SnowflakeRecordContent.NON_AVRO_SCHEMA, content.getSchemaID());
    assertTrue(TestUtils.assertError(SnowflakeErrors.ERROR_5012, content::getData));
    assertEquals("123", new String(content.getBrokenData(), StandardCharsets.UTF_8));

    // null value
    content = new SnowflakeRecordContent();
    assertEquals(1, content.getData().length);
    assertTrue(content.getData()[0].isEmpty());
    assertEquals("{}", content.getData()[0].toString());

    // AVRO struct object
    SchemaBuilder builder =
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
                SchemaBuilder.map(Schema.INT32_SCHEMA, Schema.INT32_SCHEMA).build());
    Schema schema = builder.build();
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

    content = new SnowflakeRecordContent(schema, original, false);
    assertEquals(
        "{\"int8\":12,\"int16\":12,\"int32\":12,\"int64\":12,\"float32\":12.2,\"float64\":12.2,\"boolean\":true,\"string\":\"foo\",\"bytes\":\"Zm9v\",\"array\":[\"a\",\"b\",\"c\"],\"map\":{\"field\":1},\"mapNonStringKeys\":[[1,1]]}",
        content.getData()[0].toString());

    // JSON map object
    JsonNode jsonObject =
        OBJECT_MAPPER.readTree(
            "{\"int8\":12,\"int16\":12,\"int32\":12,\"int64\":12,\"float32\":12.2,\"float64\":12.2,\"boolean\":true,\"string\":\"foo\",\"bytes\":\"Zm9v\",\"array\":[\"a\",\"b\",\"c\"],\"map\":{\"field\":1},\"mapNonStringKeys\":[[1,1]]}");
    Map<String, Object> jsonMap =
        OBJECT_MAPPER.convertValue(jsonObject, new TypeReference<Map<String, Object>>() {});
    content = new SnowflakeRecordContent(null, jsonMap, false);
    assertEquals(
        "{\"int8\":12,\"int16\":12,\"int32\":12,\"int64\":12,\"float32\":12.2,\"float64\":12.2,\"boolean\":true,\"string\":\"foo\",\"bytes\":\"Zm9v\",\"array\":[\"a\",\"b\",\"c\"],\"map\":{\"field\":1},\"mapNonStringKeys\":[[1,1]]}",
        content.getData()[0].toString());
  }

  @ParameterizedTest
  @MethodSource("invalidSchemaSource")
  public void recordService_getProcessedRecordForSnowpipe_whenInvalidSchema_throwException(
      Schema schema, Object value) {
    // given
    RecordService service = RecordServiceFactory.createRecordService(false, false);
    SinkRecord record =
        SinkRecordBuilder.forTopicPartition(TOPIC, PARTITION)
            .withValueSchema(schema)
            .withValue(value)
            .build();

    // expect
    Assertions.assertThrows(
        SnowflakeKafkaConnectorException.class,
        () -> service.getProcessedRecordForSnowpipe(record));
  }

  public static Stream<Arguments> invalidSchemaSource() throws JsonProcessingException {
    return Stream.of(
        Arguments.of(
            Named.of("schema not matching content", SchemaBuilder.string().name("aName").build()),
            new SnowflakeRecordContent(OBJECT_MAPPER.readTree("{\"name\":123}"))),
        Arguments.of(Named.of("invalid schema type", new SnowflakeJsonSchema()), "string"));
  }

  @ParameterizedTest
  @MethodSource("invalidPutKeyInputSource")
  public void recordService_putKey_whenInvalidInput_throwException(Schema keySchema, Object key) {
    // given
    RecordService service = RecordServiceFactory.createRecordService(false, false);
    SinkRecord record =
        SinkRecordBuilder.forTopicPartition(TOPIC, PARTITION)
            .withKeySchema(keySchema)
            .withKey(key)
            .build();

    // expect
    Assertions.assertThrows(
        SnowflakeKafkaConnectorException.class,
        () -> service.putKey(record, OBJECT_MAPPER.createObjectNode()));
  }

  public static Stream<Arguments> invalidPutKeyInputSource() throws JsonProcessingException {
    return Stream.of(
        Arguments.of(
            Named.of("schema not matching content", SchemaBuilder.string().name("aName").build()),
            new SnowflakeRecordContent(OBJECT_MAPPER.readTree("{\"name\":123}"))),
        Arguments.of(Named.of("invalid schema type", new SnowflakeJsonSchema()), "string"));
  }

  @ParameterizedTest
  @MethodSource("convertToJsonSource")
  public void recordService_convertToJson_whenInvalidInput_throwException(Schema schema) {
    Assertions.assertThrows(
        SnowflakeKafkaConnectorException.class,
        () -> RecordService.convertToJson(schema, null, false));
  }

  public static Stream<Arguments> convertToJsonSource() {
    return Stream.of(
        Arguments.of(Named.of("int32 schema", SchemaBuilder.int32().build())),
        Arguments.of(Named.of("snowflake json schema", new SnowflakeJsonSchema())));
  }

  @Test
  public void recordService_convertToJson_returnDefaultValue() {
    Schema schema = SchemaBuilder.int32().optional().defaultValue(123).build();
    Assertions.assertEquals("123", RecordService.convertToJson(schema, null, false).toString());
  }

  @Test
  public void testConvertToJsonReadOnlyByteBuffer() {
    String original = "bytes";
    // Expecting a json string, which has additional quotes.
    String expected = "\"" + Base64.getEncoder().encodeToString(original.getBytes()) + "\"";
    ByteBuffer buffer = ByteBuffer.wrap(original.getBytes()).asReadOnlyBuffer();
    Schema schema = SchemaBuilder.bytes().build();

    assertEquals(expected, RecordService.convertToJson(schema, buffer, false).toString());
  }

  @Test
  public void testSchematizationStringField() throws JsonProcessingException {
    RecordService service = RecordServiceFactory.createRecordService(false, true);
    SnowflakeJsonConverter jsonConverter = new SnowflakeJsonConverter();

    String value = "{\"name\":\"sf\",\"answer\":42}";
    byte[] valueContents = (value).getBytes(StandardCharsets.UTF_8);
    SchemaAndValue sv = jsonConverter.toConnectData(TOPIC, valueContents);

    SinkRecord record =
        SinkRecordBuilder.forTopicPartition(TOPIC, PARTITION).withSchemaAndValue(sv).build();

    Map<String, Object> got = service.getProcessedRecordForStreamingIngest(record);
    // each field should be dumped into string format
    // json string should not be enclosed in additional brackets
    // a non-double-quoted column name will be transformed into uppercase
    assertEquals("sf", got.get("\"NAME\""));
    assertEquals("42", got.get("\"ANSWER\""));
  }

  @Test
  public void testSchematizationArrayOfObject() throws JsonProcessingException {
    RecordService service = RecordServiceFactory.createRecordService(false, true);
    SnowflakeJsonConverter jsonConverter = new SnowflakeJsonConverter();

    String value =
        "{\"players\":[{\"name\":\"John Doe\",\"age\":30},{\"name\":\"Jane Doe\",\"age\":30}]}";
    byte[] valueContents = (value).getBytes(StandardCharsets.UTF_8);
    SchemaAndValue sv = jsonConverter.toConnectData(TOPIC, valueContents);

    SinkRecord record =
        SinkRecordBuilder.forTopicPartition(TOPIC, PARTITION).withSchemaAndValue(sv).build();

    Map<String, Object> got = service.getProcessedRecordForStreamingIngest(record);
    assertEquals(
        "[{\"name\":\"John Doe\",\"age\":30},{\"name\":\"Jane Doe\",\"age\":30}]",
        got.get("\"PLAYERS\""));
  }

  @Test
  public void testColumnNameFormatting() throws JsonProcessingException {
    RecordService service = RecordServiceFactory.createRecordService(false, true);
    SnowflakeJsonConverter jsonConverter = new SnowflakeJsonConverter();

    String value = "{\"\\\"NaMe\\\"\":\"sf\",\"AnSwEr\":42}";
    byte[] valueContents = (value).getBytes(StandardCharsets.UTF_8);
    SchemaAndValue sv = jsonConverter.toConnectData(TOPIC, valueContents);

    SinkRecord record =
        SinkRecordBuilder.forTopicPartition(TOPIC, PARTITION).withSchemaAndValue(sv).build();
    Map<String, Object> got = service.getProcessedRecordForStreamingIngest(record);

    assertTrue(got.containsKey("\"NaMe\""));
    assertTrue(got.containsKey("\"ANSWER\""));
  }

  @Test
  public void testGetProcessedRecord() throws JsonProcessingException {
    SnowflakeJsonConverter jsonConverter = new SnowflakeJsonConverter();
    SchemaAndValue nullSchemaAndValue = jsonConverter.toConnectData(TOPIC, null);
    String keyStr = "string";

    // all null
    this.testGetProcessedRecordRunner(
        new SinkRecord(TOPIC, PARTITION, null, null, null, null, PARTITION), "{}", "");

    // null value
    this.testGetProcessedRecordRunner(
        new SinkRecord(
            TOPIC,
            PARTITION,
            Schema.STRING_SCHEMA,
            keyStr,
            nullSchemaAndValue.schema(),
            null,
            PARTITION),
        "{}",
        keyStr);
    this.testGetProcessedRecordRunner(
        new SinkRecord(
            TOPIC,
            PARTITION,
            Schema.STRING_SCHEMA,
            keyStr,
            null,
            nullSchemaAndValue.value(),
            PARTITION),
        "{}",
        keyStr);

    // null key
    this.testGetProcessedRecordRunner(
        new SinkRecord(
            TOPIC,
            PARTITION,
            Schema.STRING_SCHEMA,
            null,
            nullSchemaAndValue.schema(),
            nullSchemaAndValue.value(),
            PARTITION),
        "{}",
        "");

    SnowflakeKafkaConnectorException ex =
        assertThrows(
            SnowflakeKafkaConnectorException.class,
            () ->
                this.testGetProcessedRecordRunner(
                    new SinkRecord(
                        TOPIC,
                        PARTITION,
                        null,
                        keyStr,
                        nullSchemaAndValue.schema(),
                        nullSchemaAndValue.value(),
                        PARTITION),
                    "{}",
                    keyStr));
    assertTrue(ex.checkErrorCode(SnowflakeErrors.ERROR_0010));
  }

  private void testGetProcessedRecordRunner(
      SinkRecord record, String expectedRecordContent, String expectedRecordMetadataKey)
      throws JsonProcessingException {
    RecordService service = RecordServiceFactory.createRecordService(false, false);
    Map<String, Object> recordData = service.getProcessedRecordForStreamingIngest(record);

    assertEquals(2, recordData.size());
    assertEquals(expectedRecordContent, recordData.get("RECORD_CONTENT"));
    assertTrue(recordData.get("RECORD_METADATA").toString().contains(expectedRecordMetadataKey));
  }
}
