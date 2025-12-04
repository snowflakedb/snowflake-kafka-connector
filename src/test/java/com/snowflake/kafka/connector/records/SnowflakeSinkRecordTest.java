package com.snowflake.kafka.connector.records;

import static com.snowflake.kafka.connector.Utils.TABLE_COLUMN_METADATA;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.snowflake.kafka.connector.builder.SinkRecordBuilder;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class SnowflakeSinkRecordTest {

  private static final String TOPIC = "test";
  private static final int PARTITION = 0;

  private final SnowflakeMetadataConfig metadataConfig = new SnowflakeMetadataConfig();
  private final JsonConverter jsonConverter = createJsonConverter();

  @Test
  void testValidRecord_WithJsonMap() {
    // Test creating a valid record from JSON map
    SchemaAndValue schemaAndValue = toConnectData("{\"name\": \"test\", \"value\": 123}");

    SinkRecord kafkaRecord =
        SinkRecordBuilder.forTopicPartition(TOPIC, PARTITION)
            .withSchemaAndValue(schemaAndValue)
            .build();

    SnowflakeSinkRecord record = SnowflakeSinkRecord.from(kafkaRecord, metadataConfig);

    assertTrue(record.isValid());
    assertFalse(record.isBroken());
    assertFalse(record.isTombstone());
    assertEquals(SnowflakeSinkRecord.RecordState.VALID, record.getState());

    Map<String, Object> content = record.getContent();
    assertEquals("test", content.get("name"));
    assertEquals(123L, content.get("value"));
  }

  @Test
  void testValidRecord_WithStruct() {
    // Test creating a valid record from Struct with multiple types
    Schema schema =
        SchemaBuilder.struct()
            .field("int8", Schema.INT8_SCHEMA)
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
            .build();

    Struct struct =
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
            .put("map", Collections.singletonMap("field", 1));

    SinkRecord kafkaRecord =
        SinkRecordBuilder.forTopicPartition(TOPIC, PARTITION)
            .withValueSchema(schema)
            .withValue(struct)
            .build();

    SnowflakeSinkRecord record = SnowflakeSinkRecord.from(kafkaRecord, metadataConfig);

    assertTrue(record.isValid());
    assertFalse(record.isBroken());
    assertFalse(record.isTombstone());

    Map<String, Object> content = record.getContent();
    assertEquals((byte) 12, content.get("int8"));
    assertEquals((short) 12, content.get("int16"));
    assertEquals(12, content.get("int32"));
    assertEquals(12L, content.get("int64"));
    assertEquals(12.2f, content.get("float32"));
    assertEquals(12.2, content.get("float64"));
    assertEquals(true, content.get("boolean"));
    assertEquals("foo", content.get("string"));
    assertEquals(Base64.getEncoder().encodeToString("foo".getBytes()), content.get("bytes"));
    assertEquals(Arrays.asList("a", "b", "c"), content.get("array"));
  }

  @Test
  void testTombstoneRecord() {
    // Test creating a tombstone record (null value)
    SinkRecord kafkaRecord =
        SinkRecordBuilder.forTopicPartition(TOPIC, PARTITION)
            .withValueSchema(null)
            .withValue(null)
            .build();

    SnowflakeSinkRecord record = SnowflakeSinkRecord.from(kafkaRecord, metadataConfig);

    assertFalse(record.isValid());
    assertFalse(record.isBroken());
    assertTrue(record.isTombstone());
    assertEquals(SnowflakeSinkRecord.RecordState.TOMBSTONE, record.getState());
    assertTrue(record.getContent().isEmpty());
  }

  @Test
  void testBrokenRecord_WithInvalidKeySchema() {
    // Test creating a broken record when key doesn't match schema
    SinkRecord kafkaRecord =
        SinkRecordBuilder.forTopicPartition(TOPIC, PARTITION)
            .withKeySchema(Schema.INT32_SCHEMA)
            .withKey("not an int") // String doesn't match INT32_SCHEMA
            .withValueSchema(Schema.STRING_SCHEMA)
            .withValue("{}")
            .build();

    SnowflakeSinkRecord record = SnowflakeSinkRecord.from(kafkaRecord, metadataConfig);

    assertFalse(record.isValid());
    assertTrue(record.isBroken());
    assertFalse(record.isTombstone());
    assertEquals(SnowflakeSinkRecord.RecordState.BROKEN, record.getState());
  }

  @Test
  void testBrokenRecord_WithInvalidValue() {
    // Test creating a broken record when value cannot be converted
    // Using a String value with STRING_SCHEMA but convertToMap expects Map or Struct
    SinkRecord kafkaRecord =
        SinkRecordBuilder.forTopicPartition(TOPIC, PARTITION)
            .withValueSchema(Schema.STRING_SCHEMA)
            .withValue("just a plain string")
            .build();

    SnowflakeSinkRecord record = SnowflakeSinkRecord.from(kafkaRecord, metadataConfig);

    // Record should be broken because convertToMap cannot handle plain String
    assertTrue(record.isBroken());
    assertFalse(record.isValid());
    assertFalse(record.isTombstone());
    assertEquals(SnowflakeSinkRecord.RecordState.BROKEN, record.getState());
  }

  @Test
  void testGetContentWithMetadata_WhenIncludeMetadataTrue() {
    // Test that metadata is included when flag is true
    SnowflakeSinkRecord record =
        createRecordFromJson("{\"name\": \"test\"}", createMetadataConfigWithAll());

    Map<String, Object> contentWithMetadata = record.getContentWithMetadata(true);

    assertNotNull(contentWithMetadata.get(TABLE_COLUMN_METADATA));
    assertEquals("test", contentWithMetadata.get("name"));
  }

  @Test
  void testGetContentWithMetadata_WhenIncludeMetadataFalse() {
    // Test that metadata is NOT included when flag is false
    SnowflakeSinkRecord record = createRecordFromJson("{\"name\": \"test\"}", metadataConfig);

    Map<String, Object> contentWithMetadata = record.getContentWithMetadata(false);

    assertFalse(contentWithMetadata.containsKey(TABLE_COLUMN_METADATA));
    assertEquals("test", contentWithMetadata.get("name"));
  }

  @Test
  void testMetadataContainsKey() {
    // Test that metadata contains the key
    SchemaAndValue schemaAndValue = toConnectData("{\"name\": \"test\"}");

    SinkRecord kafkaRecord =
        SinkRecordBuilder.forTopicPartition(TOPIC, PARTITION)
            .withKeySchema(Schema.STRING_SCHEMA)
            .withKey("myKey")
            .withSchemaAndValue(schemaAndValue)
            .build();

    SnowflakeSinkRecord record =
        SnowflakeSinkRecord.from(kafkaRecord, createMetadataConfigWithAll());

    Map<String, Object> metadata = record.getMetadata();
    assertEquals("myKey", metadata.get("key"));
  }

  @Test
  void testFullMetadataFields() {
    // Test that all metadata fields are present when configured
    Map<String, String> config = new HashMap<>();
    config.put("snowflake.metadata.all", "true");
    config.put("snowflake.metadata.createtime", "true");
    config.put("snowflake.metadata.topic", "true");
    config.put("snowflake.metadata.offset.and.partition", "true");
    config.put("snowflake.streaming.metadata.connectorPushTime", "true");
    SnowflakeMetadataConfig fullMetadataConfig = new SnowflakeMetadataConfig(config);

    SchemaAndValue schemaAndValue = toConnectData("{\"data\": \"value\"}");

    long createTime = 1234567890L;
    long offset = 10L;

    SinkRecord kafkaRecord =
        SinkRecordBuilder.forTopicPartition(TOPIC, PARTITION)
            .withKeySchema(Schema.STRING_SCHEMA)
            .withKey("testKey")
            .withSchemaAndValue(schemaAndValue)
            .withOffset(offset)
            .withTimestamp(createTime, TimestampType.CREATE_TIME)
            .build();

    Instant connectorPushTime = Instant.ofEpochMilli(9876543210L);
    SnowflakeSinkRecord record =
        SnowflakeSinkRecord.from(kafkaRecord, fullMetadataConfig, connectorPushTime);

    Map<String, Object> metadata = record.getMetadata();

    // Verify all metadata fields
    assertEquals(TOPIC, metadata.get("topic"));
    assertEquals(offset, metadata.get("offset"));
    assertEquals(PARTITION, metadata.get("partition"));
    assertEquals("testKey", metadata.get("key"));
    assertEquals(createTime, metadata.get("CreateTime"));
    assertEquals(connectorPushTime.toEpochMilli(), metadata.get("SnowflakeConnectorPushTime"));
  }

  @ParameterizedTest(name = "timestamp type {0} should produce metadata key {1}")
  @MethodSource("timestampTypeTestCases")
  void testMetadataWithTimestampType(TimestampType timestampType, String expectedMetadataKey) {
    Map<String, String> config = new HashMap<>();
    config.put("snowflake.metadata.createtime", "true");
    SnowflakeMetadataConfig timestampConfig = new SnowflakeMetadataConfig(config);

    SchemaAndValue schemaAndValue = toConnectData("{\"data\": \"value\"}");
    long timestamp = 1609459200000L; // 2021-01-01 00:00:00 UTC

    SinkRecord kafkaRecord =
        SinkRecordBuilder.forTopicPartition(TOPIC, PARTITION)
            .withSchemaAndValue(schemaAndValue)
            .withTimestamp(timestamp, timestampType)
            .build();

    SnowflakeSinkRecord record = SnowflakeSinkRecord.from(kafkaRecord, timestampConfig);

    Map<String, Object> metadata = record.getMetadata();
    assertEquals(timestamp, metadata.get(expectedMetadataKey));
  }

  @Test
  void testMetadataWithHeaders() {
    // Test metadata includes headers with various types
    SchemaAndValue schemaAndValue = toConnectData("{\"data\": \"value\"}");

    Headers headers = new ConnectHeaders();
    headers.addString("stringHeader", "testHeaderValue");
    headers.addInt("intHeader", 42);
    headers.addBoolean("boolHeader", true);

    SinkRecord kafkaRecord = createSinkRecordWithHeaders(schemaAndValue, headers, "key");
    SnowflakeSinkRecord record =
        SnowflakeSinkRecord.from(kafkaRecord, createMetadataConfigWithAll());

    Map<String, Object> metadata = record.getMetadata();
    assertNotNull(metadata.get("headers"));

    @SuppressWarnings("unchecked")
    Map<String, String> headersMap = (Map<String, String>) metadata.get("headers");
    assertEquals("testHeaderValue", headersMap.get("stringHeader"));
    assertEquals("42", headersMap.get("intHeader"));
    assertEquals("true", headersMap.get("boolHeader"));
  }

  @Test
  void testMetadataWithComplexHeaders() {
    // Test metadata with headers containing JSON-like complex values
    SchemaAndValue schemaAndValue = toConnectData("{\"data\": \"value\"}");

    Headers headers = new ConnectHeaders();
    headers.addString("objectAsJsonStringHeader", "{\"key1\":\"value1\",\"key2\":\"value2\"}");
    headers.addString("header2", "testheaderstring");

    SinkRecord kafkaRecord = createSinkRecordWithHeaders(schemaAndValue, headers, "key");
    SnowflakeSinkRecord record =
        SnowflakeSinkRecord.from(kafkaRecord, createMetadataConfigWithAll());

    Map<String, Object> metadata = record.getMetadata();

    @SuppressWarnings("unchecked")
    Map<String, String> headersMap = (Map<String, String>) metadata.get("headers");
    assertEquals(
        "{\"key1\":\"value1\",\"key2\":\"value2\"}", headersMap.get("objectAsJsonStringHeader"));
    assertEquals("testheaderstring", headersMap.get("header2"));
  }

  @Test
  void testContentWithArray() {
    SnowflakeSinkRecord record =
        createRecordFromJson("{\"key\": [\"a\", \"b\", \"c\"]}", metadataConfig);

    assertTrue(record.isValid());
    Map<String, Object> content = record.getContent();

    @SuppressWarnings("unchecked")
    List<String> arrayValue = (List<String>) content.get("key");
    assertEquals(Arrays.asList("a", "b", "c"), arrayValue);
  }

  @Test
  void testContentWithEmptyArray() {
    SnowflakeSinkRecord record = createRecordFromJson("{\"key\": []}", metadataConfig);

    assertTrue(record.isValid());
    Map<String, Object> content = record.getContent();

    @SuppressWarnings("unchecked")
    List<Object> arrayValue = (List<Object>) content.get("key");
    assertTrue(arrayValue.isEmpty());
  }

  @Test
  void testEmptyContentWithMetadata() {
    SnowflakeSinkRecord record = createRecordFromJson("{}", createMetadataConfigWithAll());

    assertTrue(record.isValid());
    // Content should be empty (no data fields)
    assertTrue(record.getContent().isEmpty());

    // But metadata should still be present
    Map<String, Object> contentWithMetadata = record.getContentWithMetadata(true);
    assertNotNull(contentWithMetadata.get(TABLE_COLUMN_METADATA));
  }

  @Test
  void testContentWithKeyValue() {
    SnowflakeSinkRecord record =
        createRecordFromJson("{\"key\": \"value\"}", createMetadataConfigWithAll());

    assertTrue(record.isValid());

    Map<String, Object> content = record.getContent();
    assertEquals("value", content.get("key"));

    Map<String, Object> contentWithMetadata = record.getContentWithMetadata(true);
    assertEquals("value", contentWithMetadata.get("key"));
    assertNotNull(contentWithMetadata.get(TABLE_COLUMN_METADATA));
  }

  @Test
  void testConnectorPushTime_WhenDisabled_NotPresent() {
    // Test that SnowflakeConnectorPushTime is NOT present when disabled
    Map<String, String> config = new HashMap<>();
    config.put("snowflake.metadata.all", "true");
    config.put("snowflake.streaming.metadata.connectorPushTime", "false");
    SnowflakeMetadataConfig disabledPushTimeConfig = new SnowflakeMetadataConfig(config);

    SnowflakeSinkRecord record =
        createRecordFromJson("{\"data\": \"value\"}", disabledPushTimeConfig);

    Map<String, Object> metadata = record.getMetadata();
    assertFalse(metadata.containsKey("SnowflakeConnectorPushTime"));
  }

  @Test
  void testMetadata_WhenCreateTimeDisabled_NotPresent() {
    // Test that CreateTime is NOT present when snowflake.metadata.createtime=false
    Map<String, String> config = new HashMap<>();
    config.put("snowflake.metadata.createtime", "false");
    config.put("snowflake.metadata.topic", "true");
    config.put("snowflake.metadata.offset.and.partition", "true");
    SnowflakeMetadataConfig noCreateTimeConfig = new SnowflakeMetadataConfig(config);

    SchemaAndValue schemaAndValue = toConnectData("{\"data\": \"value\"}");

    long createTime = 1234567890L;

    SinkRecord kafkaRecord =
        SinkRecordBuilder.forTopicPartition(TOPIC, PARTITION)
            .withSchemaAndValue(schemaAndValue)
            .withTimestamp(createTime, TimestampType.CREATE_TIME)
            .build();

    SnowflakeSinkRecord record = SnowflakeSinkRecord.from(kafkaRecord, noCreateTimeConfig);

    Map<String, Object> metadata = record.getMetadata();
    assertFalse(metadata.containsKey("CreateTime"));
    assertTrue(metadata.containsKey("topic"));
    assertTrue(metadata.containsKey("offset"));
    assertTrue(metadata.containsKey("partition"));
  }

  @Test
  void testMetadata_WhenTopicDisabled_NotPresent() {
    // Test that topic is NOT present when snowflake.metadata.topic=false
    Map<String, String> config = new HashMap<>();
    config.put("snowflake.metadata.createtime", "true");
    config.put("snowflake.metadata.topic", "false");
    config.put("snowflake.metadata.offset.and.partition", "true");
    SnowflakeMetadataConfig noTopicConfig = new SnowflakeMetadataConfig(config);

    SchemaAndValue schemaAndValue = toConnectData("{\"data\": \"value\"}");

    SinkRecord kafkaRecord =
        SinkRecordBuilder.forTopicPartition(TOPIC, PARTITION)
            .withSchemaAndValue(schemaAndValue)
            .withTimestamp(System.currentTimeMillis(), TimestampType.CREATE_TIME)
            .build();

    SnowflakeSinkRecord record = SnowflakeSinkRecord.from(kafkaRecord, noTopicConfig);

    Map<String, Object> metadata = record.getMetadata();
    assertFalse(metadata.containsKey("topic"));
    assertTrue(metadata.containsKey("CreateTime"));
    assertTrue(metadata.containsKey("offset"));
    assertTrue(metadata.containsKey("partition"));
  }

  @Test
  void testMetadata_WhenOffsetAndPartitionDisabled_NotPresent() {
    // Test that offset/partition are NOT present when snowflake.metadata.offset.and.partition=false
    Map<String, String> config = new HashMap<>();
    config.put("snowflake.metadata.createtime", "true");
    config.put("snowflake.metadata.topic", "true");
    config.put("snowflake.metadata.offset.and.partition", "false");
    SnowflakeMetadataConfig noOffsetPartitionConfig = new SnowflakeMetadataConfig(config);

    SchemaAndValue schemaAndValue = toConnectData("{\"data\": \"value\"}");

    SinkRecord kafkaRecord =
        SinkRecordBuilder.forTopicPartition(TOPIC, PARTITION)
            .withSchemaAndValue(schemaAndValue)
            .withTimestamp(System.currentTimeMillis(), TimestampType.CREATE_TIME)
            .build();

    SnowflakeSinkRecord record = SnowflakeSinkRecord.from(kafkaRecord, noOffsetPartitionConfig);

    Map<String, Object> metadata = record.getMetadata();
    assertFalse(metadata.containsKey("offset"));
    assertFalse(metadata.containsKey("partition"));
    assertTrue(metadata.containsKey("topic"));
    assertTrue(metadata.containsKey("CreateTime"));
  }

  @Test
  void testMetadata_WhenAllFieldsDisabled_EmptyMetadata() {
    // Test that when all metadata fields are disabled, metadata has minimal content
    Map<String, String> config = new HashMap<>();
    config.put("snowflake.metadata.createtime", "false");
    config.put("snowflake.metadata.topic", "false");
    config.put("snowflake.metadata.offset.and.partition", "false");
    config.put("snowflake.streaming.metadata.connectorPushTime", "false");
    SnowflakeMetadataConfig allDisabledConfig = new SnowflakeMetadataConfig(config);

    SchemaAndValue schemaAndValue = toConnectData("{\"data\": \"value\"}");

    // Create SinkRecord directly without key to avoid SinkRecordBuilder's default key
    SinkRecord kafkaRecord =
        new SinkRecord(
            TOPIC,
            PARTITION,
            null, // keySchema
            null, // key
            schemaAndValue.schema(),
            schemaAndValue.value(),
            0,
            System.currentTimeMillis(),
            TimestampType.CREATE_TIME);

    SnowflakeSinkRecord record = SnowflakeSinkRecord.from(kafkaRecord, allDisabledConfig);

    Map<String, Object> metadata = record.getMetadata();
    assertFalse(metadata.containsKey("offset"));
    assertFalse(metadata.containsKey("partition"));
    assertFalse(metadata.containsKey("topic"));
    assertFalse(metadata.containsKey("CreateTime"));
    assertFalse(metadata.containsKey("SnowflakeConnectorPushTime"));
    assertFalse(metadata.containsKey("key"));
  }

  @Test
  void testMetadata_WhenAllFieldsExplicitlyDisabled_ContentWithMetadataHasNoMetadataColumn() {
    // Test that when ALL individual metadata fields are disabled AND there's no key/headers,
    // the metadata map is empty and not added to content
    Map<String, String> config = new HashMap<>();
    config.put("snowflake.metadata.all", "false");
    config.put("snowflake.metadata.createtime", "false");
    config.put("snowflake.metadata.topic", "false");
    config.put("snowflake.metadata.offset.and.partition", "false");
    config.put("snowflake.streaming.metadata.connectorPushTime", "false");
    SnowflakeMetadataConfig allDisabledConfig = new SnowflakeMetadataConfig(config);

    SchemaAndValue schemaAndValue = toConnectData("{\"data\": \"value\"}");

    // Create SinkRecord without key and without timestamp to ensure metadata is truly empty
    SinkRecord kafkaRecord =
        new SinkRecord(
            TOPIC,
            PARTITION,
            null, // keySchema
            null, // key
            schemaAndValue.schema(),
            schemaAndValue.value(),
            0,
            null, // no timestamp
            TimestampType.NO_TIMESTAMP_TYPE);

    SnowflakeSinkRecord record = SnowflakeSinkRecord.from(kafkaRecord, allDisabledConfig);

    // When ALL individual metadata fields are disabled and no key present, metadata should be empty
    assertTrue(record.getMetadata().isEmpty());

    // Even when includeAllMetadata is true, no metadata column should be added because metadata is
    // empty
    Map<String, Object> contentWithMetadata = record.getContentWithMetadata(true);
    assertFalse(contentWithMetadata.containsKey(TABLE_COLUMN_METADATA));
    assertEquals("value", contentWithMetadata.get("data"));
  }

  @Test
  void testTimestamp_WhenNoTimestampType_NotPresent() {
    // Test that timestamp is NOT present when TimestampType is NO_TIMESTAMP_TYPE
    Map<String, String> config = new HashMap<>();
    config.put("snowflake.metadata.createtime", "true");
    SnowflakeMetadataConfig timestampConfig = new SnowflakeMetadataConfig(config);

    // Create record without timestamp (NO_TIMESTAMP_TYPE is default in builder)
    SnowflakeSinkRecord record = createRecordFromJson("{\"data\": \"value\"}", timestampConfig);

    Map<String, Object> metadata = record.getMetadata();
    assertFalse(metadata.containsKey("CreateTime"));
    assertFalse(metadata.containsKey("LogAppendTime"));
  }

  private static JsonConverter createJsonConverter() {
    JsonConverter converter = new JsonConverter();
    converter.configure(Collections.singletonMap("schemas.enable", false), false);
    return converter;
  }

  private static Stream<Arguments> timestampTypeTestCases() {
    return Stream.of(
        Arguments.of(TimestampType.CREATE_TIME, "CreateTime"),
        Arguments.of(TimestampType.LOG_APPEND_TIME, "LogAppendTime"));
  }

  private SchemaAndValue toConnectData(String jsonPayload) {
    return jsonConverter.toConnectData(TOPIC, jsonPayload.getBytes(StandardCharsets.UTF_8));
  }

  private SnowflakeMetadataConfig createMetadataConfigWithAll() {
    Map<String, String> config = new HashMap<>();
    config.put("snowflake.metadata.all", "true");
    return new SnowflakeMetadataConfig(config);
  }

  private SnowflakeSinkRecord createRecordFromJson(String json, SnowflakeMetadataConfig config) {
    SchemaAndValue schemaAndValue = toConnectData(json);
    SinkRecord kafkaRecord =
        SinkRecordBuilder.forTopicPartition(TOPIC, PARTITION)
            .withSchemaAndValue(schemaAndValue)
            .build();
    return SnowflakeSinkRecord.from(kafkaRecord, config);
  }

  private SinkRecord createSinkRecordWithHeaders(
      SchemaAndValue schemaAndValue, Headers headers, String key) {
    return new SinkRecord(
        TOPIC,
        PARTITION,
        Schema.STRING_SCHEMA,
        key,
        schemaAndValue.schema(),
        schemaAndValue.value(),
        0,
        null,
        TimestampType.NO_TIMESTAMP_TYPE,
        headers);
  }
}
