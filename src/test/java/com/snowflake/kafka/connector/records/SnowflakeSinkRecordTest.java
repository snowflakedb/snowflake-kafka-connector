package com.snowflake.kafka.connector.records;

import static com.snowflake.kafka.connector.Utils.TABLE_COLUMN_METADATA;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.snowflake.kafka.connector.builder.SinkRecordBuilder;
import com.snowflake.kafka.connector.streaming.iceberg.IcebergDDLTypes;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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

    SnowflakeSinkRecord record = SnowflakeSinkRecord.from(kafkaRecord, metadataConfig, true, false);

    assertTrue(record.isValid());
    assertFalse(record.isBroken());
    assertFalse(record.isTombstone());
    assertNull(record.getBrokenReason());
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

    SnowflakeSinkRecord record = SnowflakeSinkRecord.from(kafkaRecord, metadataConfig, true, false);

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
    assertArrayEquals("foo".getBytes(), (byte[]) content.get("bytes"));
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

    SnowflakeSinkRecord record = SnowflakeSinkRecord.from(kafkaRecord, metadataConfig, true, false);

    assertFalse(record.isValid());
    assertFalse(record.isBroken());
    assertTrue(record.isTombstone());
    assertNull(record.getBrokenReason());
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

    SnowflakeSinkRecord record = SnowflakeSinkRecord.from(kafkaRecord, metadataConfig, true, false);

    assertFalse(record.isValid());
    assertTrue(record.isBroken());
    assertFalse(record.isTombstone());
    assertNotNull(record.getBrokenReason());
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

    SnowflakeSinkRecord record = SnowflakeSinkRecord.from(kafkaRecord, metadataConfig, true, false);

    // Record should be broken because convertToMap cannot handle plain String
    assertTrue(record.isBroken());
    assertFalse(record.isValid());
    assertFalse(record.isTombstone());
    assertNotNull(record.getBrokenReason());
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
        SnowflakeSinkRecord.from(kafkaRecord, createMetadataConfigWithAll(), true, false);

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
        SnowflakeSinkRecord.from(kafkaRecord, fullMetadataConfig, connectorPushTime, true, false);

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

    SnowflakeSinkRecord record =
        SnowflakeSinkRecord.from(kafkaRecord, timestampConfig, true, false);

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
        SnowflakeSinkRecord.from(
            kafkaRecord, createMetadataConfigWithAllAndStructuredHeaders(), true, false);

    Map<String, Object> metadata = record.getMetadata();
    assertNotNull(metadata.get("headers"));

    @SuppressWarnings("unchecked")
    Map<String, Object> headersMap = (Map<String, Object>) metadata.get("headers");
    assertEquals("testHeaderValue", headersMap.get("stringHeader"));
    assertEquals(42, headersMap.get("intHeader"));
    assertEquals(true, headersMap.get("boolHeader"));
  }

  @Test
  void testMetadataWithHeaders_LegacyStringFlattening() {
    // With structured headers disabled, every header value is flattened to a string (original KC v4
    // behavior).
    SchemaAndValue schemaAndValue = toConnectData("{\"data\": \"value\"}");

    Headers headers = new ConnectHeaders();
    headers.addString("stringHeader", "testHeaderValue");
    headers.addInt("intHeader", 42);
    headers.addBoolean("boolHeader", true);

    SinkRecord kafkaRecord = createSinkRecordWithHeaders(schemaAndValue, headers, "key");
    SnowflakeSinkRecord record =
        SnowflakeSinkRecord.from(kafkaRecord, createMetadataConfigWithAll(), true, false);

    @SuppressWarnings("unchecked")
    Map<String, Object> headersMap = (Map<String, Object>) record.getMetadata().get("headers");
    assertEquals("testHeaderValue", headersMap.get("stringHeader"));
    assertEquals("42", headersMap.get("intHeader"));
    assertEquals("true", headersMap.get("boolHeader"));
  }

  @Test
  void testMetadataWithStructuredObjectHeader() {
    // A structured (object) header value is preserved as a nested map, landing as a VARIANT object
    // rather than a stringified representation.
    SchemaAndValue schemaAndValue = toConnectData("{\"data\": \"value\"}");

    Schema objectSchema =
        SchemaBuilder.struct()
            .field("key1", Schema.STRING_SCHEMA)
            .field("key2", Schema.STRING_SCHEMA)
            .build();
    Struct objectValue = new Struct(objectSchema).put("key1", "value1").put("key2", "value2");

    Headers headers = new ConnectHeaders();
    headers.add("objectHeader", objectValue, objectSchema);

    SinkRecord kafkaRecord = createSinkRecordWithHeaders(schemaAndValue, headers, "key");
    SnowflakeSinkRecord record =
        SnowflakeSinkRecord.from(
            kafkaRecord, createMetadataConfigWithAllAndStructuredHeaders(), true, false);

    @SuppressWarnings("unchecked")
    Map<String, Object> headersMap = (Map<String, Object>) record.getMetadata().get("headers");
    assertInstanceOf(Map.class, headersMap.get("objectHeader"));
    @SuppressWarnings("unchecked")
    Map<String, Object> objectHeader = (Map<String, Object>) headersMap.get("objectHeader");
    assertEquals("value1", objectHeader.get("key1"));
    assertEquals("value2", objectHeader.get("key2"));
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
        SnowflakeSinkRecord.from(
            kafkaRecord, createMetadataConfigWithAllAndStructuredHeaders(), true, false);

    Map<String, Object> metadata = record.getMetadata();

    @SuppressWarnings("unchecked")
    Map<String, Object> headersMap = (Map<String, Object>) metadata.get("headers");
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

    SnowflakeSinkRecord record =
        SnowflakeSinkRecord.from(kafkaRecord, noCreateTimeConfig, true, false);

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

    SnowflakeSinkRecord record = SnowflakeSinkRecord.from(kafkaRecord, noTopicConfig, true, false);

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

    SnowflakeSinkRecord record =
        SnowflakeSinkRecord.from(kafkaRecord, noOffsetPartitionConfig, true, false);

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

    SnowflakeSinkRecord record =
        SnowflakeSinkRecord.from(kafkaRecord, allDisabledConfig, true, false);

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

    SnowflakeSinkRecord record =
        SnowflakeSinkRecord.from(kafkaRecord, allDisabledConfig, true, false);

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

  @Test
  void testLegacyMode_WithTimestampStruct_JacksonCanSerialize() {
    // Reproducer for PR review comment: when enableSchematization=false,
    // wrapAsRecordContent() serializes via plain ObjectMapper (no JavaTimeModule).
    // If convertToMap returns a raw Instant, MAPPER.writeValueAsString() will throw
    // InvalidDefinitionException.
    java.util.Date nearEpochDate =
        new java.util.Date(java.time.Instant.parse("1969-04-08T00:00:00Z").toEpochMilli());

    Schema schema =
        SchemaBuilder.struct().field("ts", org.apache.kafka.connect.data.Timestamp.SCHEMA).build();
    Struct struct = new Struct(schema).put("ts", nearEpochDate);

    SinkRecord kafkaRecord =
        SinkRecordBuilder.forTopicPartition(TOPIC, PARTITION)
            .withValueSchema(schema)
            .withValue(struct)
            .build();

    // enableSchematization=false triggers wrapAsRecordContent → Jackson serialization
    SnowflakeSinkRecord record =
        SnowflakeSinkRecord.from(kafkaRecord, metadataConfig, false, false);

    // Must not be broken — Jackson must be able to serialize the converted value
    assertFalse(
        record.isBroken(), "Record should not be broken but was: " + record.getBrokenReason());
    assertTrue(record.isValid());
    assertTrue(record.getContent().containsKey("RECORD_CONTENT"));
  }

  @Test
  void testLegacyMode_WithJsonMap_WrapsInRecordContent() {
    SchemaAndValue schemaAndValue = toConnectData("{\"name\": \"test\", \"value\": 123}");
    SinkRecord kafkaRecord =
        SinkRecordBuilder.forTopicPartition(TOPIC, PARTITION)
            .withSchemaAndValue(schemaAndValue)
            .build();

    SnowflakeSinkRecord record = SnowflakeSinkRecord.from(kafkaRecord, metadataConfig, false, true);

    assertTrue(record.isValid());
    Map<String, Object> content = record.getContent();
    assertTrue(content.containsKey("RECORD_CONTENT"));
    assertEquals(1, content.size());
    @SuppressWarnings("unchecked")
    Map<String, Object> recordContent = (Map<String, Object>) content.get("RECORD_CONTENT");
    assertEquals("test", recordContent.get("name"));
    assertEquals(123L, recordContent.get("value"));
  }

  @Test
  void testLegacyMode_WithPlainString_WrapsInRecordContent() {
    SinkRecord kafkaRecord =
        SinkRecordBuilder.forTopicPartition(TOPIC, PARTITION)
            .withValueSchema(Schema.STRING_SCHEMA)
            .withValue("just a plain string")
            .build();

    SnowflakeSinkRecord record = SnowflakeSinkRecord.from(kafkaRecord, metadataConfig, false, true);

    assertTrue(record.isValid());
    Map<String, Object> content = record.getContent();
    assertTrue(content.containsKey("RECORD_CONTENT"));
    assertEquals("just a plain string", content.get("RECORD_CONTENT"));
  }

  @Test
  void testLegacyMode_WithByteArray_WrapsInRecordContent() {
    byte[] bytes = "hello".getBytes(java.nio.charset.StandardCharsets.UTF_8);
    SinkRecord kafkaRecord =
        SinkRecordBuilder.forTopicPartition(TOPIC, PARTITION)
            .withValueSchema(Schema.BYTES_SCHEMA)
            .withValue(bytes)
            .build();

    SnowflakeSinkRecord record = SnowflakeSinkRecord.from(kafkaRecord, metadataConfig, false, true);

    assertTrue(record.isValid());
    Map<String, Object> content = record.getContent();
    assertTrue(content.containsKey("RECORD_CONTENT"));
    assertArrayEquals(bytes, (byte[]) content.get("RECORD_CONTENT"));
  }

  @Test
  void testLegacyMode_TombstoneStillWorks() {
    SinkRecord kafkaRecord =
        SinkRecordBuilder.forTopicPartition(TOPIC, PARTITION)
            .withValueSchema(null)
            .withValue(null)
            .build();

    SnowflakeSinkRecord record = SnowflakeSinkRecord.from(kafkaRecord, metadataConfig, false, true);

    assertTrue(record.isTombstone());
  }

  @Test
  void testSchematizedMode_WithPlainString_StillBroken() {
    SinkRecord kafkaRecord =
        SinkRecordBuilder.forTopicPartition(TOPIC, PARTITION)
            .withValueSchema(Schema.STRING_SCHEMA)
            .withValue("just a plain string")
            .build();

    SnowflakeSinkRecord record = SnowflakeSinkRecord.from(kafkaRecord, metadataConfig, true, false);

    assertTrue(record.isBroken());
  }

  @Test
  void testNormalizationEnabled_UppercasesColumnNames() {
    SchemaAndValue schemaAndValue = toConnectData("{\"city\": \"Hsinchu\", \"Country\": \"TW\"}");
    SinkRecord kafkaRecord =
        SinkRecordBuilder.forTopicPartition(TOPIC, PARTITION)
            .withSchemaAndValue(schemaAndValue)
            .build();

    SnowflakeSinkRecord record = SnowflakeSinkRecord.from(kafkaRecord, metadataConfig, true, true);

    assertTrue(record.isValid());
    Map<String, Object> content = record.getContent();
    // Unquoted identifiers are uppercased
    assertTrue(content.containsKey("CITY"));
    assertTrue(content.containsKey("COUNTRY"));
    assertFalse(content.containsKey("city"));
    assertFalse(content.containsKey("Country"));
  }

  @Test
  void testNormalizationDisabled_PreservesColumnNames() {
    SchemaAndValue schemaAndValue = toConnectData("{\"city\": \"Hsinchu\", \"Country\": \"TW\"}");
    SinkRecord kafkaRecord =
        SinkRecordBuilder.forTopicPartition(TOPIC, PARTITION)
            .withSchemaAndValue(schemaAndValue)
            .build();

    SnowflakeSinkRecord record = SnowflakeSinkRecord.from(kafkaRecord, metadataConfig, true, false);

    assertTrue(record.isValid());
    Map<String, Object> content = record.getContent();
    // Column names preserved as-is
    assertTrue(content.containsKey("city"));
    assertTrue(content.containsKey("Country"));
    assertFalse(content.containsKey("CITY"));
    assertFalse(content.containsKey("COUNTRY"));
  }

  @Test
  void testNormalizationEnabled_QuotedIdentifierPreservesCase() {
    // Quoted SQL identifiers strip quotes and preserve case
    Schema schema =
        SchemaBuilder.struct()
            .field("\"MyCol\"", Schema.STRING_SCHEMA)
            .field("simple", Schema.STRING_SCHEMA)
            .build();
    Struct struct = new Struct(schema).put("\"MyCol\"", "value1").put("simple", "value2");

    SinkRecord kafkaRecord =
        SinkRecordBuilder.forTopicPartition(TOPIC, PARTITION)
            .withValueSchema(schema)
            .withValue(struct)
            .build();

    SnowflakeSinkRecord record = SnowflakeSinkRecord.from(kafkaRecord, metadataConfig, true, true);

    assertTrue(record.isValid());
    Map<String, Object> content = record.getContent();
    // Quoted "MyCol" → strips quotes → MyCol (case preserved)
    assertTrue(content.containsKey("MyCol"));
    // Unquoted simple → SIMPLE (uppercased)
    assertTrue(content.containsKey("SIMPLE"));

    // Schema field names should also be normalized
    Schema normalizedSchema = record.getSchema();
    assertNotNull(normalizedSchema);
    assertNotNull(normalizedSchema.field("MyCol"));
    assertNotNull(normalizedSchema.field("SIMPLE"));
    assertNull(normalizedSchema.field("\"MyCol\""));
    assertNull(normalizedSchema.field("simple"));
  }

  @Test
  void testIcebergMetadata_padsKeyAndHeadersOnly_whenAbsent() {
    // KEY and HEADERS are record-level (not config-gated): a keyless/headerless record omits them
    // even with all metadata flags on. The strict typed-OBJECT cast rejects a *missing* schema
    // field, so conform pads exactly these two with null (the config-gated fields are guaranteed
    // present by config-time validation and are not padded).
    SchemaAndValue schemaAndValue = toConnectData("{\"id\": 1}");
    SinkRecord kafkaRecord =
        new SinkRecord(
            TOPIC,
            PARTITION,
            null,
            null,
            schemaAndValue.schema(),
            schemaAndValue.value(),
            5L,
            1609459200000L,
            TimestampType.CREATE_TIME);

    SnowflakeSinkRecord record =
        SnowflakeSinkRecord.from(kafkaRecord, createMetadataConfigWithAll(), true, false);

    @SuppressWarnings("unchecked")
    Map<String, Object> metadata =
        (Map<String, Object>) record.getContentWithMetadata(true, true).get(TABLE_COLUMN_METADATA);

    // Populated config-gated fields are retained.
    assertEquals(TOPIC, metadata.get("topic"));
    assertEquals(5L, metadata.get("offset"));
    assertEquals(PARTITION, metadata.get("partition"));
    assertEquals(1609459200000L, metadata.get("CreateTime"));
    // KEY and HEADERS are absent from the record, so they must be present-and-null (padded), not
    // missing — otherwise the strict Iceberg cast would reject the row.
    assertTrue(metadata.containsKey("key"), "absent key must be padded with null");
    assertNull(metadata.get("key"));
    assertTrue(metadata.containsKey("headers"), "absent headers must be padded with null");
    assertNull(metadata.get("headers"));

    // No fields outside the Iceberg schema.
    assertTrue(
        SnowflakeSinkRecord.ICEBERG_METADATA_FIELDS.containsAll(metadata.keySet()),
        "conformed map must not contain fields outside ICEBERG_METADATA_FIELDS");
  }

  @Test
  void testIcebergMetadata_normalizesLogAppendTimeToCreateTime() {
    // Kafka emits the timestamp under TimestampType.name ("LogAppendTime"); the Iceberg schema only
    // declares "CreateTime", so an unnormalized key would be both an extra field AND a missing one.
    SchemaAndValue schemaAndValue = toConnectData("{\"id\": 1}");
    SinkRecord kafkaRecord =
        new SinkRecord(
            TOPIC,
            PARTITION,
            null,
            null,
            schemaAndValue.schema(),
            schemaAndValue.value(),
            0L,
            123L,
            TimestampType.LOG_APPEND_TIME);

    SnowflakeSinkRecord record =
        SnowflakeSinkRecord.from(kafkaRecord, createMetadataConfigWithAll(), true, false);

    @SuppressWarnings("unchecked")
    Map<String, Object> metadata =
        (Map<String, Object>) record.getContentWithMetadata(true, true).get(TABLE_COLUMN_METADATA);

    assertEquals(123L, metadata.get("CreateTime"));
    assertFalse(metadata.containsKey("LogAppendTime"));
  }

  @Test
  void testFdnMetadata_isNotConformedToIcebergSchema() {
    // FDN path (conform=false): RECORD_METADATA is VARIANT, so the sparse map is left as-is.
    SchemaAndValue schemaAndValue = toConnectData("{\"id\": 1}");
    SinkRecord kafkaRecord =
        new SinkRecord(
            TOPIC,
            PARTITION,
            null,
            null,
            schemaAndValue.schema(),
            schemaAndValue.value(),
            0L,
            123L,
            TimestampType.CREATE_TIME);

    SnowflakeSinkRecord record =
        SnowflakeSinkRecord.from(kafkaRecord, createMetadataConfigWithAll(), true, false);

    @SuppressWarnings("unchecked")
    Map<String, Object> metadata =
        (Map<String, Object>) record.getContentWithMetadata(true, false).get(TABLE_COLUMN_METADATA);

    // Absent fields stay absent (not null-filled) — FDN behavior unchanged.
    assertFalse(metadata.containsKey("key"));
    assertFalse(metadata.containsKey("headers"));
  }

  @Test
  void testIcebergMetadata_coercesNonStringKeyToString() {
    // The Iceberg schema declares `key STRING`; an INT-keyed topic yields an Integer key, which
    // would fail the strict typed-OBJECT cast. Conform must coerce it to a String.
    SchemaAndValue schemaAndValue = toConnectData("{\"id\": 1}");
    SinkRecord kafkaRecord =
        SinkRecordBuilder.forTopicPartition(TOPIC, PARTITION)
            .withKeySchema(Schema.INT32_SCHEMA)
            .withKey(123)
            .withSchemaAndValue(schemaAndValue)
            .build();

    SnowflakeSinkRecord record =
        SnowflakeSinkRecord.from(kafkaRecord, createMetadataConfigWithAll(), true, false);

    @SuppressWarnings("unchecked")
    Map<String, Object> metadata =
        (Map<String, Object>) record.getContentWithMetadata(true, true).get(TABLE_COLUMN_METADATA);

    assertTrue(metadata.get("key") instanceof String, "key must be coerced to String for Iceberg");
    assertEquals("123", metadata.get("key"));
  }

  @Test
  void testIcebergMetadataFields_matchDdlSchema() {
    // ICEBERG_METADATA_FIELDS must exactly match the fields declared in
    // IcebergDDLTypes.ICEBERG_METADATA_OBJECT_SCHEMA — the strict typed-OBJECT cast makes this an
    // exact-match invariant, so a schema change without updating the field list would silently
    // break ingestion. This test fails loudly on drift.
    String schema = IcebergDDLTypes.ICEBERG_METADATA_OBJECT_SCHEMA;
    String body = schema.substring(schema.indexOf('(') + 1, schema.lastIndexOf(')'));

    // Split on top-level commas only (the `headers MAP(VARCHAR, VARCHAR)` entry has nested commas).
    Set<String> declaredFields = new HashSet<>();
    int depth = 0;
    StringBuilder cur = new StringBuilder();
    for (char c : body.toCharArray()) {
      if (c == '(') {
        depth++;
      } else if (c == ')') {
        depth--;
      }
      if (c == ',' && depth == 0) {
        declaredFields.add(cur.toString().trim().split("\\s+")[0]);
        cur.setLength(0);
      } else {
        cur.append(c);
      }
    }
    if (cur.toString().trim().length() > 0) {
      declaredFields.add(cur.toString().trim().split("\\s+")[0]);
    }

    assertEquals(
        new HashSet<>(SnowflakeSinkRecord.ICEBERG_METADATA_FIELDS),
        declaredFields,
        "ICEBERG_METADATA_FIELDS is out of sync with ICEBERG_METADATA_OBJECT_SCHEMA");
  }

  // ---- conformIcebergMetadata post-Change-2 behavior tests ----

  @Test
  void testIcebergMetadata_createTimePassthrough() {
    // A record with CREATE_TIME timestamp must pass through unchanged (no rename needed).
    SchemaAndValue schemaAndValue = toConnectData("{\"id\": 1}");
    long ts = 1_609_459_200_000L;
    SinkRecord kafkaRecord =
        new SinkRecord(
            TOPIC,
            PARTITION,
            null,
            null,
            schemaAndValue.schema(),
            schemaAndValue.value(),
            1L,
            ts,
            TimestampType.CREATE_TIME);

    SnowflakeSinkRecord record =
        SnowflakeSinkRecord.from(kafkaRecord, createMetadataConfigWithAll(), true, false);

    @SuppressWarnings("unchecked")
    Map<String, Object> metadata =
        (Map<String, Object>) record.getContentWithMetadata(true, true).get(TABLE_COLUMN_METADATA);

    assertEquals(ts, metadata.get("CreateTime"), "CreateTime value must pass through");
    assertFalse(metadata.containsKey("LogAppendTime"), "LogAppendTime must not be present");
  }

  @Test
  void testIcebergMetadata_unknownFieldThrows() {
    // buildMetadata only ever emits fields declared in ICEBERG_METADATA_FIELDS, so an out-of-schema
    // field means KC metadata emission and the Iceberg RECORD_METADATA schema have drifted apart.
    // conformIcebergMetadata surfaces that loudly (IllegalStateException) rather than silently
    // dropping the field. No public path can inject a top-level metadata field outside the schema,
    // so we call the (package-private) method directly with a crafted map.
    Map<String, Object> raw = new HashMap<>();
    raw.put("topic", TOPIC);
    raw.put("unexpectedField", "x");

    IllegalStateException ex =
        assertThrows(
            IllegalStateException.class, () -> SnowflakeSinkRecord.conformIcebergMetadata(raw));
    assertTrue(
        ex.getMessage().contains("unexpectedField"),
        "message should name the offending field; got: " + ex.getMessage());
  }

  @Test
  void testIcebergMetadata_fullMetadataMapPassesThroughUnchanged() {
    // With full metadata enabled (topic, offset+partition, createtime, connectorPushTime, key,
    // headers), every ICEBERG_METADATA_FIELDS entry should be populated and present after conform.
    Map<String, String> cfg = new HashMap<>();
    cfg.put("snowflake.metadata.all", "true");
    cfg.put("snowflake.streaming.metadata.connectorPushTime", "true");
    SnowflakeMetadataConfig fullConfig = new SnowflakeMetadataConfig(cfg);

    SchemaAndValue schemaAndValue = toConnectData("{\"id\": 1}");
    long ts = 1_609_459_200_000L;
    SinkRecord kafkaRecord =
        new SinkRecord(
            TOPIC,
            PARTITION,
            Schema.STRING_SCHEMA,
            "myKey",
            schemaAndValue.schema(),
            schemaAndValue.value(),
            7L,
            ts,
            TimestampType.CREATE_TIME);

    Instant connectorPushTime = Instant.ofEpochMilli(9_876_543_210L);
    SnowflakeSinkRecord record =
        SnowflakeSinkRecord.from(kafkaRecord, fullConfig, connectorPushTime, true, false);

    @SuppressWarnings("unchecked")
    Map<String, Object> metadata =
        (Map<String, Object>) record.getContentWithMetadata(true, true).get(TABLE_COLUMN_METADATA);

    assertEquals(TOPIC, metadata.get("topic"));
    assertEquals(7L, metadata.get("offset"));
    assertEquals(PARTITION, metadata.get("partition"));
    assertEquals("myKey", metadata.get("key"));
    assertEquals(ts, metadata.get("CreateTime"));
    assertEquals(connectorPushTime.toEpochMilli(), metadata.get("SnowflakeConnectorPushTime"));
    // All populated fields are within the Iceberg schema.
    assertTrue(SnowflakeSinkRecord.ICEBERG_METADATA_FIELDS.containsAll(metadata.keySet()));
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

  private SnowflakeMetadataConfig createMetadataConfigWithAllAndStructuredHeaders() {
    Map<String, String> config = new HashMap<>();
    config.put("snowflake.metadata.all", "true");
    config.put("snowflake.feature.structured.headers", "true");
    return new SnowflakeMetadataConfig(config);
  }

  private SnowflakeSinkRecord createRecordFromJson(String json, SnowflakeMetadataConfig config) {
    SchemaAndValue schemaAndValue = toConnectData(json);
    SinkRecord kafkaRecord =
        SinkRecordBuilder.forTopicPartition(TOPIC, PARTITION)
            .withSchemaAndValue(schemaAndValue)
            .build();
    return SnowflakeSinkRecord.from(kafkaRecord, config, true, false);
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
