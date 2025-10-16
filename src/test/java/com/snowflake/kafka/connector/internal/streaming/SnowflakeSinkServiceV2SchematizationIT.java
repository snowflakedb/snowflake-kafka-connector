package com.snowflake.kafka.connector.internal.streaming;

import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.ENABLE_SCHEMATIZATION_CONFIG;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_CONFIG;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.ERRORS_TOLERANCE_CONFIG;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.SNOWPIPE_STREAMING_MAX_CLIENT_LAG;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.SNOWPIPE_STREAMING_V2_ENABLED;
import static com.snowflake.kafka.connector.internal.streaming.channel.TopicPartitionChannel.NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE;
import static org.awaitility.Awaitility.await;

import com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig;
import com.snowflake.kafka.connector.builder.SinkRecordBuilder;
import com.snowflake.kafka.connector.dlq.InMemoryKafkaRecordErrorReporter;
import com.snowflake.kafka.connector.internal.SchematizationTestUtils;
import com.snowflake.kafka.connector.internal.SnowflakeConnectionService;
import com.snowflake.kafka.connector.internal.SnowflakeErrors;
import com.snowflake.kafka.connector.internal.SnowflakeSinkService;
import com.snowflake.kafka.connector.internal.TestUtils;
import com.snowflake.kafka.connector.internal.streaming.v2.PipeNameProvider;
import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class SnowflakeSinkServiceV2SchematizationIT extends SnowflakeSinkServiceV2BaseIT {

  private final SnowflakeConnectionService conn = TestUtils.getConnectionServiceForStreaming();
  private Map<String, String> config;
  private SnowflakeSinkService service;
  private String pipe;

  @BeforeEach
  public void setup() {
    config = TestUtils.getConfForStreaming();
    config.put(ENABLE_SCHEMATIZATION_CONFIG, "true");
    config.put(
        SnowflakeSinkConnectorConfig.VALUE_CONVERTER_CONFIG_FIELD,
        "org.apache.kafka.connect.json.JsonConverter");
    config.put(SnowflakeSinkConnectorConfig.VALUE_SCHEMA_REGISTRY_CONFIG_FIELD, "http://fake-url");
    config.put("schemas.enable", "false");
    config.put(ERRORS_TOLERANCE_CONFIG, "all");
    config.put(ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_CONFIG, "dlq_topic");
    pipe = PipeNameProvider.pipeName(config, table);
  }

  @AfterEach
  public void teardown() {
    service.closeAll();
    TestUtils.dropTable(table);
    TestUtils.dropPipe(pipe);
  }

  @Test
  public void testSchematizationWithTableCreationAndJsonInput() throws Exception {
    SinkRecord jsonRecordValue = createComplexTestRecord(partition, 0);

    service =
        StreamingSinkServiceBuilder.builder(conn, config)
            .withSinkTaskContext(new InMemorySinkTaskContext(Collections.singleton(topicPartition)))
            .build();
    service.startPartition(table, topicPartition);

    // The first insert should fail and schema evolution will kick in to update the schema
    service.insert(Collections.singletonList(jsonRecordValue));
    TestUtils.assertWithRetry(
        () -> service.getOffset(topicPartition) == NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE, 20, 5);
    TestUtils.checkTableSchema(table, SchematizationTestUtils.SF_JSON_SCHEMA_FOR_TABLE_CREATION);

    // Retry the insert should succeed now with the updated schema
    service.insert(Collections.singletonList(jsonRecordValue));
    TestUtils.assertWithRetry(() -> service.getOffset(topicPartition) == 1, 20, 5);

    TestUtils.checkTableContentOneRow(
        table, SchematizationTestUtils.CONTENT_FOR_JSON_TABLE_CREATION);
  }

  @Test
  public void testSchemaEvolutionNotAvailableInSsv2() {
    // given
    config.put(SNOWPIPE_STREAMING_V2_ENABLED, "true");
    SinkRecord jsonRecordValue = createComplexTestRecord(partition, 0);
    InMemoryKafkaRecordErrorReporter errorReporter = new InMemoryKafkaRecordErrorReporter();
    service =
        StreamingSinkServiceBuilder.builder(conn, config)
            .withSinkTaskContext(new InMemorySinkTaskContext(Collections.singleton(topicPartition)))
            .withErrorReporter(errorReporter)
            .build();
    service.startPartition(table, topicPartition);

    // when
    service.insert(Collections.singletonList(jsonRecordValue));

    // then
    Assertions.assertEquals(1, errorReporter.getReportedRecords().size());
  }

  @Test
  public void testSchematizationSchemaEvolutionWithNonNullableColumn() throws Exception {
    SinkRecord jsonRecordValue = recordForNullabilityTest(0);

    service =
        StreamingSinkServiceBuilder.builder(conn, config)
            .withSinkTaskContext(new InMemorySinkTaskContext(Collections.singleton(topicPartition)))
            .build();
    service.startPartition(table, topicPartition);

    // The first insert should fail and schema evolution will kick in to add the column
    service.insert(Collections.singletonList(jsonRecordValue));
    TestUtils.assertWithRetry(
        () -> service.getOffset(topicPartition) == NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE, 20, 5);

    // The second insert should fail again and schema evolution will kick in to update the
    // first not-nullable column nullability
    service.insert(Collections.singletonList(jsonRecordValue));
    TestUtils.assertWithRetry(
        () -> service.getOffset(topicPartition) == NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE, 20, 5);

    // The third insert should fail again and schema evolution will kick in to update the
    // second not-nullable column nullability
    service.insert(Collections.singletonList(jsonRecordValue));
    TestUtils.assertWithRetry(
        () -> service.getOffset(topicPartition) == NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE, 20, 5);

    // Retry the insert should succeed now with the updated schema
    service.insert(Collections.singletonList(jsonRecordValue));
    TestUtils.assertWithRetry(() -> service.getOffset(topicPartition) == 1, 20, 5);
  }

  @Test
  void testSkippingOffsetsInSchemaEvolution() throws Exception {
    long maxClientLagSeconds = 1L;
    long schemaEvolutionDelayMs = 3 * 1000L; // must be enough for sdk to flush and commit
    long assertionSleepTimeMs = 6 * 1000L;

    config.put(SNOWPIPE_STREAMING_MAX_CLIENT_LAG, String.valueOf(maxClientLagSeconds));

    // setup a table with a single field
    conn.createTableWithOnlyMetadataColumn(table);
    createNonNullableColumn(table, "id_int8", "int");

    service =
        StreamingSinkServiceBuilder.builder(conn, config)
            .withSinkTaskContext(new InMemorySinkTaskContext(Collections.singleton(topicPartition)))
            .withSchemaEvolutionService(
                new DelayedSchemaEvolutionService(conn, schemaEvolutionDelayMs))
            .build();
    service.startPartition(table, topicPartition);

    service.insert(
        Arrays.asList(
            recordWithSingleField(partition, 0),
            recordWithSingleField(partition, 1),
            recordWithTwoFields(partition, 2),
            recordWithTwoFields(partition, 3)));

    // wait for processing all records and running ingest sdk thread
    Thread.sleep(assertionSleepTimeMs);

    // records 0 and 1 are ingested, 2 triggers schema evolution, 3 is skipped
    // getOffset() result is returned from preCommit() so Kafka will send next record starting from
    // this offset
    await().atMost(10, TimeUnit.SECONDS).until(() -> service.getOffset(topicPartition) == 2);

    // Kafka sends remaining messages
    service.insert(
        Arrays.asList(recordWithTwoFields(partition, 2), recordWithTwoFields(partition, 3)));

    await().atMost(10, TimeUnit.SECONDS).until(() -> service.getOffset(topicPartition) == 4);
  }

  @Test
  public void snowflakeSinkTask_put_whenJsonRecordCannotBeSchematized_sendRecordToDLQ() {
    // given
    config.put(SNOWPIPE_STREAMING_V2_ENABLED, "true");

    InMemoryKafkaRecordErrorReporter errorReporter = new InMemoryKafkaRecordErrorReporter();

    service =
        StreamingSinkServiceBuilder.builder(conn, config)
            .withSinkTaskContext(new InMemorySinkTaskContext(Collections.singleton(topicPartition)))
            .withErrorReporter(errorReporter)
            .build();
    service.startPartition(table, topicPartition);

    // Create a record that cannot be schematized (array at root level)
    String notSchematizeableJsonRecord = "[{\"name\":\"sf\",\"answer\":42}]";
    SinkRecord record = createKafkaRecordWithoutSchema(notSchematizeableJsonRecord, 0);

    // when
    service.insert(record);

    // then
    Assertions.assertEquals(1, errorReporter.getReportedRecords().size());
  }

  @Test
  void shouldSendRecordToDlqIfSchemaNotMatched() {
    // given
    config.put(SNOWPIPE_STREAMING_V2_ENABLED, "true");

    conn.createTableWithOnlyMetadataColumn(table);
    createNonNullableColumn(table, "\"ID_INT8\"", "boolean");

    Schema schema = SchemaBuilder.struct().field("id_int8", Schema.INT8_SCHEMA).build();
    Struct struct = new Struct(schema).put("id_int8", (byte) 2);
    // 2 cannot be cast to boolean
    SinkRecord invalidBooleanRecord = createKafkaRecordWithoutSchema(partition, 0, struct);

    InMemoryKafkaRecordErrorReporter errorReporter = new InMemoryKafkaRecordErrorReporter();
    service =
        StreamingSinkServiceBuilder.builder(conn, config)
            .withSinkTaskContext(new InMemorySinkTaskContext(Collections.singleton(topicPartition)))
            .withErrorReporter(errorReporter)
            .build();
    service.startPartition(table, topicPartition);

    // when
    service.insert(invalidBooleanRecord);

    // then
    Assertions.assertEquals(1, errorReporter.getReportedRecords().size());
  }

  /**
   * Test for SNOW-2266941: Unable to insert timestamp (google.protobuf.Timestamp) type into regular
   * Snowflake table (via JSON with schema). This test validates that timestamp logical types are
   * handled correctly for normal Snowflake tables.
   */
  @Test
  public void testTimestampLogicalTypeSchemaEvolution() throws Exception {
    // Create table with only metadata column
    conn.createTableWithOnlyMetadataColumn(table);

    service =
        StreamingSinkServiceBuilder.builder(conn, config)
            .withSinkTaskContext(new InMemorySinkTaskContext(Collections.singleton(topicPartition)))
            .build();
    service.startPartition(table, topicPartition);

    SinkRecord timestampRecord = createKafkaRecordWithSchema(timestampWithSchemaExample(), 0);
    service.insert(Collections.singletonList(timestampRecord));

    // Wait for schema evolution to complete
    TestUtils.assertWithRetry(
        () -> service.getOffset(topicPartition) == NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE, 20, 5);

    Map<String, String> expectedSchema =
        Map.of("RECORD_METADATA", "VARIANT", "TIMESTAMP_RECEIVED", "TIMESTAMP_NTZ");
    TestUtils.checkTableSchema(table, expectedSchema);

    // Retry the insert should succeed now with the updated schema
    service.insert(Collections.singletonList(timestampRecord));
    TestUtils.assertWithRetry(() -> service.getOffset(topicPartition) == 1, 20, 5);

    // Verify the timestamp content was inserted correctly
    Map<String, Object> expectedContent = new HashMap<>();
    expectedContent.put("RECORD_METADATA", "RECORD_METADATA_PLACE_HOLDER");
    expectedContent.put(
        "TIMESTAMP_RECEIVED", java.sql.Timestamp.valueOf("2023-01-01 00:00:00.000"));
    TestUtils.checkTableContentOneRow(table, expectedContent);

    // Insert another record with the same schema to ensure it works consistently
    SinkRecord timestampRecord2 = createKafkaRecordWithSchema(timestampWithSchemaExample(), 1);
    service.insert(Collections.singletonList(timestampRecord2));
    TestUtils.assertWithRetry(() -> service.getOffset(topicPartition) == 2, 20, 5);
  }

  private SinkRecord createComplexTestRecord(int partition, long offset) {
    SchemaBuilder schemaBuilder =
        SchemaBuilder.struct()
            .field("id_int8", Schema.INT8_SCHEMA)
            .field("id_int8_optional", Schema.OPTIONAL_INT8_SCHEMA)
            .field("id_int16", Schema.INT16_SCHEMA)
            .field("\"id_int32_double_quotes\"", Schema.INT32_SCHEMA)
            .field("id_int64", Schema.INT64_SCHEMA)
            .field("first_name", Schema.STRING_SCHEMA)
            .field("rating_float32", Schema.FLOAT32_SCHEMA)
            .field("rating_float64", Schema.FLOAT64_SCHEMA)
            .field("approval", Schema.BOOLEAN_SCHEMA)
            .field("info_array", SchemaBuilder.array(Schema.STRING_SCHEMA).build())
            .field(
                "info_map", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).build());

    Struct original =
        new Struct(schemaBuilder.build())
            .put("id_int8", (byte) 0)
            .put("id_int16", (short) 42)
            .put("\"id_int32_double_quotes\"", 42)
            .put("id_int64", 42L)
            .put("first_name", "zekai")
            .put("rating_float32", 0.99f)
            .put("rating_float64", 0.99d)
            .put("approval", true)
            .put("info_array", Arrays.asList("a", "b"))
            .put("info_map", Collections.singletonMap("field", 3));

    JsonConverter jsonConverter = new JsonConverter();
    jsonConverter.configure(config, false);
    byte[] converted = jsonConverter.fromConnectData(topic, original.schema(), original);

    SchemaAndValue jsonInputValue = jsonConverter.toConnectData(topic, converted);

    return new SinkRecord(
        topic,
        partition,
        Schema.STRING_SCHEMA,
        "test",
        jsonInputValue.schema(),
        jsonInputValue.value(),
        offset);
  }

  private SinkRecord recordForNullabilityTest(long offset) {
    Schema schema =
        SchemaBuilder.struct()
            .field("id_int8", Schema.INT8_SCHEMA)
            .field("id_int8_non_nullable_null_value", Schema.OPTIONAL_INT8_SCHEMA)
            .build();
    Struct original =
        new Struct(schema).put("id_int8", (byte) 0).put("id_int8_non_nullable_null_value", null);

    JsonConverter jsonConverter = new JsonConverter();
    jsonConverter.configure(config, false);
    byte[] converted = jsonConverter.fromConnectData(topic, original.schema(), original);
    conn.createTableWithOnlyMetadataColumn(table);
    createNonNullableColumn(table, "id_int8_non_nullable_missing_value", "int");
    createNonNullableColumn(table, "id_int8_non_nullable_null_value", "int");

    SchemaAndValue jsonInputValue = jsonConverter.toConnectData(topic, converted);

    return new SinkRecord(
        topic,
        partition,
        Schema.STRING_SCHEMA,
        "test",
        jsonInputValue.schema(),
        jsonInputValue.value(),
        offset);
  }

  private SinkRecord recordWithSingleField(int partition, long offset) {
    Schema schema = SchemaBuilder.struct().field("id_int8", Schema.INT8_SCHEMA).build();
    Struct struct = new Struct(schema).put("id_int8", (byte) 0);
    return createKafkaRecordWithoutSchema(partition, offset, struct);
  }

  private SinkRecord recordWithTwoFields(int partition, long offset) {
    Schema schema =
        SchemaBuilder.struct()
            .field("id_int8", Schema.INT8_SCHEMA)
            .field("id_int8_2", Schema.INT8_SCHEMA)
            .build();
    Struct struct = new Struct(schema).put("id_int8", (byte) 0).put("id_int8_2", (byte) 0);
    return createKafkaRecordWithoutSchema(partition, offset, struct);
  }

  private SinkRecord createKafkaRecordWithoutSchema(int partition, long offset, Struct struct) {
    JsonConverter jsonConverter = new JsonConverter();
    Map<String, String> converterConfig = new HashMap<>();
    converterConfig.put("schemas.enable", "false");
    jsonConverter.configure(converterConfig, false);

    // Convert Struct -> JSON -> SchemaAndValue for consistent test behavior
    byte[] converted = jsonConverter.fromConnectData(topic, struct.schema(), struct);
    SchemaAndValue jsonInputValue = jsonConverter.toConnectData(topic, converted);

    return SinkRecordBuilder.forTopicPartition(topic, partition)
        .withSchemaAndValue(jsonInputValue)
        .withOffset(offset)
        .withKey("test")
        .build();
  }

  private void createNonNullableColumn(String tableName, String colName, String colDataType) {
    String createTableQuery =
        "alter table identifier(?) add " + colName + " " + colDataType + " not null";

    try {
      PreparedStatement stmt = conn.getConnection().prepareStatement(createTableQuery);
      stmt.setString(1, tableName);
      stmt.setString(2, colName);
      stmt.execute();
      stmt.close();
    } catch (SQLException e) {
      throw SnowflakeErrors.ERROR_2007.getException(e);
    }
  }

  /** Helper method to create a Kafka record from JSON string */
  private SinkRecord createKafkaRecord(String jsonWithSchema, long offset, boolean withSchema) {
    JsonConverter jsonConverter = new JsonConverter();
    Map<String, String> converterConfig = new HashMap<>();
    converterConfig.put("schemas.enable", String.valueOf(withSchema));
    jsonConverter.configure(converterConfig, false);

    byte[] valueBytes = jsonWithSchema.getBytes(StandardCharsets.UTF_8);
    SchemaAndValue schemaAndValue = jsonConverter.toConnectData(topic, valueBytes);

    return SinkRecordBuilder.forTopicPartition(topic, partition)
        .withSchemaAndValue(schemaAndValue)
        .withOffset(offset)
        .withKey("test")
        .build();
  }

  /**
   * Convenience method to create a Kafka record from JSON without schema (schemas.enable = false)
   */
  private SinkRecord createKafkaRecordWithoutSchema(String jsonPayload, long offset) {
    return createKafkaRecord(jsonPayload, offset, false);
  }

  /** Convenience method to create a Kafka record from JSON with schema (schemas.enable = true) */
  private SinkRecord createKafkaRecordWithSchema(String jsonWithSchema, long offset) {
    return createKafkaRecord(jsonWithSchema, offset, true);
  }

  private String timestampWithSchemaExample() {
    return "{ \"schema\": { \"type\": \"struct\", \"fields\": [{  \"field\" :"
        + " \"timestamp_received\", \"type\" : \"int64\", \"name\" :"
        + " \"org.apache.kafka.connect.data.Timestamp\", \"version\" : 1  }]}, \"payload\":"
        + " {\"timestamp_received\" : 1672531200000 }}";
  }
}
