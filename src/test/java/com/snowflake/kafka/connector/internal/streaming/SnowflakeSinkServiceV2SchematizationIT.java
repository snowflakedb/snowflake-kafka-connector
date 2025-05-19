package com.snowflake.kafka.connector.internal.streaming;

import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.ENABLE_SCHEMATIZATION_CONFIG;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.SNOWPIPE_STREAMING_MAX_CLIENT_LAG;
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
import com.snowflake.kafka.connector.records.SnowflakeJsonConverter;
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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class SnowflakeSinkServiceV2SchematizationIT extends SnowflakeSinkServiceV2BaseIT {

  private final SnowflakeConnectionService conn = TestUtils.getConnectionServiceForStreaming();
  private Map<String, String> config;

  @BeforeEach
  public void setup() {
    config = TestUtils.getConfForStreaming();
    config.put(ENABLE_SCHEMATIZATION_CONFIG, "true");
  }

  @Test
  public void testSchematizationWithTableCreationAndJsonInput() throws Exception {
    config.put(
        SnowflakeSinkConnectorConfig.VALUE_CONVERTER_CONFIG_FIELD,
        "org.apache.kafka.connect.json.JsonConverter");
    config.put(SnowflakeSinkConnectorConfig.VALUE_SCHEMA_REGISTRY_CONFIG_FIELD, "http://fake-url");
    config.put("schemas.enable", "false");
    // get rid of these at the end
    SnowflakeSinkConnectorConfig.setDefaultValues(config);

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
    conn.createTableWithOnlyMetadataColumn(table);

    SchemaAndValue jsonInputValue = jsonConverter.toConnectData(topic, converted);

    long startOffset = 0;

    SinkRecord jsonRecordValue =
        new SinkRecord(
            topic,
            partition,
            Schema.STRING_SCHEMA,
            "test",
            jsonInputValue.schema(),
            jsonInputValue.value(),
            startOffset);

    SnowflakeSinkService service =
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
    TestUtils.assertWithRetry(() -> service.getOffset(topicPartition) == startOffset + 1, 20, 5);

    TestUtils.checkTableContentOneRow(
        table, SchematizationTestUtils.CONTENT_FOR_JSON_TABLE_CREATION);

    service.closeAll();
  }

  @Test
  public void testSchematizationSchemaEvolutionWithNonNullableColumn() throws Exception {
    config.put(
        SnowflakeSinkConnectorConfig.VALUE_CONVERTER_CONFIG_FIELD,
        "org.apache.kafka.connect.json.JsonConverter");
    config.put(SnowflakeSinkConnectorConfig.VALUE_SCHEMA_REGISTRY_CONFIG_FIELD, "http://fake-url");
    config.put("schemas.enable", "false");
    // get rid of these at the end

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
    createNonNullableColumn(table, "id_int8_non_nullable_missing_value");
    createNonNullableColumn(table, "id_int8_non_nullable_null_value");

    SchemaAndValue jsonInputValue = jsonConverter.toConnectData(topic, converted);

    long startOffset = 0;

    SinkRecord jsonRecordValue =
        new SinkRecord(
            topic,
            partition,
            Schema.STRING_SCHEMA,
            "test",
            jsonInputValue.schema(),
            jsonInputValue.value(),
            startOffset);

    SnowflakeSinkService service =
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
    TestUtils.assertWithRetry(() -> service.getOffset(topicPartition) == startOffset + 1, 20, 5);

    service.closeAll();
  }

  @Test
  void testSkippingOffsetsInSchemaEvolution() throws Exception {
    long maxClientLagSeconds = 1L;
    long schemaEvolutionDelayMs = 3 * 1000L; // must be enough for sdk to flush and commit
    long assertionSleepTimeMs = 6 * 1000L;

    config.put(
        SnowflakeSinkConnectorConfig.VALUE_CONVERTER_CONFIG_FIELD,
        "org.apache.kafka.connect.json.JsonConverter");
    config.put(SnowflakeSinkConnectorConfig.VALUE_SCHEMA_REGISTRY_CONFIG_FIELD, "http://fake-url");
    config.put("schemas.enable", "false");
    config.put(SNOWPIPE_STREAMING_MAX_CLIENT_LAG, String.valueOf(maxClientLagSeconds));

    // setup a table with a single field
    conn.createTableWithOnlyMetadataColumn(table);
    createNonNullableColumn(table, "id_int8");

    SnowflakeSinkService service =
        StreamingSinkServiceBuilder.builder(conn, config)
            .withSinkTaskContext(new InMemorySinkTaskContext(Collections.singleton(topicPartition)))
            .withSchemaEvolutionService(
                new DelayedSchemaEvolutionService(conn, schemaEvolutionDelayMs))
            .build();
    service.startPartition(table, topicPartition);

    service.insert(
        Arrays.asList(
            recordWithSingleField(0),
            recordWithSingleField(1),
            recordWithTwoFields(2),
            recordWithTwoFields(3)));

    // wait for processing all records and running ingest sdk thread
    Thread.sleep(assertionSleepTimeMs);

    // records 0 and 1 are ingested, 2 triggers schema evolution, 3 is skipped
    // getOffset() result is returned from preCommit() so Kafka will send next record starting from
    // this offset
    await().atMost(10, TimeUnit.SECONDS).until(() -> service.getOffset(topicPartition) == 2);

    // Kafka sends remaining messages
    service.insert(Arrays.asList(recordWithTwoFields(2), recordWithTwoFields(3)));

    await().atMost(10, TimeUnit.SECONDS).until(() -> service.getOffset(topicPartition) == 4);
  }

  @Test
  public void snowflakeSinkTask_put_whenJsonRecordCannotBeSchematized_sendRecordToDLQ() {
    // given
    config.put(ENABLE_SCHEMATIZATION_CONFIG, "true");

    InMemoryKafkaRecordErrorReporter errorReporter = new InMemoryKafkaRecordErrorReporter();

    SnowflakeSinkService service =
        StreamingSinkServiceBuilder.builder(conn, config)
            .withSinkTaskContext(new InMemorySinkTaskContext(Collections.singleton(topicPartition)))
            .withErrorReporter(errorReporter)
            .build();
    service.startPartition(table, topicPartition);

    SnowflakeJsonConverter jsonConverter = new SnowflakeJsonConverter();
    String notSchematizeableJsonRecord =
        "[{\"name\":\"sf\",\"answer\":42}]"; // cannot schematize array
    byte[] valueContents = (notSchematizeableJsonRecord).getBytes(StandardCharsets.UTF_8);
    SchemaAndValue sv = jsonConverter.toConnectData(topic, valueContents);

    SinkRecord record =
        SinkRecordBuilder.forTopicPartition(topic, partition).withSchemaAndValue(sv).build();

    // when
    service.insert(record);

    // then
    Assertions.assertEquals(1, errorReporter.getReportedRecords().size());
  }

  private SinkRecord recordWithSingleField(long offset) {
    Schema schema = SchemaBuilder.struct().field("id_int8", Schema.INT8_SCHEMA).build();
    Struct struct = new Struct(schema).put("id_int8", (byte) 0);
    return getSinkRecord(offset, struct);
  }

  private SinkRecord recordWithTwoFields(long offset) {
    Schema schema =
        SchemaBuilder.struct()
            .field("id_int8", Schema.INT8_SCHEMA)
            .field("id_int8_2", Schema.INT8_SCHEMA)
            .build();
    Struct struct = new Struct(schema).put("id_int8", (byte) 0).put("id_int8_2", (byte) 0);
    return getSinkRecord(offset, struct);
  }

  private SinkRecord getSinkRecord(long offset, Struct struct) {
    JsonConverter jsonConverter = new JsonConverter();
    Map<String, String> config = new HashMap<>();
    config.put("schemas.enable", "false");
    jsonConverter.configure(config, false);
    byte[] converted = jsonConverter.fromConnectData(topic, struct.schema(), struct);
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

  private void createNonNullableColumn(String tableName, String colName) {
    String createTableQuery = "alter table identifier(?) add " + colName + " int not null";

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
}
