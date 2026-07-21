package com.snowflake.kafka.connector.internal.streaming;

import com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams;
import com.snowflake.kafka.connector.builder.SinkRecordBuilder;
import com.snowflake.kafka.connector.config.SinkTaskConfig;
import com.snowflake.kafka.connector.dlq.InMemoryKafkaRecordErrorReporter;
import com.snowflake.kafka.connector.internal.SnowflakeConnectionService;
import com.snowflake.kafka.connector.internal.SnowflakeSinkService;
import com.snowflake.kafka.connector.internal.TestUtils;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class SnowflakeSinkServiceV2SchematizationIT extends SnowflakeSinkServiceV2BaseIT {

  private final SnowflakeConnectionService conn = TestUtils.getConnectionService();
  private SinkTaskConfig sinkTaskConfig;
  private SnowflakeSinkService service;
  private String pipe;

  @BeforeEach
  public void setup() {
    Map<String, String> config = TestUtils.getConnectorConfigurationForStreaming(false);
    sinkTaskConfig =
        SinkTaskConfig.builderFrom(config).tolerateErrors(true).dlqTopicName("dlq_topic").build();
    pipe = table;
  }

  @AfterEach
  public void teardown() {
    service.closeAll();
    TestUtils.dropTable(table);
    TestUtils.dropPipe(pipe);
  }

  @Test
  public void snowflakeSinkTask_put_whenJsonRecordCannotBeSchematized_sendRecordToDLQ() {
    // given
    conn.createTableWithOnlyMetadataColumn(table);

    InMemoryKafkaRecordErrorReporter errorReporter = new InMemoryKafkaRecordErrorReporter();

    service =
        StreamingSinkServiceBuilder.builder(conn, sinkTaskConfig)
            .withSinkTaskContext(new InMemorySinkTaskContext(Collections.singleton(topicPartition)))
            .withErrorReporter(errorReporter)
            .build();
    service.startPartition(topicPartition);
    service.awaitInitialization();

    // Create a record that cannot be schematized (array at root level)
    String notSchematizeableJsonRecord = "[{\"name\":\"sf\",\"answer\":42}]";
    SinkRecord record = createKafkaRecordWithoutSchema(notSchematizeableJsonRecord, 0);

    // when
    service.insert(record);

    // then
    Assertions.assertEquals(1, errorReporter.getReportedRecords().size());
  }

  /**
   * SNOW-3766306: A TIME string with a UTC offset (e.g. "00:00:00Z") must land in the table — not
   * be silently dropped — when client-side validation is enabled. The normalizer strips the offset
   * to LocalTime before passing to the SSv2 SDK.
   */
  @Test
  public void timeColumnWithUtcOffset_landsNotDropped() throws Exception {
    conn.createTableWithOnlyMetadataColumn(table);
    conn.executeQueryWithParameters("ALTER TABLE \"" + table + "\" ADD COLUMN COL_TIMETZ TIME(9)");

    Map<String, String> config = TestUtils.getConnectorConfigurationForStreaming(false);
    config.put(KafkaConnectorConfigParams.SNOWFLAKE_VALIDATION, "client_side");
    SinkTaskConfig taskConfig =
        SinkTaskConfig.builderFrom(config).tolerateErrors(true).dlqTopicName("dlq").build();

    InMemoryKafkaRecordErrorReporter reporter = new InMemoryKafkaRecordErrorReporter();
    service =
        StreamingSinkServiceBuilder.builder(conn, taskConfig)
            .withSinkTaskContext(new InMemorySinkTaskContext(Collections.singleton(topicPartition)))
            .withErrorReporter(reporter)
            .build();
    service.startPartition(topicPartition);
    service.awaitInitialization();

    SinkRecord record = createKafkaRecordWithoutSchema("{\"COL_TIMETZ\":\"00:00:00Z\"}", 0);
    service.insert(record);

    Assertions.assertEquals(
        0,
        reporter.getReportedRecords().size(),
        "DLQ must be empty: 00:00:00Z should normalize and land (SNOW-3766306)");

    TestUtils.assertWithRetry(() -> TestUtils.tableSize(table) == 1, 5, 20);

    List<Map<String, Object>> rows = TestUtils.getTableRows(table);
    Assertions.assertEquals(1, rows.size());
    Object v = rows.get(0).get("COL_TIMETZ");
    Assertions.assertNotNull(v, "COL_TIMETZ should not be null");
    Assertions.assertTrue(
        v.toString().startsWith("00:00:00"), "TIME value should be 00:00:00, got: " + v);
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
}
