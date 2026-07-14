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
import java.util.Map;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Regression test for SNOW-3766306: the SSv2 XP TIME parser silently dropped time strings that
 * carried a UTC offset without fractional seconds (e.g. {@code "00:00:00Z"}). After the fix,
 * client-side validation rejects such strings and routes them to the DLQ, while a valid
 * fractional-seconds time string (e.g. {@code "07:59:59.999999Z"}) is accepted. A second test
 * verifies that space-separated TIMESTAMP_NTZ values are accepted.
 */
public class SnowflakeSinkServiceV2XpParityIT extends SnowflakeSinkServiceV2BaseIT {

  private final SnowflakeConnectionService conn = TestUtils.getConnectionService();
  private SnowflakeSinkService service;

  @AfterEach
  public void teardown() {
    if (service != null) {
      service.closeAll();
    }
    TestUtils.dropTable(table);
  }

  /**
   * Inserts a TIME value without fractional seconds and a UTC offset (the broken format) and a
   * valid fractional-seconds TIME value. Asserts that:
   *
   * <ul>
   *   <li>The invalid record is routed to the DLQ (client-side validation catches it).
   *   <li>The valid record reaches the Snowflake table (exactly 1 row).
   * </ul>
   */
  @Test
  public void timeOffsetWithoutFraction_isRejectedNotSilentlyDropped() throws Exception {
    // Pre-create table with a TIME(9) column so the column type is fixed and schematization
    // does not interfere with the type during the test.
    conn.createTableWithOnlyMetadataColumn(table);
    conn.executeQueryWithParameters("ALTER TABLE \"" + table + "\" ADD COLUMN COL_TIMETZ TIME(9)");

    Map<String, String> config = TestUtils.getConnectorConfigurationForStreaming(false);
    config.put(KafkaConnectorConfigParams.SNOWFLAKE_VALIDATION, "client_side");

    SinkTaskConfig sinkTaskConfig =
        SinkTaskConfig.builderFrom(config).tolerateErrors(true).dlqTopicName("dlq").build();

    InMemoryKafkaRecordErrorReporter errorReporter = new InMemoryKafkaRecordErrorReporter();

    service =
        StreamingSinkServiceBuilder.builder(conn, sinkTaskConfig)
            .withSinkTaskContext(new InMemorySinkTaskContext(Collections.singleton(topicPartition)))
            .withErrorReporter(errorReporter)
            .build();
    service.startPartition(topicPartition);
    service.awaitInitialization();

    // Offset 0: time without fractional seconds — XP rejects this, client-side validation must
    // catch it and route to DLQ rather than silently dropping it.
    SinkRecord invalidRecord =
        createKafkaRecordWithoutSchema("{\"COL_TIMETZ\":\"00:00:00Z\"}", 0);
    service.insert(invalidRecord);

    // Offset 1: time with fractional seconds — valid per XP, must be ingested.
    SinkRecord validRecord =
        createKafkaRecordWithoutSchema("{\"COL_TIMETZ\":\"07:59:59.999999Z\"}", 1);
    service.insert(validRecord);

    // DLQ check is synchronous: validation fires during insert().
    Assertions.assertEquals(
        1,
        errorReporter.getReportedRecords().size(),
        "Expected exactly 1 record routed to DLQ (the offset-without-fraction TIME value)");

    // Table size check is async (SSv2 channel flush); retry until the row arrives.
    TestUtils.assertWithRetry(() -> TestUtils.tableSize(table) == 1, 5, 20);
  }

  /**
   * Inserts a TIMESTAMP_NTZ value using a space separator between date and time (ISO 8601 allows
   * both 'T' and space). Asserts that the record is accepted (no DLQ, exactly 1 row in table).
   */
  @Test
  public void timestampSpaceSeparator_isAccepted() throws Exception {
    conn.createTableWithOnlyMetadataColumn(table);
    conn.executeQueryWithParameters(
        "ALTER TABLE \"" + table + "\" ADD COLUMN COL_TS TIMESTAMP_NTZ(9)");

    Map<String, String> config = TestUtils.getConnectorConfigurationForStreaming(false);
    config.put(KafkaConnectorConfigParams.SNOWFLAKE_VALIDATION, "client_side");

    SinkTaskConfig sinkTaskConfig =
        SinkTaskConfig.builderFrom(config).tolerateErrors(true).dlqTopicName("dlq").build();

    InMemoryKafkaRecordErrorReporter errorReporter = new InMemoryKafkaRecordErrorReporter();

    service =
        StreamingSinkServiceBuilder.builder(conn, sinkTaskConfig)
            .withSinkTaskContext(new InMemorySinkTaskContext(Collections.singleton(topicPartition)))
            .withErrorReporter(errorReporter)
            .build();
    service.startPartition(topicPartition);
    service.awaitInitialization();

    SinkRecord record =
        createKafkaRecordWithoutSchema("{\"COL_TS\":\"2024-01-15 10:30:00\"}", 0);
    service.insert(record);

    Assertions.assertEquals(
        0,
        errorReporter.getReportedRecords().size(),
        "Expected space-separated TIMESTAMP_NTZ to be accepted (no DLQ entries)");

    TestUtils.assertWithRetry(() -> TestUtils.tableSize(table) == 1, 5, 20);
  }

  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------

  private SinkRecord createKafkaRecordWithoutSchema(String jsonPayload, long offset) {
    JsonConverter jsonConverter = new JsonConverter();
    Map<String, String> converterConfig = new HashMap<>();
    converterConfig.put("schemas.enable", "false");
    jsonConverter.configure(converterConfig, false);

    byte[] valueBytes = jsonPayload.getBytes(StandardCharsets.UTF_8);
    SchemaAndValue schemaAndValue = jsonConverter.toConnectData(topic, valueBytes);

    return SinkRecordBuilder.forTopicPartition(topic, partition)
        .withSchemaAndValue(schemaAndValue)
        .withOffset(offset)
        .withKey("test")
        .build();
  }
}
