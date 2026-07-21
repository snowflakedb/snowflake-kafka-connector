package com.snowflake.kafka.connector.internal.streaming;

import com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams;
import com.snowflake.kafka.connector.builder.SinkRecordBuilder;
import com.snowflake.kafka.connector.config.SinkTaskConfig;
import com.snowflake.kafka.connector.dlq.InMemoryKafkaRecordErrorReporter;
import com.snowflake.kafka.connector.internal.SnowflakeConnectionService;
import com.snowflake.kafka.connector.internal.SnowflakeSinkService;
import com.snowflake.kafka.connector.internal.TestUtils;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Regression test for SNOW-3766306: TIME strings with a UTC offset (e.g. {@code "00:00:00Z"}) must
 * be normalized (offset stripped) before reaching the SDK so they land in the table rather than
 * being silently dropped by the SSv2 server.
 */
public class SnowflakeSinkServiceV2XpParityIT extends SnowflakeSinkServiceV2BaseIT {

  private final SnowflakeConnectionService conn = TestUtils.getConnectionService();

  // Accumulate all tables created across tests so teardown can clean them all up.
  private final List<String> tablesToDrop = new ArrayList<>();
  private final List<SnowflakeSinkService> servicesToClose = new ArrayList<>();

  @AfterEach
  public void teardown() {
    for (SnowflakeSinkService svc : servicesToClose) {
      try {
        svc.closeAll();
      } catch (Exception ignored) {
        // best-effort
      }
    }
    for (String t : tablesToDrop) {
      TestUtils.dropTable(t);
    }
    // Also drop the base-class table (may or may not have been used).
    TestUtils.dropTable(table);
  }

  // ---------------------------------------------------------------------------
  // Tests
  // ---------------------------------------------------------------------------

  /**
   * TIME isostring with UTC offset must be normalized and land in the table (not DLQ'd). Inserts
   * two values; asserts DLQ empty and both rows present with correct values.
   */
  @Test
  public void timeIsostringOffset_landsNotDropped() throws Exception {
    String t = freshTable("COL_TIMETZ TIME(9)");
    ServiceAndReporter sar = startService(t);

    sar.service.insert(record(t, "{\"COL_TIMETZ\":\"00:00:00Z\"}", 0));
    sar.service.insert(record(t, "{\"COL_TIMETZ\":\"07:59:59.999999Z\"}", 1));

    // DLQ must be empty — normalization means both values are accepted.
    Assertions.assertEquals(
        0,
        sar.reporter.getReportedRecords().size(),
        "DLQ must be empty: both Z-time values should normalize and land");

    // Wait for both rows to flush.
    TestUtils.assertWithRetry(() -> TestUtils.tableSize(t) == 2, 5, 20);

    // Query back the values and verify they match the normalized form.
    // getTableRows returns each row as Map<columnName, Object>; TIME columns arrive as
    // java.sql.Time whose toString() gives HH:mm:ss (without nanos in the default repr),
    // so we also accept the full nano form by using startsWith.
    List<Map<String, Object>> rows = TestUtils.getTableRows(t);
    Assertions.assertEquals(2, rows.size(), "Expected 2 rows after flush");

    // Collect the string representations and sort so the assertion is order-independent.
    List<String> timeValues = new ArrayList<>();
    for (Map<String, Object> row : rows) {
      Object v = row.get("COL_TIMETZ");
      Assertions.assertNotNull(v, "COL_TIMETZ must not be null");
      timeValues.add(v.toString());
    }
    Collections.sort(timeValues);

    Assertions.assertTrue(
        timeValues.get(0).startsWith("00:00:00"),
        "First TIME value should be 00:00:00, got: " + timeValues.get(0));
    Assertions.assertTrue(
        timeValues.get(1).startsWith("07:59:59"),
        "Second TIME value should start with 07:59:59, got: " + timeValues.get(1));
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  /** Container for a started service and its error reporter. */
  private static class ServiceAndReporter {
    final SnowflakeSinkService service;
    final InMemoryKafkaRecordErrorReporter reporter;

    ServiceAndReporter(SnowflakeSinkService service, InMemoryKafkaRecordErrorReporter reporter) {
      this.service = service;
      this.reporter = reporter;
    }
  }

  /**
   * Creates and returns a new table name with the given extra column DDL appended. The table is
   * registered for teardown automatically.
   *
   * @param extraColumnDdl e.g. {@code "COL_TIMETZ TIME(9)"}
   */
  private String freshTable(String extraColumnDdl) throws Exception {
    String t = TestUtils.randomTableName();
    tablesToDrop.add(t);
    conn.createTableWithOnlyMetadataColumn(t);
    conn.executeQueryWithParameters("ALTER TABLE \"" + t + "\" ADD COLUMN " + extraColumnDdl);
    return t;
  }

  /**
   * Builds and starts a streaming sink service with client-side validation enabled against the
   * given table. The topic name equals the table name (matching base-class convention for
   * single-table tests).
   */
  private ServiceAndReporter startService(String tableName) throws Exception {
    Map<String, String> config = TestUtils.getConnectorConfigurationForStreaming(false);
    config.put(KafkaConnectorConfigParams.SNOWFLAKE_VALIDATION, "client_side");

    SinkTaskConfig sinkTaskConfig =
        SinkTaskConfig.builderFrom(config).tolerateErrors(true).dlqTopicName("dlq").build();

    InMemoryKafkaRecordErrorReporter reporter = new InMemoryKafkaRecordErrorReporter();

    TopicPartition tp = new TopicPartition(tableName, 0);
    SnowflakeSinkService svc =
        StreamingSinkServiceBuilder.builder(conn, sinkTaskConfig)
            .withSinkTaskContext(new InMemorySinkTaskContext(Collections.singleton(tp)))
            .withErrorReporter(reporter)
            .build();
    svc.startPartition(tp);
    svc.awaitInitialization();

    servicesToClose.add(svc);
    return new ServiceAndReporter(svc, reporter);
  }

  /**
   * Builds a schema-less Kafka {@link SinkRecord} for the given topic (= table name), payload, and
   * offset.
   */
  private SinkRecord record(String topicName, String jsonPayload, long offset) {
    JsonConverter converter = new JsonConverter();
    Map<String, String> cfg = new HashMap<>();
    cfg.put("schemas.enable", "false");
    converter.configure(cfg, false);

    byte[] valueBytes = jsonPayload.getBytes(StandardCharsets.UTF_8);
    SchemaAndValue schemaAndValue = converter.toConnectData(topicName, valueBytes);

    return SinkRecordBuilder.forTopicPartition(topicName, 0)
        .withSchemaAndValue(schemaAndValue)
        .withOffset(offset)
        .withKey("test")
        .build();
  }
}
