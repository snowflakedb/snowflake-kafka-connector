package com.snowflake.kafka.connector.internal.streaming;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.snowflake.kafka.connector.config.SinkTaskConfig;
import com.snowflake.kafka.connector.config.SnowflakeValidation;
import com.snowflake.kafka.connector.dlq.InMemoryKafkaRecordErrorReporter;
import com.snowflake.kafka.connector.internal.SnowflakeConnectionService;
import com.snowflake.kafka.connector.internal.SnowflakeSinkService;
import com.snowflake.kafka.connector.internal.TestUtils;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

/**
 * End-to-end coverage for the 128MB LOB limit: a VARIANT just under the ceiling is ingested, and
 * one byte past it is rejected locally and routed to the DLQ.
 *
 * <p>The record is a Java Map so no converter sits between the test and the connector. Payload size
 * is the serialized VARIANT ({@code {"a":"..."}}) relative to the 128MB ceiling minus the 64-byte
 * server-skew buffer.
 *
 * <p>Skipped in default CI: a ~128MB row is TRACE-logged by the connector and has twice cancelled
 * the 6-hour AWS integration job. Set {@code SNOWFLAKE_RUN_LARGE_LOB_IT=true} to run it.
 */
@Timeout(value = 15, unit = TimeUnit.MINUTES)
@EnabledIfEnvironmentVariable(named = "SNOWFLAKE_RUN_LARGE_LOB_IT", matches = "true")
public class LargeLobIngestionIT extends SnowflakeSinkServiceV2BaseIT {

  private static final int BYTES_1_MB = 1024 * 1024;
  private static final int LOB_CEILING_BYTES = 128 * BYTES_1_MB;
  // Matches DataValidationUtil.MAX_SEMI_STRUCTURED_LENGTH (package-private from this IT).
  private static final int SERVER_SKEW_BYTES = 64;
  // Serialized form is {"a":"<n chars>"}, which adds 8 bytes around the string.
  private static final int JSON_WRAPPER_BYTES = 8;
  private static final int BYTES_AT_CEILING =
      LOB_CEILING_BYTES - SERVER_SKEW_BYTES - JSON_WRAPPER_BYTES;
  private static final int BYTES_OVER_CEILING = BYTES_AT_CEILING + 1;

  /** VARIANT so that the semi-structured branch of the size check is the one being exercised. */
  private static final String PAYLOAD_COLUMN = "PAYLOAD";

  private final SnowflakeConnectionService conn = TestUtils.getConnectionServiceWithEncryptedKey();
  private SinkTaskConfig.Builder configBuilder;

  @BeforeEach
  public void setup() {
    configBuilder =
        SinkTaskConfig.builderFrom(TestUtils.getConnectorConfigurationForStreaming(true))
            .validation(SnowflakeValidation.CLIENT_SIDE)
            .enableSchematization(true);

    // Created up front so that the large row is not spent on triggering schema evolution.
    conn.createTableWithOnlyMetadataColumn(table);
    conn.executeQueryWithParameters(
        "alter table identifier(?) add column " + PAYLOAD_COLUMN + " variant", table);
  }

  @AfterEach
  public void afterEach() {
    TestUtils.dropTable(table);
    TestUtils.dropPipe(table);
  }

  @Test
  public void variantAtLobCeiling_isIngested() throws Exception {
    SnowflakeSinkService service = startService(new InMemoryKafkaRecordErrorReporter());

    service.insert(payloadRecord(BYTES_AT_CEILING, 0));

    TestUtils.assertWithRetry(() -> service.getOffset(topicPartition) == 1, 5, 60);
    TestUtils.assertWithRetry(() -> TestUtils.tableSize(table) == 1, 5, 60);

    int ingestedBytes = payloadByteLength();
    assertTrue(
        ingestedBytes >= BYTES_AT_CEILING,
        "expected at least "
            + BYTES_AT_CEILING
            + " bytes in "
            + PAYLOAD_COLUMN
            + ", got "
            + ingestedBytes);

    service.closeAll();
  }

  @Test
  public void variantOneByteOverLobCeiling_isRoutedToDlq() throws Exception {
    configBuilder.tolerateErrors(true).dlqTopicName("DLQ_TOPIC").errorsLogEnable(true);
    InMemoryKafkaRecordErrorReporter errorReporter = new InMemoryKafkaRecordErrorReporter();
    SnowflakeSinkService service = startService(errorReporter);

    service.insert(payloadRecord(BYTES_OVER_CEILING, 0));

    TestUtils.assertWithRetry(() -> errorReporter.getReportedRecords().size() == 1, 5, 20);
    assertEquals(0, TestUtils.tableSize(table), "oversized record must not be ingested");

    String reportedError = errorReporter.getReportedRecords().get(0).getException().getMessage();
    assertTrue(
        reportedError.contains("Variant too long"),
        "expected a client-side size failure, got: " + reportedError);

    service.closeAll();
  }

  private SnowflakeSinkService startService(InMemoryKafkaRecordErrorReporter errorReporter) {
    SnowflakeSinkService service =
        StreamingSinkServiceBuilder.builder(conn, configBuilder.build())
            .withSinkTaskContext(new InMemorySinkTaskContext(Collections.singleton(topicPartition)))
            .withErrorReporter(errorReporter)
            .build();
    service.startPartition(topicPartition);
    service.awaitInitialization();
    return service;
  }

  /** Builds a record whose {@code PAYLOAD} is {@code {"a":"<contentBytes ASCII>"}}. */
  private SinkRecord payloadRecord(int contentBytes, long offset) {
    char[] content = new char[contentBytes];
    Arrays.fill(content, 'a');
    Map<String, Object> payload = new HashMap<>();
    payload.put("a", new String(content));
    Map<String, Object> value = new HashMap<>();
    value.put(PAYLOAD_COLUMN, payload);

    return new SinkRecord(topic, partition, Schema.STRING_SCHEMA, "key", null, value, offset);
  }

  private int payloadByteLength() {
    return TestUtils.executeQueryAndCollectResult(
        "select octet_length(to_json(" + PAYLOAD_COLUMN + ")) as LEN from identifier(?)",
        table,
        resultSet -> {
          try {
            resultSet.next();
            return resultSet.getInt("LEN");
          } catch (SQLException e) {
            throw new IllegalStateException(e);
          }
        });
  }
}
