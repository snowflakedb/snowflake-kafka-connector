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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * End-to-end coverage for the 128MB LOB limit enforced by client-side validation: a payload far
 * beyond the old 16MB ceiling reaches Snowflake intact, while one past 128MB is rejected locally
 * and routed to the DLQ.
 *
 * <p>The payload is assembled as an array of 1MB strings rather than one huge string because
 * Kafka's JsonConverter refuses to deserialize a single string value larger than 20MB. The record
 * is built from a plain Java Map so that no converter sits between the test and the connector.
 */
public class LargeLobIngestionIT extends SnowflakeSinkServiceV2BaseIT {

  private static final int ONE_MB = 1024 * 1024;

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
  public void variantJustUnder128Mb_isIngested() throws Exception {
    int chunks = 127;
    SnowflakeSinkService service = startService(new InMemoryKafkaRecordErrorReporter());

    service.insert(payloadRecord(chunks, 0));

    TestUtils.assertWithRetry(() -> service.getOffset(topicPartition) == 1, 5, 60);
    TestUtils.assertWithRetry(() -> TestUtils.tableSize(table) == 1, 5, 60);

    // Whole payload landed, not a truncated prefix.
    int ingestedBytes = payloadByteLength();
    assertTrue(
        ingestedBytes > chunks * ONE_MB,
        "expected more than "
            + chunks * ONE_MB
            + " bytes in "
            + PAYLOAD_COLUMN
            + ", got "
            + ingestedBytes);

    service.closeAll();
  }

  @Test
  public void variantAbove128Mb_isRoutedToDlq() throws Exception {
    configBuilder.tolerateErrors(true).dlqTopicName("DLQ_TOPIC").errorsLogEnable(true);
    InMemoryKafkaRecordErrorReporter errorReporter = new InMemoryKafkaRecordErrorReporter();
    SnowflakeSinkService service = startService(errorReporter);

    service.insert(payloadRecord(129, 0));

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

  /** Builds a record whose {@code PAYLOAD} column holds {@code chunks} MB of JSON. */
  private SinkRecord payloadRecord(int chunks, long offset) {
    List<String> parts = new ArrayList<>(chunks);
    for (int chunk = 0; chunk < chunks; chunk++) {
      char[] content = new char[ONE_MB];
      Arrays.fill(content, (char) ('a' + chunk % 26));
      parts.add(new String(content));
    }

    Map<String, Object> payload = new HashMap<>();
    payload.put("chunks", parts);
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
