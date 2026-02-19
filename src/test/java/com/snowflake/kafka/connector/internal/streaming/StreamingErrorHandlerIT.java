package com.snowflake.kafka.connector.internal.streaming;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.snowflake.kafka.connector.builder.SinkRecordBuilder;
import com.snowflake.kafka.connector.dlq.InMemoryKafkaRecordErrorReporter;
import com.snowflake.kafka.connector.internal.SnowflakeConnectionService;
import com.snowflake.kafka.connector.internal.SnowflakeKafkaConnectorException;
import com.snowflake.kafka.connector.internal.TestUtils;
import com.snowflake.kafka.connector.internal.streaming.v2.SnowpipeStreamingPartitionChannel;
import com.snowflake.kafka.connector.internal.streaming.v2.StreamingClientManager;
import com.snowflake.kafka.connector.internal.telemetry.SnowflakeTelemetryService;
import com.snowflake.kafka.connector.records.SnowflakeMetadataConfig;
import com.snowflake.kafka.connector.records.SnowflakeSinkRecord;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Integration tests verifying client-side broken record errors are routed through {@link
 * StreamingErrorHandler} with proper {@code errors.tolerance} semantics.
 *
 * <p>These tests exercise the full path: {@link SnowpipeStreamingPartitionChannel#insertRecord} →
 * broken record detection → {@link StreamingErrorHandler#handleError} → DLQ / throw.
 */
class StreamingErrorHandlerIT {

  private static final String TOPIC = "test_topic";
  private static final int PARTITION = 0;

  private String connectorName;
  private String channelName;
  private String pipeName;

  private SnowflakeConnectionService mockConnectionService;
  private SnowflakeTelemetryService mockTelemetryService;
  private InMemorySinkTaskContext sinkTaskContext;

  @BeforeEach
  void setUp() {
    final String uniqueId = UUID.randomUUID().toString().substring(0, 8);
    connectorName = "test_connector_" + uniqueId;
    channelName = "test_channel_" + uniqueId;
    pipeName = "test_pipe_" + uniqueId;

    mockConnectionService = mock(SnowflakeConnectionService.class);
    mockTelemetryService = mock(SnowflakeTelemetryService.class);
    when(mockConnectionService.getTelemetryClient()).thenReturn(mockTelemetryService);

    sinkTaskContext =
        new InMemorySinkTaskContext(Collections.singleton(new TopicPartition(TOPIC, PARTITION)));

    StreamingClientManager.setIngestClientSupplier(new FakeIngestClientSupplier());
  }

  @AfterEach
  void tearDown() {
    StreamingClientManager.resetIngestClientSupplier();
  }

  // ── errors.tolerance = NONE (default) ──────────────────────────────────────

  @Test
  void brokenRecord_toleranceNone_shouldThrowDataException() {
    InMemoryKafkaRecordErrorReporter errorReporter = new InMemoryKafkaRecordErrorReporter();
    Map<String, String> config = baseConfig();

    SnowpipeStreamingPartitionChannel channel = createChannel(config, errorReporter);
    SinkRecord brokenSinkRecord = buildBrokenValueRecord(0);

    DataException thrown =
        assertThrows(DataException.class, () -> channel.insertRecord(brokenSinkRecord, true));

    // The cause should be the original SnowflakeKafkaConnectorException from convertToMap
    assertNotNull(thrown.getCause(), "DataException should wrap the original conversion exception");
    assertTrue(
        thrown.getCause() instanceof SnowflakeKafkaConnectorException,
        "Cause should be the original SnowflakeKafkaConnectorException, got: "
            + thrown.getCause().getClass().getName());
    assertEquals(0, errorReporter.getReportedRecords().size());
  }

  @Test
  void brokenKeyRecord_toleranceNone_shouldThrowDataException() {
    InMemoryKafkaRecordErrorReporter errorReporter = new InMemoryKafkaRecordErrorReporter();
    Map<String, String> config = baseConfig();

    SnowpipeStreamingPartitionChannel channel = createChannel(config, errorReporter);
    SinkRecord brokenSinkRecord = buildBrokenKeyRecord(0);

    DataException thrown =
        assertThrows(DataException.class, () -> channel.insertRecord(brokenSinkRecord, true));

    assertNotNull(thrown.getCause(), "DataException should wrap the original conversion exception");
    assertEquals(0, errorReporter.getReportedRecords().size());
  }

  // ── errors.tolerance = ALL + DLQ configured ────────────────────────────────

  @Test
  void brokenRecord_toleranceAll_withDLQ_shouldSendOriginalExceptionToDLQ() {
    InMemoryKafkaRecordErrorReporter errorReporter = new InMemoryKafkaRecordErrorReporter();
    Map<String, String> config = baseConfig();
    config.put("errors.tolerance", "all");
    config.put("errors.deadletterqueue.topic.name", "my-dlq-topic");

    SnowpipeStreamingPartitionChannel channel = createChannel(config, errorReporter);
    SinkRecord brokenSinkRecord = buildBrokenValueRecord(0);

    // Should NOT throw
    channel.insertRecord(brokenSinkRecord, true);

    assertEquals(1, errorReporter.getReportedRecords().size());

    InMemoryKafkaRecordErrorReporter.ReportedRecord reported =
        errorReporter.getReportedRecords().get(0);
    assertEquals(brokenSinkRecord, reported.getRecord());

    // The DLQ should receive the original conversion exception, not a generic DataException
    assertTrue(
        reported.getException() instanceof SnowflakeKafkaConnectorException,
        "DLQ should receive the original SnowflakeKafkaConnectorException from convertToMap, got: "
            + reported.getException().getClass().getName());
  }

  @Test
  void brokenKeyRecord_toleranceAll_withDLQ_shouldSendOriginalExceptionToDLQ() {
    InMemoryKafkaRecordErrorReporter errorReporter = new InMemoryKafkaRecordErrorReporter();
    Map<String, String> config = baseConfig();
    config.put("errors.tolerance", "all");
    config.put("errors.deadletterqueue.topic.name", "my-dlq-topic");

    SnowpipeStreamingPartitionChannel channel = createChannel(config, errorReporter);
    SinkRecord brokenSinkRecord = buildBrokenKeyRecord(0);

    channel.insertRecord(brokenSinkRecord, true);

    assertEquals(1, errorReporter.getReportedRecords().size());
    InMemoryKafkaRecordErrorReporter.ReportedRecord reported =
        errorReporter.getReportedRecords().get(0);
    assertEquals(brokenSinkRecord, reported.getRecord());

    // The DLQ exception must be the exact same instance captured in SnowflakeSinkRecord
    SnowflakeSinkRecord sinkRecord =
        SnowflakeSinkRecord.from(brokenSinkRecord, new SnowflakeMetadataConfig());
    assertSame(
        sinkRecord.getBrokenReason().getClass(),
        reported.getException().getClass(),
        "DLQ exception type should match the original brokenReason type");
  }

  @Test
  void multipleBrokenRecords_toleranceAll_withDLQ_shouldSendAllToDLQ() {
    InMemoryKafkaRecordErrorReporter errorReporter = new InMemoryKafkaRecordErrorReporter();
    Map<String, String> config = baseConfig();
    config.put("errors.tolerance", "all");
    config.put("errors.deadletterqueue.topic.name", "my-dlq-topic");

    SnowpipeStreamingPartitionChannel channel = createChannel(config, errorReporter);

    channel.insertRecord(buildBrokenValueRecord(0), true);
    channel.insertRecord(buildBrokenValueRecord(1), false);
    channel.insertRecord(buildBrokenValueRecord(2), false);

    assertEquals(3, errorReporter.getReportedRecords().size());
  }

  // ── errors.tolerance = ALL + no DLQ → should fail ──────────────────────────

  @Test
  void brokenRecord_toleranceAll_noDLQ_shouldThrowDataException() {
    InMemoryKafkaRecordErrorReporter errorReporter = new InMemoryKafkaRecordErrorReporter();
    Map<String, String> config = baseConfig();
    config.put("errors.tolerance", "all");
    // No DLQ topic configured

    SnowpipeStreamingPartitionChannel channel = createChannel(config, errorReporter);
    SinkRecord brokenSinkRecord = buildBrokenValueRecord(0);

    DataException thrown =
        assertThrows(DataException.class, () -> channel.insertRecord(brokenSinkRecord, true));

    assertTrue(
        thrown.getMessage().contains("not configured"),
        "Error message should indicate DLQ is not configured, got: " + thrown.getMessage());
    assertNotNull(thrown.getCause(), "DataException should wrap the original conversion exception");
    assertEquals(0, errorReporter.getReportedRecords().size());
  }

  // ── Helpers ────────────────────────────────────────────────────────────────

  private Map<String, String> baseConfig() {
    return new HashMap<>(TestUtils.getConnectorConfigurationForStreaming(false));
  }

  /**
   * Creates a SinkRecord whose value triggers a broken record (plain String with STRING_SCHEMA).
   */
  private SinkRecord buildBrokenValueRecord(long offset) {
    return SinkRecordBuilder.forTopicPartition(TOPIC, PARTITION)
        .withValueSchema(Schema.STRING_SCHEMA)
        .withValue("plain string - not a map")
        .withOffset(offset)
        .build();
  }

  /** Creates a SinkRecord whose key triggers a broken record (String with INT32 key schema). */
  private SinkRecord buildBrokenKeyRecord(long offset) {
    return SinkRecordBuilder.forTopicPartition(TOPIC, PARTITION)
        .withKeySchema(Schema.INT32_SCHEMA)
        .withKey("not an int")
        .withValueSchema(Schema.STRING_SCHEMA)
        .withValue("{}")
        .withOffset(offset)
        .build();
  }

  private SnowpipeStreamingPartitionChannel createChannel(
      Map<String, String> config, InMemoryKafkaRecordErrorReporter errorReporter) {
    StreamingErrorHandler errorHandler =
        new StreamingErrorHandler(config, errorReporter, mockTelemetryService);

    return new SnowpipeStreamingPartitionChannel(
        "test_table",
        channelName,
        pipeName,
        new TopicPartition(TOPIC, PARTITION),
        mockConnectionService,
        config,
        errorReporter,
        new SnowflakeMetadataConfig(),
        sinkTaskContext,
        false,
        null,
        connectorName,
        "0",
        errorHandler);
  }
}
