package com.snowflake.kafka.connector.internal.streaming;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams;
import com.snowflake.kafka.connector.builder.SinkRecordBuilder;
import com.snowflake.kafka.connector.dlq.InMemoryKafkaRecordErrorReporter;
import com.snowflake.kafka.connector.internal.SnowflakeKafkaConnectorException;
import com.snowflake.kafka.connector.internal.TestUtils;
import com.snowflake.kafka.connector.internal.metrics.TaskMetrics;
import com.snowflake.kafka.connector.internal.streaming.telemetry.SnowflakeTelemetryChannelStatus;
import com.snowflake.kafka.connector.internal.streaming.v2.SnowpipeStreamingPartitionChannel;
import com.snowflake.kafka.connector.internal.streaming.v2.channel.PartitionOffsetTracker;
import com.snowflake.kafka.connector.internal.telemetry.SnowflakeTelemetryService;
import com.snowflake.kafka.connector.records.SnowflakeMetadataConfig;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
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

  private String channelName;
  private String pipeName;

  private SnowflakeTelemetryService mockTelemetryService;
  private InMemorySinkTaskContext sinkTaskContext;

  @BeforeEach
  void setUp() {
    final String uniqueId = UUID.randomUUID().toString().substring(0, 8);
    channelName = "test_channel_" + uniqueId;
    pipeName = "test_pipe_" + uniqueId;

    mockTelemetryService = mock(SnowflakeTelemetryService.class);

    sinkTaskContext =
        new InMemorySinkTaskContext(Collections.singleton(new TopicPartition(TOPIC, PARTITION)));
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

    // DLQ should receive DataException (KCv3-compatible) with original exception as cause
    assertTrue(
        reported.getException() instanceof DataException,
        "DLQ should receive DataException wrapper, got: "
            + reported.getException().getClass().getName());
    assertNotNull(
        reported.getException().getCause(),
        "DataException should have the original exception as cause");
    assertTrue(
        reported.getException().getCause() instanceof SnowflakeKafkaConnectorException,
        "DataException cause should be SnowflakeKafkaConnectorException, got: "
            + reported.getException().getCause().getClass().getName());
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

    // DLQ should receive DataException wrapper with original exception as cause
    assertTrue(
        reported.getException() instanceof DataException,
        "DLQ should receive DataException wrapper, got: "
            + reported.getException().getClass().getName());
    assertNotNull(reported.getException().getCause(), "DataException should have cause");
  }

  @Test
  void multipleBrokenRecords_toleranceAll_withDLQ_shouldSendOnlyBrokenToDLQ() {
    InMemoryKafkaRecordErrorReporter errorReporter = new InMemoryKafkaRecordErrorReporter();
    Map<String, String> config = baseConfig();
    config.put("errors.tolerance", "all");
    config.put("errors.deadletterqueue.topic.name", "my-dlq-topic");

    SnowpipeStreamingPartitionChannel channel = createChannel(config, errorReporter);

    channel.insertRecord(buildBrokenValueRecord(0), true);
    channel.insertRecord(buildValidRecord(1), false);
    channel.insertRecord(buildBrokenValueRecord(2), false);
    channel.insertRecord(buildBrokenValueRecord(3), false);

    assertEquals(3, errorReporter.getReportedRecords().size());
  }

  // ── errors.tolerance = ALL + no DLQ → should silently drop ─────────────────

  @Test
  void brokenRecord_toleranceAll_noDLQ_shouldSilentlyDrop() {
    InMemoryKafkaRecordErrorReporter errorReporter = new InMemoryKafkaRecordErrorReporter();
    Map<String, String> config = baseConfig();
    config.put("errors.tolerance", "all");
    // No DLQ topic configured

    SnowpipeStreamingPartitionChannel channel = createChannel(config, errorReporter);
    SinkRecord brokenSinkRecord = buildBrokenValueRecord(0);

    // Should NOT throw - record is silently dropped with a warning log
    channel.insertRecord(brokenSinkRecord, true);

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

  /** Creates a valid SinkRecord with a schemaless JSON map value. */
  private SinkRecord buildValidRecord(long offset) {
    JsonConverter jsonConverter = new JsonConverter();
    jsonConverter.configure(Collections.singletonMap("schemas.enable", "false"), false);
    SchemaAndValue schemaAndValue =
        jsonConverter.toConnectData(TOPIC, "{\"name\": \"test\"}".getBytes(StandardCharsets.UTF_8));
    return SinkRecordBuilder.forTopicPartition(TOPIC, PARTITION)
        .withSchemaAndValue(schemaAndValue)
        .withOffset(offset)
        .build();
  }

  private SnowpipeStreamingPartitionChannel createChannel(
      Map<String, String> config, InMemoryKafkaRecordErrorReporter errorReporter) {
    StreamingErrorHandler errorHandler =
        new StreamingErrorHandler(config, errorReporter, mockTelemetryService);

    final TopicPartition topicPartition = new TopicPartition(TOPIC, PARTITION);
    final PartitionOffsetTracker offsetTracker =
        new PartitionOffsetTracker(topicPartition, sinkTaskContext, channelName);
    final SnowflakeTelemetryChannelStatus telemetryChannelStatus =
        new SnowflakeTelemetryChannelStatus(
            "test_table",
            "test_connector",
            channelName,
            System.currentTimeMillis(),
            Optional.empty(),
            offsetTracker.persistedOffsetRef(),
            offsetTracker.processedOffsetRef(),
            offsetTracker.consumerGroupOffsetRef());

    boolean enableSchematization =
        Boolean.parseBoolean(
            config.getOrDefault(
                KafkaConnectorConfigParams.SNOWFLAKE_ENABLE_SCHEMATIZATION,
                String.valueOf(
                    KafkaConnectorConfigParams.SNOWFLAKE_ENABLE_SCHEMATIZATION_DEFAULT)));

    return new SnowpipeStreamingPartitionChannel(
        "test_table",
        channelName,
        pipeName,
        new FakeSnowflakeStreamingIngestClient(pipeName, "test_connector"),
        mockTelemetryService,
        telemetryChannelStatus,
        offsetTracker,
        new SnowflakeMetadataConfig(),
        enableSchematization,
        errorHandler,
        TaskMetrics.noop(),
        false,
        null);
  }
}
