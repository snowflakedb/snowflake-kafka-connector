package com.snowflake.kafka.connector.internal.streaming.v2;

import static com.snowflake.kafka.connector.internal.streaming.channel.TopicPartitionChannel.NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.snowflake.ingest.streaming.ChannelStatus;
import com.snowflake.ingest.streaming.ChannelStatusBatch;
import com.snowflake.ingest.streaming.OpenChannelResult;
import com.snowflake.ingest.streaming.SFException;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import com.snowflake.kafka.connector.builder.SinkRecordBuilder;
import com.snowflake.kafka.connector.internal.DescribeTableRow;
import com.snowflake.kafka.connector.internal.SnowflakeConnectionService;
import com.snowflake.kafka.connector.internal.metrics.TaskMetrics;
import com.snowflake.kafka.connector.internal.streaming.InMemorySinkTaskContext;
import com.snowflake.kafka.connector.internal.streaming.StreamingErrorHandler;
import com.snowflake.kafka.connector.internal.streaming.TopicPartitionChannelInsertionException;
import com.snowflake.kafka.connector.internal.streaming.telemetry.SnowflakeTelemetryChannelStatus;
import com.snowflake.kafka.connector.internal.streaming.v2.channel.PartitionOffsetTracker;
import com.snowflake.kafka.connector.internal.telemetry.SnowflakeTelemetryService;
import com.snowflake.kafka.connector.records.SnowflakeMetadataConfig;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SnowpipeStreamingPartitionChannelTest {

  private static final String CONNECTOR_NAME = "test_connector";
  private static final String TABLE_NAME = "test_table";
  private static final String TOPIC_NAME = "test_topic";
  private static final int PARTITION = 0;

  private String channelName;
  private String pipeName;

  private SnowflakeTelemetryService mockTelemetryService;
  private StreamingErrorHandler mockErrorHandler;
  private ExecutorService openChannelIoExecutor;
  private TrackingIngestClientSupplier trackingClientSupplier;
  private TrackingStreamingIngestClient trackingClient;
  private InMemorySinkTaskContext sinkTaskContext;

  @BeforeEach
  void setUp() {
    // Generate unique names to avoid StreamingClientPools caching issues between tests
    final String uniqueId = UUID.randomUUID().toString().substring(0, 8);
    channelName = "test_channel_" + uniqueId;
    pipeName = "test_pipe_" + uniqueId;

    mockTelemetryService = mock(SnowflakeTelemetryService.class);
    mockErrorHandler = mock(StreamingErrorHandler.class);

    sinkTaskContext =
        new InMemorySinkTaskContext(
            Collections.singleton(new TopicPartition(TOPIC_NAME, PARTITION)));

    trackingClientSupplier = new TrackingIngestClientSupplier();
    trackingClient = new TrackingStreamingIngestClient(pipeName, trackingClientSupplier);
    openChannelIoExecutor = Executors.newSingleThreadExecutor();
  }

  @AfterEach
  void tearDown() {
    openChannelIoExecutor.shutdownNow();
  }

  @Test
  void shouldNotCloseChannelOnFirstOpen() {
    // When: Creating a new channel (first open)
    final SnowpipeStreamingPartitionChannel channel = createPartitionChannel();

    // Then: close() should not have been called because channel was null initially
    assertEquals(0, trackingClientSupplier.getCloseCallCount());
  }

  @Test
  void shouldCloseOpenChannelBeforeReopening() {
    // Given: A partition channel is created and its underlying channel is open
    final SnowpipeStreamingPartitionChannel partitionChannel = createPartitionChannel();
    assertEquals(1, trackingClientSupplier.getTotalChannelsCreated());
    assertTrue(!partitionChannel.isChannelClosed(), "Channel should be open before recovery");

    // Record close count before recovery
    final int closeCountBeforeRecovery = trackingClientSupplier.getCloseCallCount();

    // When: appendRow throws SFException, triggering the fallback that reopens the channel
    trackingClientSupplier.setThrowOnAppendRow(true);
    RuntimeException thrown =
        assertThrows(
            RuntimeException.class, () -> partitionChannel.insertRecord(buildValidRecord(0), true));
    assertTrue(
        thrown.getCause() instanceof TopicPartitionChannelInsertionException,
        "Expected TopicPartitionChannelInsertionException cause, got: " + thrown.getCause());

    // Then: the old channel should have been closed before the new one was opened
    assertEquals(closeCountBeforeRecovery + 1, trackingClientSupplier.getCloseCallCount());
    assertEquals(2, trackingClientSupplier.getTotalChannelsCreated());
  }

  private SinkRecord buildValidRecord(long offset) {
    JsonConverter jsonConverter = new JsonConverter();
    jsonConverter.configure(Collections.singletonMap("schemas.enable", "false"), false);
    SchemaAndValue schemaAndValue =
        jsonConverter.toConnectData(
            TOPIC_NAME, "{\"name\": \"test\"}".getBytes(StandardCharsets.UTF_8));
    return SinkRecordBuilder.forTopicPartition(TOPIC_NAME, PARTITION)
        .withSchemaAndValue(schemaAndValue)
        .withOffset(offset)
        .build();
  }

  private SnowpipeStreamingPartitionChannel createPartitionChannel() {
    final TopicPartition topicPartition = new TopicPartition(TOPIC_NAME, PARTITION);
    final PartitionOffsetTracker offsetTracker =
        new PartitionOffsetTracker(topicPartition, sinkTaskContext, channelName);
    final SnowflakeTelemetryChannelStatus telemetryChannelStatus =
        new SnowflakeTelemetryChannelStatus(
            TABLE_NAME,
            CONNECTOR_NAME,
            channelName,
            System.currentTimeMillis(),
            Optional.empty(),
            offsetTracker.persistedOffsetRef(),
            offsetTracker.processedOffsetRef(),
            offsetTracker.consumerGroupOffsetRef());

    return new SnowpipeStreamingPartitionChannel(
        TABLE_NAME,
        channelName,
        pipeName,
        trackingClient,
        openChannelIoExecutor,
        mockTelemetryService,
        telemetryChannelStatus,
        offsetTracker,
        new SnowflakeMetadataConfig(),
        false,
        mockErrorHandler,
        TaskMetrics.noop(),
        false,
        false,
        null);
  }

  @Test
  void parseOffsetToken_nullReturnsNoOffset() {
    assertEquals(
        NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE,
        SnowpipeStreamingPartitionChannel.parseOffsetToken(null, "test_channel"));
  }

  @Test
  void parseOffsetToken_validToken() {
    assertEquals(42L, SnowpipeStreamingPartitionChannel.parseOffsetToken("42", "test_channel"));
    assertEquals(0L, SnowpipeStreamingPartitionChannel.parseOffsetToken("0", "test_channel"));
    assertEquals(
        Long.MAX_VALUE,
        SnowpipeStreamingPartitionChannel.parseOffsetToken(
            String.valueOf(Long.MAX_VALUE), "test_channel"));
  }

  @Test
  void parseOffsetToken_invalidTokenThrows() {
    assertThrows(
        ConnectException.class,
        () -> SnowpipeStreamingPartitionChannel.parseOffsetToken("not_a_number", "test_channel"));
    assertThrows(
        ConnectException.class,
        () -> SnowpipeStreamingPartitionChannel.parseOffsetToken("", "test_channel"));
    assertThrows(
        ConnectException.class,
        () -> SnowpipeStreamingPartitionChannel.parseOffsetToken("12.5", "test_channel"));
  }

  // --- Validation integration tests ---

  private SnowflakeConnectionService mockConnService;

  private SnowpipeStreamingPartitionChannel createValidationEnabledChannel(
      List<DescribeTableRow> describeResult,
      boolean enableSchematization,
      boolean hasSchemaEvolutionPermission) {
    mockConnService = mock(SnowflakeConnectionService.class);
    when(mockConnService.describeTable(TABLE_NAME)).thenReturn(Optional.of(describeResult));

    final TopicPartition topicPartition = new TopicPartition(TOPIC_NAME, PARTITION);
    final PartitionOffsetTracker offsetTracker =
        new PartitionOffsetTracker(topicPartition, sinkTaskContext, channelName);
    final SnowflakeTelemetryChannelStatus telemetryChannelStatus =
        new SnowflakeTelemetryChannelStatus(
            TABLE_NAME,
            CONNECTOR_NAME,
            channelName,
            System.currentTimeMillis(),
            Optional.empty(),
            offsetTracker.persistedOffsetRef(),
            offsetTracker.processedOffsetRef(),
            offsetTracker.consumerGroupOffsetRef());

    return new SnowpipeStreamingPartitionChannel(
        TABLE_NAME,
        channelName,
        pipeName,
        trackingClient,
        openChannelIoExecutor,
        mockTelemetryService,
        telemetryChannelStatus,
        offsetTracker,
        new SnowflakeMetadataConfig(),
        enableSchematization,
        mockErrorHandler,
        TaskMetrics.noop(),
        true,
        hasSchemaEvolutionPermission,
        mockConnService);
  }

  private static final List<DescribeTableRow> STANDARD_TABLE_SCHEMA =
      Arrays.asList(
          new DescribeTableRow("RECORD_CONTENT", "VARIANT", null, "Y"),
          new DescribeTableRow("RECORD_METADATA", "VARIANT", null, "Y"));

  private static final List<DescribeTableRow> SCHEMATIZED_TABLE_SCHEMA =
      Arrays.asList(
          new DescribeTableRow("RECORD_METADATA", "VARIANT", null, "Y"),
          new DescribeTableRow("NAME", "VARCHAR(16777216)", null, "Y"));

  @Test
  void validationEnabled_validRecord_insertsSuccessfully() {
    // enableSchematization=true: record {"name":"test"} becomes flat column NAME
    SnowpipeStreamingPartitionChannel channel =
        createValidationEnabledChannel(SCHEMATIZED_TABLE_SCHEMA, true, true);
    SinkRecord record = buildValidRecord(0);

    channel.insertRecord(record, true);

    verify(mockErrorHandler, never()).handleError(any(Exception.class), any(SinkRecord.class));
    assertEquals(1, trackingClientSupplier.getTotalChannelsCreated());
  }

  @Test
  void schematizationOff_skipsValidation_evenIfConfiguredTrue() {
    // When schematization=false, client validation is implicitly disabled
    List<DescribeTableRow> schema =
        Arrays.asList(new DescribeTableRow("RECORD_METADATA", "VARIANT", null, "Y"));

    // Pass clientValidationEnabled=true in the constructor, but schematization=false
    // should override it to effectively false
    SnowpipeStreamingPartitionChannel channel = createValidationEnabledChannel(schema, false, true);
    SinkRecord record = buildValidRecord(0);

    channel.insertRecord(record, true);

    // No validation error even though table is missing RECORD_CONTENT — validation was skipped
    verify(mockErrorHandler, never()).handleError(any(Exception.class), any(SinkRecord.class));
  }

  @Test
  void validationEnabled_extraColumn_triggersSchemaEvolution() {
    // Table only has RECORD_METADATA — RECORD_CONTENT will be "extra"
    List<DescribeTableRow> schema =
        Arrays.asList(new DescribeTableRow("RECORD_METADATA", "VARIANT", null, "Y"));

    SnowpipeStreamingPartitionChannel channel = createValidationEnabledChannel(schema, true, true);

    SinkRecord record = buildValidRecord(0);
    channel.insertRecord(record, true);

    // Schema evolution attempted, but refreshed schema still missing RECORD_CONTENT -> error
    verify(mockErrorHandler).handleError(any(Exception.class), eq(record));
  }

  @Test
  void validationEnabled_schemaEvolutionDisabled_structuralErrorRoutesToDlq() {
    List<DescribeTableRow> schema =
        Arrays.asList(new DescribeTableRow("RECORD_METADATA", "VARIANT", null, "Y"));

    SnowpipeStreamingPartitionChannel channel = createValidationEnabledChannel(schema, true, false);

    SinkRecord record = buildValidRecord(0);
    channel.insertRecord(record, true);

    verify(mockErrorHandler).handleError(any(Exception.class), eq(record));
    verify(mockConnService, never()).appendColumnsToTable(any(), any());
    verify(mockConnService, never()).alterNonNullableColumns(any(), any());
  }

  @Test
  void validationEnabled_describeTableFails_disablesValidation() {
    mockConnService = mock(SnowflakeConnectionService.class);
    when(mockConnService.describeTable(TABLE_NAME)).thenReturn(Optional.empty());

    final TopicPartition topicPartition = new TopicPartition(TOPIC_NAME, PARTITION);
    final PartitionOffsetTracker offsetTracker =
        new PartitionOffsetTracker(topicPartition, sinkTaskContext, channelName);
    final SnowflakeTelemetryChannelStatus telemetryChannelStatus =
        new SnowflakeTelemetryChannelStatus(
            TABLE_NAME,
            CONNECTOR_NAME,
            channelName,
            System.currentTimeMillis(),
            Optional.empty(),
            offsetTracker.persistedOffsetRef(),
            offsetTracker.processedOffsetRef(),
            offsetTracker.consumerGroupOffsetRef());

    SnowpipeStreamingPartitionChannel channel =
        new SnowpipeStreamingPartitionChannel(
            TABLE_NAME,
            channelName,
            pipeName,
            trackingClient,
            openChannelIoExecutor,
            mockTelemetryService,
            telemetryChannelStatus,
            offsetTracker,
            new SnowflakeMetadataConfig(),
            true,
            mockErrorHandler,
            TaskMetrics.noop(),
            true,
            true,
            mockConnService);

    SinkRecord record = buildValidRecord(0);
    channel.insertRecord(record, true);

    verify(mockErrorHandler, never()).handleError(any(Exception.class), any(SinkRecord.class));
  }

  @Test
  void validationEnabled_notNullColumn_detectsMissingValue() {
    // RECORD_CONTENT and RECORD_METADATA are nullable, but REQUIRED_COL is NOT NULL
    List<DescribeTableRow> schema =
        Arrays.asList(
            new DescribeTableRow("RECORD_CONTENT", "VARIANT", null, "Y"),
            new DescribeTableRow("RECORD_METADATA", "VARIANT", null, "Y"),
            new DescribeTableRow("REQUIRED_COL", "VARCHAR(100)", null, "N"));

    // hasSchemaEvolutionPermission=true so schema evolution is attempted for the missing NOT NULL
    // col
    SnowpipeStreamingPartitionChannel channel = createValidationEnabledChannel(schema, true, true);

    // Record doesn't have REQUIRED_COL — should trigger structural error
    SinkRecord record = buildValidRecord(0);
    channel.insertRecord(record, true);

    verify(mockErrorHandler).handleError(any(Exception.class), eq(record));
  }

  @Test
  void validationEnabled_multipleExtraColumns_passesCorrectlyQuotedNames() {
    List<DescribeTableRow> schema =
        Arrays.asList(new DescribeTableRow("RECORD_METADATA", "VARIANT", null, "Y"));

    SnowpipeStreamingPartitionChannel channel = createValidationEnabledChannel(schema, true, true);

    String json = "{\"city\": \"Hsinchu\", \"age\": 25, \"country\": \"TW\"}";
    JsonConverter jsonConverter = new JsonConverter();
    jsonConverter.configure(Collections.singletonMap("schemas.enable", "false"), false);
    SchemaAndValue schemaAndValue =
        jsonConverter.toConnectData(TOPIC_NAME, json.getBytes(StandardCharsets.UTF_8));
    SinkRecord record =
        SinkRecordBuilder.forTopicPartition(TOPIC_NAME, PARTITION)
            .withSchemaAndValue(schemaAndValue)
            .withOffset(0)
            .build();

    channel.insertRecord(record, true);

    verify(mockConnService)
        .appendColumnsToTable(
            eq(TABLE_NAME),
            argThat(
                columnInfos -> {
                  if (columnInfos == null) return false;
                  boolean hasCity = columnInfos.containsKey("\"CITY\"");
                  boolean hasAge = columnInfos.containsKey("\"AGE\"");
                  boolean hasCountry = columnInfos.containsKey("\"COUNTRY\"");
                  return hasCity && hasAge && hasCountry;
                }));
  }

  /** Shared state holder that tracks channel operations for verification in tests. */
  static class TrackingIngestClientSupplier {

    private final AtomicInteger closeCallCount = new AtomicInteger(0);
    private final AtomicInteger totalChannelsCreated = new AtomicInteger(0);
    private volatile boolean throwOnOffsetToken;
    private volatile boolean throwOnAppendRow;

    int getCloseCallCount() {
      return closeCallCount.get();
    }

    int getTotalChannelsCreated() {
      return totalChannelsCreated.get();
    }

    void setThrowOnOffsetToken(boolean throwOnOffsetToken) {
      this.throwOnOffsetToken = throwOnOffsetToken;
    }

    void setThrowOnAppendRow(boolean throwOnAppendRow) {
      this.throwOnAppendRow = throwOnAppendRow;
    }

    void incrementCloseCallCount() {
      closeCallCount.incrementAndGet();
    }

    int incrementChannelsCreated() {
      return totalChannelsCreated.incrementAndGet();
    }
  }

  /** Streaming ingest client that creates tracking channels. */
  static class TrackingStreamingIngestClient implements SnowflakeStreamingIngestClient {

    private final String pipeName;
    private final TrackingIngestClientSupplier supplier;
    private final ConcurrentHashMap<String, TrackingStreamingIngestChannel> channels =
        new ConcurrentHashMap<>();

    TrackingStreamingIngestClient(
        final String pipeName, final TrackingIngestClientSupplier supplier) {
      this.pipeName = pipeName;
      this.supplier = supplier;
    }

    @Override
    public OpenChannelResult openChannel(final String channelName, final String offsetToken) {
      supplier.incrementChannelsCreated();
      final ChannelStatus channelStatus =
          new ChannelStatus(
              "db",
              "schema",
              pipeName,
              channelName,
              "SUCCESS",
              offsetToken,
              Instant.now(),
              0,
              0,
              0,
              null,
              null,
              null,
              null,
              Instant.now());
      final TrackingStreamingIngestChannel channel =
          new TrackingStreamingIngestChannel(pipeName, channelName, supplier);
      channels.put(channelName, channel);
      return new OpenChannelResult(channel, channelStatus);
    }

    @Override
    public OpenChannelResult openChannel(final String channelName) {
      return openChannel(channelName, null);
    }

    @Override
    public void close() {}

    @Override
    public CompletableFuture<Void> close(
        final boolean waitForFlush, final Duration timeoutDuration) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void initiateFlush() {}

    @Override
    public void dropChannel(final String channelName) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, String> getLatestCommittedOffsetTokens(final List<String> channelNames) {
      throw new UnsupportedOperationException();
    }

    @Override
    public ChannelStatusBatch getChannelStatus(final List<String> channelNames) {
      Map<String, ChannelStatus> statusMap = new HashMap<>();
      for (String name : channelNames) {
        TrackingStreamingIngestChannel ch = channels.get(name);
        if (ch != null) {
          statusMap.put(name, ch.getChannelStatus());
        }
      }
      return new ChannelStatusBatch(statusMap);
    }

    @Override
    public boolean isClosed() {
      return false;
    }

    @Override
    public CompletableFuture<Void> waitForFlush(final Duration timeoutDuration) {
      throw new UnsupportedOperationException();
    }

    @Override
    public String getDBName() {
      throw new UnsupportedOperationException();
    }

    @Override
    public String getSchemaName() {
      throw new UnsupportedOperationException();
    }

    @Override
    public String getPipeName() {
      return pipeName;
    }

    @Override
    public String getClientName() {
      throw new UnsupportedOperationException();
    }
  }

  /** Streaming ingest channel that tracks close() calls. */
  static class TrackingStreamingIngestChannel implements SnowflakeStreamingIngestChannel {

    private final String pipeName;
    private final String channelName;
    private final TrackingIngestClientSupplier supplier;
    private volatile boolean closed = false;

    TrackingStreamingIngestChannel(
        final String pipeName,
        final String channelName,
        final TrackingIngestClientSupplier supplier) {
      this.pipeName = pipeName;
      this.channelName = channelName;
      this.supplier = supplier;
    }

    @Override
    public String getDBName() {
      throw new UnsupportedOperationException();
    }

    @Override
    public String getSchemaName() {
      throw new UnsupportedOperationException();
    }

    @Override
    public String getPipeName() {
      return pipeName;
    }

    @Override
    public String getFullyQualifiedPipeName() {
      return pipeName;
    }

    @Override
    public String getFullyQualifiedChannelName() {
      return channelName;
    }

    @Override
    public boolean isClosed() {
      return closed;
    }

    @Override
    public String getChannelName() {
      return channelName;
    }

    @Override
    public void close() {
      closed = true;
      supplier.incrementCloseCallCount();
    }

    @Override
    public void close(final boolean waitForFlush, final Duration timeoutDuration) {
      close();
    }

    @Override
    public void appendRow(final Map<String, Object> row, final String offsetToken) {
      if (supplier.throwOnAppendRow) {
        throw new SFException("ChannelInvalidated", "Test simulated channel invalidation", 0, "");
      }
    }

    @Override
    public void appendRows(
        final Iterable<Map<String, Object>> rows,
        final String startOffsetToken,
        final String endOffsetToken) {}

    @Override
    public String getLatestCommittedOffsetToken() {
      if (supplier.throwOnOffsetToken) {
        throw new SFException("ChannelInvalidated", "Test simulated channel invalidation", 0, "");
      }
      return null;
    }

    @Override
    public ChannelStatus getChannelStatus() {
      return new ChannelStatus(
          "db",
          "schema",
          pipeName,
          channelName,
          "SUCCESS",
          null,
          Instant.now(),
          0,
          0,
          0,
          null,
          null,
          null,
          null,
          Instant.now());
    }

    @Override
    public CompletableFuture<Void> waitForCommit(
        final Predicate<String> tokenChecker, final Duration timeoutDuration) {
      throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Void> waitForFlush(final Duration timeoutDuration) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void initiateFlush() {}
  }
}
