package com.snowflake.kafka.connector.internal.streaming.v2;

import static com.snowflake.kafka.connector.internal.streaming.channel.TopicPartitionChannel.NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.snowflake.ingest.streaming.ChannelStatus;
import com.snowflake.ingest.streaming.ChannelStatusBatch;
import com.snowflake.ingest.streaming.OpenChannelResult;
import com.snowflake.ingest.streaming.SFException;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import com.snowflake.kafka.connector.builder.SinkRecordBuilder;
import com.snowflake.kafka.connector.config.SinkTaskConfig;
import com.snowflake.kafka.connector.config.SinkTaskConfigTestBuilder;
import com.snowflake.kafka.connector.config.SnowflakeValidation;
import com.snowflake.kafka.connector.internal.DescribeTableRow;
import com.snowflake.kafka.connector.internal.SnowflakeConnectionService;
import com.snowflake.kafka.connector.internal.metrics.TaskMetrics;
import com.snowflake.kafka.connector.internal.streaming.InMemorySinkTaskContext;
import com.snowflake.kafka.connector.internal.streaming.StreamingErrorHandler;
import com.snowflake.kafka.connector.internal.streaming.telemetry.SnowflakeTelemetryChannelStatus;
import com.snowflake.kafka.connector.internal.streaming.v2.channel.PartitionOffsetTracker;
import com.snowflake.kafka.connector.internal.streaming.v2.migration.Ssv1MigrationMode;
import com.snowflake.kafka.connector.internal.streaming.v2.migration.Ssv1MigrationResponse;
import com.snowflake.kafka.connector.internal.telemetry.SnowflakeTelemetryService;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
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
  private static final String SSV1_CHANNEL_NAME = TOPIC_NAME + "_" + PARTITION;

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
    // Wait for async init to complete
    channel.getChannel();

    // Then: close() should not have been called because channel was null initially
    assertEquals(0, trackingClientSupplier.getCloseCallCount());
  }

  @Test
  void shouldCloseOpenChannelBeforeReopening() {
    // Given: A partition channel is created and its underlying channel is open
    final SnowpipeStreamingPartitionChannel partitionChannel = createPartitionChannel();
    // Wait for async init to complete
    partitionChannel.getChannel();
    assertEquals(1, trackingClientSupplier.getTotalChannelsCreated());
    assertTrue(!partitionChannel.isChannelClosed(), "Channel should be open before recovery");

    // Record close count before recovery
    final int closeCountBeforeRecovery = trackingClientSupplier.getCloseCallCount();

    // When: appendRow throws SFException once, triggering the fallback that reopens the channel.
    // After recovery the fallback completes normally — no exception propagates.
    trackingClientSupplier.setNonRetryableAppendRowFailures(1);
    partitionChannel.insertRecord(buildValidRecord(0));

    // reopenChannel closes the old channel before opening a new one
    assertEquals(closeCountBeforeRecovery + 1, trackingClientSupplier.getCloseCallCount());
    assertEquals(2, trackingClientSupplier.getTotalChannelsCreated());
  }

  @Test
  void closeChannelAsyncCancelsInitializationBeforeChannelOpens() throws Exception {
    // Block the single-threaded executor so the channel init task is queued but not started
    CountDownLatch blockExecutor = new CountDownLatch(1);
    openChannelIoExecutor.submit(
        () -> {
          blockExecutor.await();
          return null;
        });

    SnowpipeStreamingPartitionChannel partitionChannel = createPartitionChannel();

    // closeChannelAsync sets cancelled=true while the init task is still queued
    CompletableFuture<Void> closeFuture = partitionChannel.closeChannelAsync();

    // Unblock the executor — init task starts, sees cancelled=true, throws CancellationException
    blockExecutor.countDown();

    // The close future should complete via the exceptionally branch
    closeFuture.get(5, TimeUnit.SECONDS);

    // No SDK channel was ever opened or closed
    assertEquals(0, trackingClientSupplier.getTotalChannelsCreated());
    assertEquals(0, trackingClientSupplier.getCloseCallCount());
  }

  @Test
  void reopenChannelRecoversAfterFailedAsyncInitialization() {
    // Make the first openChannel call (during async init) throw
    trackingClientSupplier.setThrowOnOpenChannel(true);
    SnowpipeStreamingPartitionChannel partitionChannel = createPartitionChannel();

    // Wait for the async init to complete exceptionally
    assertThrows(SFException.class, partitionChannel::getChannel);
    assertEquals(
        0,
        trackingClientSupplier.getTotalChannelsCreated(),
        "No channels should have been created since openChannel threw");

    // Allow subsequent openChannel calls to succeed (simulating a transient failure)
    trackingClientSupplier.setThrowOnOpenChannel(false);

    // First insertRecord triggers recovery via the Failsafe fallback. reopenChannel handles
    // the failed init future gracefully (skips close, opens a new channel). After successful
    // recovery the record is inserted on the new channel — no exception propagates.
    partitionChannel.insertRecord(buildValidRecord(0));

    assertEquals(
        1,
        trackingClientSupplier.getTotalChannelsCreated(),
        "reopenChannel should have opened a new channel after transient init failure");
  }

  @Test
  void reopenChannelClosesOldChannelWhenAsyncInitSucceeded() {
    SnowpipeStreamingPartitionChannel partitionChannel = createPartitionChannel();
    partitionChannel.getChannel();
    assertEquals(1, trackingClientSupplier.getTotalChannelsCreated());
    assertEquals(0, trackingClientSupplier.getCloseCallCount());

    // Trigger reopenChannel via appendRow SFException (throw once, then succeed on new channel)
    trackingClientSupplier.setNonRetryableAppendRowFailures(1);
    partitionChannel.insertRecord(buildValidRecord(0));

    // reopenChannel should have closed the old channel BEFORE opening the new one
    assertEquals(
        1,
        trackingClientSupplier.getCloseCallCount(),
        "Old channel should have been closed during reopenChannel");
    assertEquals(
        2,
        trackingClientSupplier.getTotalChannelsCreated(),
        "A new channel should have been opened during reopenChannel");
  }

  @Test
  void reopenChannel_runsOnPutThread_notOnIoExecutor() throws Exception {
    SnowpipeStreamingPartitionChannel partitionChannel = createPartitionChannel();
    partitionChannel.getChannel(); // Wait for async first-open to finish.
    assertEquals(1, trackingClientSupplier.getTotalChannelsCreated());

    Thread initialOpenThread = trackingClientSupplier.getOpenChannelThreads().get(0);
    assertNotEquals(
        Thread.currentThread(),
        initialOpenThread,
        "Sanity: initial open should run on openChannelIoExecutor, not the test thread");

    // Trigger recovery: appendRow throws once, reopen runs, then appendRow on the new channel
    // succeeds.
    trackingClientSupplier.setNonRetryableAppendRowFailures(1);
    partitionChannel.insertRecord(buildValidRecord(0));

    assertEquals(2, trackingClientSupplier.getTotalChannelsCreated());

    // The recovery openChannel must have run on the test thread (the "put thread"), not on
    // openChannelIoExecutor. Pre-§B code dispatched it via CompletableFuture.thenApply on the
    // executor — so this assertion fails against today's master.
    Thread recoveryOpenThread = trackingClientSupplier.getOpenChannelThreads().get(1);
    assertEquals(
        Thread.currentThread(),
        recoveryOpenThread,
        "Recovery's openChannel must run on the put thread (synchronous reopen)");
  }

  @Test
  void insertRecordThrowsBackpressureExceptionOnRetryableError() {
    SnowpipeStreamingPartitionChannel partitionChannel = createPartitionChannel();
    partitionChannel.getChannel();
    assertEquals(1, trackingClientSupplier.getTotalChannelsCreated());

    // appendRow will throw MemoryThresholdExceeded (retryable error)
    trackingClientSupplier.setRetryableAppendRowFailures(1);

    // BackpressureException should propagate up (not caught in this layer)
    // Task 4 will handle it at the batch-level insert() loop
    BackpressureException exception =
        assertThrows(
            BackpressureException.class, () -> partitionChannel.insertRecord(buildValidRecord(0)));

    assertEquals("SDK backpressure: MemoryThresholdExceeded", exception.getMessage());

    // No channel reopening should have happened - the exception signals backpressure, not channel
    // invalidation
    assertEquals(0, trackingClientSupplier.getCloseCallCount());
    assertEquals(1, trackingClientSupplier.getTotalChannelsCreated());
  }

  @Test
  void nonSFExceptionFromAppendRowPropagatesWithoutRecovery() {
    SnowpipeStreamingPartitionChannel partitionChannel = createPartitionChannel();
    partitionChannel.getChannel();
    assertEquals(1, trackingClientSupplier.getTotalChannelsCreated());

    IllegalStateException cause = new IllegalStateException("unexpected error");
    trackingClientSupplier.setAppendRowRuntimeException(cause);

    IllegalStateException thrown =
        assertThrows(
            IllegalStateException.class, () -> partitionChannel.insertRecord(buildValidRecord(0)));

    assertSame(cause, thrown);
    // No recovery should have been attempted
    assertEquals(0, trackingClientSupplier.getCloseCallCount());
    assertEquals(1, trackingClientSupplier.getTotalChannelsCreated());
  }

  @Test
  void isInitializingReturnsTrueWhileChannelFutureIsPending() throws Exception {
    // Block the executor so the channel init task is queued but not started
    CountDownLatch blockExecutor = new CountDownLatch(1);
    openChannelIoExecutor.submit(
        () -> {
          blockExecutor.await();
          return null;
        });

    SnowpipeStreamingPartitionChannel partitionChannel = createPartitionChannel();

    assertTrue(partitionChannel.isInitializing(), "Should be initializing while future is pending");

    // Unblock and wait for init to complete
    blockExecutor.countDown();
    partitionChannel.getChannel();

    assertFalse(
        partitionChannel.isInitializing(), "Should not be initializing after future completes");
  }

  @Test
  void channelInvalidationRecovery_taskSurvivesAndContinuesIngesting() {
    // This test validates the fix for the channel invalidation recovery bug:
    // Before the fix, a channel invalidation (SFException on appendRow) would trigger
    // the fallback to reopen the channel, but then unconditionally re-throw the exception,
    // causing the KC framework to kill the task as "unrecoverable".
    // After the fix, the fallback reopens the channel and completes normally, allowing
    // Failsafe to re-execute appendRow on the new channel.

    SnowpipeStreamingPartitionChannel partitionChannel = createPartitionChannel();
    partitionChannel.getChannel();
    assertEquals(1, trackingClientSupplier.getTotalChannelsCreated());

    // Insert first record successfully
    partitionChannel.insertRecord(buildValidRecord(0));

    // Simulate channel invalidation: appendRow throws once (non-retryable SFException),
    // then succeeds on the reopened channel.
    trackingClientSupplier.setNonRetryableAppendRowFailures(1);
    partitionChannel.insertRecord(buildValidRecord(1));

    // The channel should have been reopened (old closed, new opened)
    assertEquals(1, trackingClientSupplier.getCloseCallCount());
    assertEquals(2, trackingClientSupplier.getTotalChannelsCreated());

    // Subsequent records should continue to be ingested on the new channel
    partitionChannel.insertRecord(buildValidRecord(2));
    partitionChannel.insertRecord(buildValidRecord(3));

    // No additional channel reopenings
    assertEquals(1, trackingClientSupplier.getCloseCallCount());
    assertEquals(2, trackingClientSupplier.getTotalChannelsCreated());
  }

  @Test
  void channelInvalidation_failsTaskAfterMaxConsecutiveRecoveries() {
    // If the channel is permanently broken (every appendRow fails), the circuit breaker
    // should trip after MAX_CONSECUTIVE_RECOVERIES (5) and throw ConnectException to
    // kill the task — rather than silently dropping records forever.

    SnowpipeStreamingPartitionChannel partitionChannel = createPartitionChannel();
    partitionChannel.getChannel();
    assertEquals(1, trackingClientSupplier.getTotalChannelsCreated());

    // Every appendRow throws — channel is permanently invalid
    trackingClientSupplier.setThrowOnAppendRow(true);

    // The first 5 records trigger recovery attempts (channel reopening).
    // The 6th record exceeds MAX_CONSECUTIVE_RECOVERIES and throws ConnectException.
    ConnectException thrown =
        assertThrows(
            ConnectException.class,
            () -> {
              for (int i = 0; i < 20; i++) {
                partitionChannel.insertRecord(buildValidRecord(i));
              }
            });

    assertTrue(
        thrown.getMessage().contains("failed after 5 consecutive recovery attempts"),
        "Expected circuit breaker message, got: " + thrown.getMessage());

    // 1 initial + 5 recoveries = 6 channels created before the circuit breaker tripped
    assertEquals(6, trackingClientSupplier.getTotalChannelsCreated());
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

    SinkTaskConfig taskConfig =
        SinkTaskConfigTestBuilder.builder()
            .connectorName(CONNECTOR_NAME)
            .taskId("0")
            .enableSchematization(false)
            .enableColumnIdentifierNormalization(true)
            .validation(SnowflakeValidation.SERVER_SIDE)
            .build();

    return new SnowpipeStreamingPartitionChannel(
        TABLE_NAME,
        channelName,
        pipeName,
        trackingClient,
        openChannelIoExecutor,
        mockTelemetryService,
        telemetryChannelStatus,
        offsetTracker,
        taskConfig,
        mockErrorHandler,
        TaskMetrics.noop(),
        false,
        null,
        Optional.empty());
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
      boolean shouldEvolveSchema) {
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

    SinkTaskConfig taskConfig =
        SinkTaskConfigTestBuilder.builder()
            .connectorName(CONNECTOR_NAME)
            .taskId("0")
            .enableSchematization(enableSchematization)
            .enableColumnIdentifierNormalization(true)
            .validation(SnowflakeValidation.CLIENT_SIDE)
            .build();

    return new SnowpipeStreamingPartitionChannel(
        TABLE_NAME,
        channelName,
        pipeName,
        trackingClient,
        openChannelIoExecutor,
        mockTelemetryService,
        telemetryChannelStatus,
        offsetTracker,
        taskConfig,
        mockErrorHandler,
        TaskMetrics.noop(),
        shouldEvolveSchema,
        mockConnService,
        Optional.empty());
  }

  private static final List<DescribeTableRow> STANDARD_TABLE_SCHEMA =
      Arrays.asList(
          new DescribeTableRow("RECORD_CONTENT", "VARIANT", null, "Y"),
          new DescribeTableRow("RECORD_METADATA", "VARIANT", null, "Y"));

  @Test
  void validationEnabled_validRecord_insertsSuccessfully() {
    // enableSchematization=false so the record is wrapped into RECORD_CONTENT/RECORD_METADATA
    SnowpipeStreamingPartitionChannel channel =
        createValidationEnabledChannel(STANDARD_TABLE_SCHEMA, false, true);
    SinkRecord record = buildValidRecord(0);

    channel.insertRecord(record);

    verify(mockErrorHandler, never()).handleError(any(Exception.class), any(SinkRecord.class));
    assertEquals(1, trackingClientSupplier.getTotalChannelsCreated());
  }

  @Test
  void validationEnabled_extraColumn_triggersSchemaEvolution() {
    // Table only has RECORD_METADATA — RECORD_CONTENT will be "extra"
    List<DescribeTableRow> schema =
        Arrays.asList(new DescribeTableRow("RECORD_METADATA", "VARIANT", null, "Y"));

    SnowpipeStreamingPartitionChannel channel = createValidationEnabledChannel(schema, true, true);

    SinkRecord record = buildValidRecord(0);
    channel.insertRecord(record);

    // Schema evolution attempted, but refreshed schema still missing RECORD_CONTENT -> error
    verify(mockErrorHandler).handleError(any(Exception.class), eq(record));
  }

  @Test
  void validationEnabled_schemaEvolutionDisabled_structuralErrorRoutesToDlq() {
    List<DescribeTableRow> schema =
        Arrays.asList(new DescribeTableRow("RECORD_METADATA", "VARIANT", null, "Y"));

    SnowpipeStreamingPartitionChannel channel = createValidationEnabledChannel(schema, true, false);

    SinkRecord record = buildValidRecord(0);
    channel.insertRecord(record);

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

    SinkTaskConfig taskConfig =
        SinkTaskConfigTestBuilder.builder()
            .connectorName(CONNECTOR_NAME)
            .taskId("0")
            .enableSchematization(true)
            .enableColumnIdentifierNormalization(true)
            .validation(SnowflakeValidation.CLIENT_SIDE)
            .build();

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
            taskConfig,
            mockErrorHandler,
            TaskMetrics.noop(),
            true,
            mockConnService,
            Optional.empty());

    SinkRecord record = buildValidRecord(0);
    channel.insertRecord(record);

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

    // shouldEvolveSchema=true so schema evolution is attempted for the missing NOT NULL
    // col
    SnowpipeStreamingPartitionChannel channel = createValidationEnabledChannel(schema, true, true);

    // Record doesn't have REQUIRED_COL — should trigger structural error
    SinkRecord record = buildValidRecord(0);
    channel.insertRecord(record);

    verify(mockErrorHandler).handleError(any(Exception.class), eq(record));
  }

  @Test
  void validationEnabled_multipleExtraColumns_passesRawColumnNames() {
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

    channel.insertRecord(record);

    verify(mockConnService)
        .appendColumnsToTable(
            eq(TABLE_NAME),
            argThat(
                columnInfos -> {
                  if (columnInfos == null) return false;
                  boolean hasCity = columnInfos.containsKey("CITY");
                  boolean hasAge = columnInfos.containsKey("AGE");
                  boolean hasCountry = columnInfos.containsKey("COUNTRY");
                  return hasCity && hasAge && hasCountry;
                }));
  }

  @Test
  void validationEnabled_identityColumnMissing_insertsSuccessfully() {
    List<DescribeTableRow> schema =
        Arrays.asList(
            new DescribeTableRow(
                "ID", "NUMBER(38,0)", null, "N", null, "IDENTITY START 1 INCREMENT 1"),
            new DescribeTableRow("RECORD_CONTENT", "VARIANT", null, "Y"),
            new DescribeTableRow("RECORD_METADATA", "VARIANT", null, "Y"));

    // enableSchematization=false so the record populates RECORD_CONTENT/RECORD_METADATA only
    SnowpipeStreamingPartitionChannel channel = createValidationEnabledChannel(schema, false, true);
    SinkRecord record = buildValidRecord(0);

    channel.insertRecord(record);

    // Identity column is missing from the row but should not trigger an error
    verify(mockErrorHandler, never()).handleError(any(Exception.class), any(SinkRecord.class));
  }

  @Test
  void validationEnabled_defaultNotNullColumnMissing_insertsSuccessfully() {
    List<DescribeTableRow> schema =
        Arrays.asList(
            new DescribeTableRow("RECORD_CONTENT", "VARIANT", null, "Y"),
            new DescribeTableRow("RECORD_METADATA", "VARIANT", null, "Y"),
            new DescribeTableRow(
                "CREATED_AT", "TIMESTAMP_NTZ(9)", null, "N", "CURRENT_TIMESTAMP()", null));

    SnowpipeStreamingPartitionChannel channel = createValidationEnabledChannel(schema, false, true);
    SinkRecord record = buildValidRecord(0);

    channel.insertRecord(record);

    verify(mockErrorHandler, never()).handleError(any(Exception.class), any(SinkRecord.class));
  }

  // --- SSv1 offset migration tests ---

  private SnowpipeStreamingPartitionChannel createPartitionChannelWithMigration(
      Ssv1MigrationMode migrationMode, SnowflakeConnectionService mockConn) {
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

    SinkTaskConfig migrationTaskConfig =
        SinkTaskConfigTestBuilder.builder()
            .connectorName(CONNECTOR_NAME)
            .taskId("0")
            .enableSchematization(false)
            .enableColumnIdentifierNormalization(true)
            .validation(SnowflakeValidation.SERVER_SIDE)
            .ssv1MigrationMode(migrationMode)
            .build();

    return new SnowpipeStreamingPartitionChannel(
        TABLE_NAME,
        channelName,
        pipeName,
        trackingClient,
        openChannelIoExecutor,
        mockTelemetryService,
        telemetryChannelStatus,
        offsetTracker,
        migrationTaskConfig,
        mockErrorHandler,
        TaskMetrics.noop(),
        false,
        mockConn,
        migrationMode == Ssv1MigrationMode.SKIP
            ? Optional.empty()
            : Optional.of(SSV1_CHANNEL_NAME));
  }

  @Test
  void migration_skip_doesNotConsultSsv1() {
    SnowflakeConnectionService mockConn = mock(SnowflakeConnectionService.class);

    SnowpipeStreamingPartitionChannel channel =
        createPartitionChannelWithMigration(Ssv1MigrationMode.SKIP, mockConn);
    channel.getChannel();

    // System function should never be called when mode is SKIP
    verify(mockConn, never()).migrateSsv1ChannelOffset(any(), any(), any(), any());
  }

  @Test
  void migration_bestEffort_usesSsv1OffsetWhenSsv2HasNone() {
    SnowflakeConnectionService mockConn = mock(SnowflakeConnectionService.class);
    when(mockConn.migrateSsv1ChannelOffset(TABLE_NAME, SSV1_CHANNEL_NAME, channelName, pipeName))
        .thenReturn(Ssv1MigrationResponse.migrated(100L));

    SnowpipeStreamingPartitionChannel channel =
        createPartitionChannelWithMigration(Ssv1MigrationMode.BEST_EFFORT, mockConn);
    channel.getChannel();

    // SSv2 has no offset (null from FakeClient), so SSv1 should be consulted
    verify(mockConn).migrateSsv1ChannelOffset(TABLE_NAME, SSV1_CHANNEL_NAME, channelName, pipeName);
    // Kafka offset should be set to ssv1Offset + 1 (101)
    assertEquals(101L, sinkTaskContext.offset(new TopicPartition(TOPIC_NAME, PARTITION)));
  }

  @Test
  void migration_bestEffort_proceedsWhenSsv1NotFound() {
    SnowflakeConnectionService mockConn = mock(SnowflakeConnectionService.class);
    when(mockConn.migrateSsv1ChannelOffset(TABLE_NAME, SSV1_CHANNEL_NAME, channelName, pipeName))
        .thenReturn(Ssv1MigrationResponse.channelNotFound());

    SnowpipeStreamingPartitionChannel channel =
        createPartitionChannelWithMigration(Ssv1MigrationMode.BEST_EFFORT, mockConn);
    channel.getChannel();

    // SSv1 not found — best_effort falls through to consumer group offset
    verify(mockConn).migrateSsv1ChannelOffset(TABLE_NAME, SSV1_CHANNEL_NAME, channelName, pipeName);
  }

  @Test
  void migration_bestEffort_proceedsWhenSsv1HasNoOffset() {
    SnowflakeConnectionService mockConn = mock(SnowflakeConnectionService.class);
    when(mockConn.migrateSsv1ChannelOffset(TABLE_NAME, SSV1_CHANNEL_NAME, channelName, pipeName))
        .thenReturn(Ssv1MigrationResponse.channelFoundNoOffset());

    SnowpipeStreamingPartitionChannel channel =
        createPartitionChannelWithMigration(Ssv1MigrationMode.BEST_EFFORT, mockConn);
    channel.getChannel();

    // SSv1 channel exists but has no committed offset — best_effort falls through
    verify(mockConn).migrateSsv1ChannelOffset(TABLE_NAME, SSV1_CHANNEL_NAME, channelName, pipeName);
  }

  @Test
  void migration_bestEffort_ignoresSsv1WhenSsv2HasOffset() {
    // Pre-seed an offset in the tracking client so SSv2 openChannel returns a non-null offset
    trackingClient =
        new TrackingStreamingIngestClient(pipeName, trackingClientSupplier) {
          @Override
          public OpenChannelResult openChannel(String channelNameArg, String offsetToken) {
            OpenChannelResult result = super.openChannel(channelNameArg, offsetToken);
            ChannelStatus status =
                new ChannelStatus(
                    "db",
                    "schema",
                    pipeName,
                    channelNameArg,
                    "SUCCESS",
                    "50",
                    Instant.now(),
                    0,
                    0,
                    0,
                    null,
                    null,
                    null,
                    null,
                    Instant.now());
            return new OpenChannelResult(result.getChannel(), status);
          }
        };

    SnowflakeConnectionService mockConn = mock(SnowflakeConnectionService.class);

    SnowpipeStreamingPartitionChannel channel =
        createPartitionChannelWithMigration(Ssv1MigrationMode.BEST_EFFORT, mockConn);
    channel.getChannel();

    // SSv2 already has an offset, so system function should NOT be called
    verify(mockConn, never()).migrateSsv1ChannelOffset(any(), any(), any(), any());
    // Kafka offset should be set to ssv2Offset + 1 (51)
    assertEquals(51L, sinkTaskContext.offset(new TopicPartition(TOPIC_NAME, PARTITION)));
  }

  @Test
  void migration_strict_usesSsv1OffsetWhenFound() {
    SnowflakeConnectionService mockConn = mock(SnowflakeConnectionService.class);
    when(mockConn.migrateSsv1ChannelOffset(TABLE_NAME, SSV1_CHANNEL_NAME, channelName, pipeName))
        .thenReturn(Ssv1MigrationResponse.migrated(100L));

    SnowpipeStreamingPartitionChannel channel =
        createPartitionChannelWithMigration(Ssv1MigrationMode.STRICT, mockConn);
    channel.getChannel();

    // SSv1 found — strict mode migrates the offset just like best_effort
    verify(mockConn).migrateSsv1ChannelOffset(TABLE_NAME, SSV1_CHANNEL_NAME, channelName, pipeName);
    assertEquals(101L, sinkTaskContext.offset(new TopicPartition(TOPIC_NAME, PARTITION)));
  }

  @Test
  void migration_strict_throwsWhenSsv1NotFound() {
    SnowflakeConnectionService mockConn = mock(SnowflakeConnectionService.class);
    when(mockConn.migrateSsv1ChannelOffset(TABLE_NAME, SSV1_CHANNEL_NAME, channelName, pipeName))
        .thenReturn(Ssv1MigrationResponse.channelNotFound());

    SnowpipeStreamingPartitionChannel channel =
        createPartitionChannelWithMigration(Ssv1MigrationMode.STRICT, mockConn);

    // SSv1 not found — strict mode fails rather than falling through
    assertThrows(ConnectException.class, () -> channel.getChannel());
  }

  @Test
  void migration_strict_proceedsWhenSsv1HasNoOffset() {
    SnowflakeConnectionService mockConn = mock(SnowflakeConnectionService.class);
    when(mockConn.migrateSsv1ChannelOffset(TABLE_NAME, SSV1_CHANNEL_NAME, channelName, pipeName))
        .thenReturn(Ssv1MigrationResponse.channelFoundNoOffset());

    SnowpipeStreamingPartitionChannel channel =
        createPartitionChannelWithMigration(Ssv1MigrationMode.STRICT, mockConn);
    channel.getChannel();

    // SSv1 channel exists but has no committed offset — strict does NOT throw because the
    // channel was found (nothing to migrate is different from channel not existing)
    verify(mockConn).migrateSsv1ChannelOffset(TABLE_NAME, SSV1_CHANNEL_NAME, channelName, pipeName);
  }

  @Test
  void migration_ssv2OpenFails_doesNotConsultSsv1() {
    // Simulate SSv2 openChannel failure
    trackingClientSupplier.setThrowOnOpenChannel(true);

    SnowflakeConnectionService mockConn = mock(SnowflakeConnectionService.class);

    SnowpipeStreamingPartitionChannel channel =
        createPartitionChannelWithMigration(Ssv1MigrationMode.BEST_EFFORT, mockConn);

    // SSv2 open failed, so the channel init future should fail
    assertThrows(RuntimeException.class, () -> channel.getChannel());

    // System function should NOT have been called — SSv2 must open successfully first
    verify(mockConn, never()).migrateSsv1ChannelOffset(any(), any(), any(), any());
  }

  @Test
  void migration_systemFunctionFails_propagatesException() {
    SnowflakeConnectionService mockConn = mock(SnowflakeConnectionService.class);
    when(mockConn.migrateSsv1ChannelOffset(TABLE_NAME, SSV1_CHANNEL_NAME, channelName, pipeName))
        .thenThrow(
            new RuntimeException(
                "SYSTEM$MIGRATE_SSV1_CHANNEL_OFFSET failed for ssv1Channel=" + SSV1_CHANNEL_NAME));

    SnowpipeStreamingPartitionChannel channel =
        createPartitionChannelWithMigration(Ssv1MigrationMode.BEST_EFFORT, mockConn);

    // The system function failure must propagate, not silently fall through to consumer group
    // offset. Falling through would risk duplicates if the consumer group offset is behind
    // the SSv1 offset.
    RuntimeException exception = assertThrows(RuntimeException.class, () -> channel.getChannel());
    assertTrue(exception.getMessage().contains("SYSTEM$MIGRATE_SSV1_CHANNEL_OFFSET"));
  }

  @Test
  void migration_bestEffort_consultsSsv1DuringReopenChannel() {
    SnowflakeConnectionService mockConn = mock(SnowflakeConnectionService.class);
    when(mockConn.migrateSsv1ChannelOffset(TABLE_NAME, SSV1_CHANNEL_NAME, channelName, pipeName))
        .thenReturn(Ssv1MigrationResponse.migrated(100L));

    // Fail the initial channel open so no migration fires during construction
    trackingClientSupplier.setThrowOnOpenChannel(true);

    SnowpipeStreamingPartitionChannel channel =
        createPartitionChannelWithMigration(Ssv1MigrationMode.BEST_EFFORT, mockConn);
    assertThrows(RuntimeException.class, () -> channel.getChannel());

    // Initial open failed — system function should not have been called
    verify(mockConn, never()).migrateSsv1ChannelOffset(any(), any(), any(), any());

    // Allow the next openChannel to succeed
    trackingClientSupplier.setThrowOnOpenChannel(false);

    // Trigger reopenChannel via insertRecord: getChannel() re-throws the SFException from the
    // failed init future, which AppendRowWithFallbackPolicy catches and invokes reopenChannel.
    // reopenChannel's .exceptionally() handler handles the failed init, then opens a new channel.
    channel.insertRecord(buildValidRecord(0));

    // Wait for the async reopen to complete
    channel.getChannel();

    // reopenChannel should have consulted SSv1 exactly once (the initial open never reached it)
    verify(mockConn, times(1))
        .migrateSsv1ChannelOffset(TABLE_NAME, SSV1_CHANNEL_NAME, channelName, pipeName);
    // Kafka offset should be set to ssv1Offset + 1 (101)
    assertEquals(101L, sinkTaskContext.offset(new TopicPartition(TOPIC_NAME, PARTITION)));
  }

  /** Shared state holder that tracks channel operations for verification in tests. */
  static class TrackingIngestClientSupplier {

    private final AtomicInteger closeCallCount = new AtomicInteger(0);
    private final AtomicInteger totalChannelsCreated = new AtomicInteger(0);
    private volatile boolean throwOnOffsetToken;
    private volatile boolean throwOnAppendRow;
    private volatile boolean throwOnOpenChannel;
    private final AtomicInteger retryableAppendRowFailures = new AtomicInteger(0);
    private final AtomicInteger nonRetryableAppendRowFailures = new AtomicInteger(0);
    private volatile RuntimeException appendRowRuntimeException;
    private volatile CountDownLatch blockOnOpenChannel;
    private final List<Thread> openChannelThreads = Collections.synchronizedList(new ArrayList<>());

    int getCloseCallCount() {
      return closeCallCount.get();
    }

    int getTotalChannelsCreated() {
      return totalChannelsCreated.get();
    }

    List<Thread> getOpenChannelThreads() {
      return new ArrayList<>(openChannelThreads);
    }

    void setThrowOnOffsetToken(boolean throwOnOffsetToken) {
      this.throwOnOffsetToken = throwOnOffsetToken;
    }

    void setThrowOnAppendRow(boolean throwOnAppendRow) {
      this.throwOnAppendRow = throwOnAppendRow;
    }

    void setThrowOnOpenChannel(boolean throwOnOpenChannel) {
      this.throwOnOpenChannel = throwOnOpenChannel;
    }

    void setRetryableAppendRowFailures(int count) {
      this.retryableAppendRowFailures.set(count);
    }

    void setNonRetryableAppendRowFailures(int count) {
      this.nonRetryableAppendRowFailures.set(count);
    }

    void setAppendRowRuntimeException(RuntimeException exception) {
      this.appendRowRuntimeException = exception;
    }

    void setBlockOnOpenChannel(CountDownLatch latch) {
      this.blockOnOpenChannel = latch;
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
      supplier.openChannelThreads.add(Thread.currentThread());
      if (supplier.throwOnOpenChannel) {
        throw new SFException("OpenChannelFailed", "Test simulated openChannel failure", 0, "");
      }
      CountDownLatch latch = supplier.blockOnOpenChannel;
      if (latch != null) {
        try {
          latch.await();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new RuntimeException(e);
        }
      }
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
      if (supplier.appendRowRuntimeException != null) {
        throw supplier.appendRowRuntimeException;
      }
      if (supplier.retryableAppendRowFailures.getAndUpdate(n -> n > 0 ? n - 1 : 0) > 0) {
        throw new SFException("MemoryThresholdExceeded", "Test simulated backpressure", 0, "");
      }
      if (supplier.nonRetryableAppendRowFailures.getAndUpdate(n -> n > 0 ? n - 1 : 0) > 0) {
        throw new SFException("ChannelInvalidated", "Test simulated channel invalidation", 0, "");
      }
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
