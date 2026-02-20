package com.snowflake.kafka.connector.internal.streaming.v2;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.snowflake.ingest.streaming.ChannelStatus;
import com.snowflake.ingest.streaming.ChannelStatusBatch;
import com.snowflake.ingest.streaming.OpenChannelResult;
import com.snowflake.ingest.streaming.SFException;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import com.snowflake.kafka.connector.dlq.InMemoryKafkaRecordErrorReporter;
import com.snowflake.kafka.connector.internal.SnowflakeConnectionService;
import com.snowflake.kafka.connector.internal.TestUtils;
import com.snowflake.kafka.connector.internal.streaming.InMemorySinkTaskContext;
import com.snowflake.kafka.connector.internal.streaming.StreamingClientProperties;
import com.snowflake.kafka.connector.internal.streaming.StreamingErrorHandler;
import com.snowflake.kafka.connector.internal.streaming.v2.client.StreamingClientFactory;
import com.snowflake.kafka.connector.internal.streaming.v2.client.StreamingClientSupplier;
import com.snowflake.kafka.connector.internal.telemetry.SnowflakeTelemetryService;
import com.snowflake.kafka.connector.records.SnowflakeMetadataConfig;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SnowpipeStreamingPartitionChannelTest {

  private static final String TASK_ID = "0";
  private static final String TABLE_NAME = "test_table";
  private static final String TOPIC_NAME = "test_topic";
  private static final int PARTITION = 0;

  // Use unique names per test to avoid StreamingClientPools caching issues
  private String connectorName;
  private String channelName;
  private String pipeName;

  private SnowflakeConnectionService mockConnectionService;
  private SnowflakeTelemetryService mockTelemetryService;
  private InMemorySinkTaskContext sinkTaskContext;
  private StreamingErrorHandler mockErrorHandler;
  private Map<String, String> connectorConfig;
  private TrackingIngestClientSupplier trackingClientSupplier;

  @BeforeEach
  void setUp() {
    // Generate unique names to avoid StreamingClientPools caching issues between tests
    final String uniqueId = UUID.randomUUID().toString().substring(0, 8);
    connectorName = "test_connector_" + uniqueId;
    channelName = "test_channel_" + uniqueId;
    pipeName = "test_pipe_" + uniqueId;

    mockConnectionService = mock(SnowflakeConnectionService.class);
    mockTelemetryService = mock(SnowflakeTelemetryService.class);
    mockErrorHandler = mock(StreamingErrorHandler.class);

    when(mockConnectionService.getTelemetryClient()).thenReturn(mockTelemetryService);

    sinkTaskContext =
        new InMemorySinkTaskContext(
            Collections.singleton(new TopicPartition(TOPIC_NAME, PARTITION)));

    connectorConfig = TestUtils.getConnectorConfigurationForStreaming(false);
    trackingClientSupplier = new TrackingIngestClientSupplier();
    StreamingClientFactory.setStreamingClientSupplier(trackingClientSupplier);
  }

  @AfterEach
  void tearDown() {
    StreamingClientFactory.resetStreamingClientSupplier();
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

    // When: Trigger a channel recovery by calling fetchOffsetTokenWithRetry which will fail
    // and invoke the fallback that reopens the channel
    // We simulate this by directly testing the behavior pattern:
    // The channel should be open at this point
    assertTrue(!partitionChannel.isChannelClosed(), "Channel should be open before recovery");

    // Record close count before recovery
    final int closeCountBeforeRecovery = trackingClientSupplier.getCloseCallCount();
    trackingClientSupplier.setThrowOnOffsetToken(true);

    try {
      // This will trigger the fallback path which reopens the channel
      partitionChannel.fetchOffsetTokenWithRetry();
    } catch (Exception e) {
      // Expected - the retry policy may throw after exhausting retries
    }

    assertEquals(closeCountBeforeRecovery + 1, trackingClientSupplier.getCloseCallCount());
  }

  private SnowpipeStreamingPartitionChannel createPartitionChannel() {
    return new SnowpipeStreamingPartitionChannel(
        TABLE_NAME,
        channelName,
        pipeName,
        new TopicPartition(TOPIC_NAME, PARTITION),
        mockConnectionService,
        connectorConfig,
        new InMemoryKafkaRecordErrorReporter(),
        new SnowflakeMetadataConfig(),
        sinkTaskContext,
        false,
        null,
        connectorName,
        TASK_ID,
        mockErrorHandler);
  }

  /** Custom StreamingClientSupplier that tracks channel operations for verification in tests. */
  static class TrackingIngestClientSupplier implements StreamingClientSupplier {

    private final AtomicInteger closeCallCount = new AtomicInteger(0);
    private final AtomicInteger totalChannelsCreated = new AtomicInteger(0);
    private volatile boolean throwOnOffsetToken;

    @Override
    public SnowflakeStreamingIngestClient get(
        final String clientName,
        final String dbName,
        final String schemaName,
        final String pipeName,
        final Map<String, String> connectorConfig,
        final StreamingClientProperties streamingClientProperties) {
      return new TrackingStreamingIngestClient(pipeName, this);
    }

    int getCloseCallCount() {
      return closeCallCount.get();
    }

    int getTotalChannelsCreated() {
      return totalChannelsCreated.get();
    }

    void setThrowOnOffsetToken(boolean throwOnOffsetToken) {
      this.throwOnOffsetToken = throwOnOffsetToken;
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
      throw new UnsupportedOperationException();
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
    public void appendRow(final Map<String, Object> row, final String offsetToken) {}

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
