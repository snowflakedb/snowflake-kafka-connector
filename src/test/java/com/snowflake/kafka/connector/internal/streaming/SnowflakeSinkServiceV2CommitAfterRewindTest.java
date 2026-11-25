package com.snowflake.kafka.connector.internal.streaming;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
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
import com.snowflake.kafka.connector.internal.SnowflakeConnectionService;
import com.snowflake.kafka.connector.internal.metrics.TaskMetrics;
import com.snowflake.kafka.connector.internal.streaming.channel.InflightUsage;
import com.snowflake.kafka.connector.internal.streaming.channel.TopicPartitionChannel;
import com.snowflake.kafka.connector.internal.streaming.telemetry.SnowflakeTelemetryChannelStatus;
import com.snowflake.kafka.connector.internal.streaming.v2.SnowpipeStreamingPartitionChannel;
import com.snowflake.kafka.connector.internal.streaming.v2.channel.PartitionOffsetTracker;
import com.snowflake.kafka.connector.internal.streaming.v2.client.StreamingClientFactory;
import com.snowflake.kafka.connector.internal.streaming.v2.service.BatchOffsetFetcher;
import com.snowflake.kafka.connector.internal.streaming.v2.service.PartitionChannelManager;
import com.snowflake.kafka.connector.internal.streaming.v2.service.ThreadPools;
import com.snowflake.kafka.connector.internal.telemetry.SnowflakeTelemetryService;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The offset Kafka Connect commits for a partition after the connector rewinds it, while its
 * channel has no offset token in Snowflake.
 *
 * <p>Kafka Connect's {@code WorkerSinkTask.rewind()} records a rewind target as the partition's
 * last committed offset without committing it, and {@code commitOffsets()} starts from those last
 * committed offsets and overlays what {@code preCommit} returns. A partition left out of {@code
 * preCommit} is therefore committed at its rewind target the next time any other partition moves.
 * If rows before that target were appended but not yet committed in Snowflake, a crash then loses
 * them: the reopened channel has no token and resumes from the Kafka committed offset.
 */
class SnowflakeSinkServiceV2CommitAfterRewindTest {

  private static final String TOPIC = "test_topic";
  private static final int PARTITION = 0;
  private static final String PIPE = "test_pipe";

  private final TopicPartition tp = new TopicPartition(TOPIC, PARTITION);
  private String connectorName;
  private String channelName;
  private ExecutorService ioExecutor;
  private ExecutorService fetchExecutor;
  private InMemorySinkTaskContext sinkTaskContext;
  private ConcurrentHashMap<TopicPartition, Long> pendingOffsetResets;

  @BeforeEach
  void setUp() {
    connectorName = "test_connector_" + UUID.randomUUID().toString().substring(0, 8);
    channelName = PartitionChannelManager.makeChannelName(connectorName, TOPIC, PARTITION);
    ioExecutor = Executors.newSingleThreadExecutor();
    fetchExecutor = Executors.newSingleThreadExecutor();
    sinkTaskContext = new InMemorySinkTaskContext(Set.of(tp));
    pendingOffsetResets = new ConcurrentHashMap<>();
    // Every channel status Snowflake reports for this test has no offset token.
    StreamingClientFactory.setStreamingClientSupplier(
        (clientName, dbName, schemaName, pipeName, properties) -> {
          SnowflakeStreamingIngestClient client = mock(SnowflakeStreamingIngestClient.class);
          when(client.getChannelStatus(any()))
              .thenAnswer(
                  invocation -> {
                    Map<String, ChannelStatus> statuses = new HashMap<>();
                    for (String name : invocation.<List<String>>getArgument(0)) {
                      statuses.put(name, statusWithoutToken(name));
                    }
                    return new ChannelStatusBatch(statuses);
                  });
          return client;
        });
  }

  @AfterEach
  void tearDown() {
    ioExecutor.shutdownNow();
    fetchExecutor.shutdownNow();
    StreamingClientFactory.resetStreamingClientSupplier();
    ThreadPools.closeForTask(connectorName);
  }

  @Test
  void capRewindOfChannelWithoutTokenDoesNotCommitPastAppendedRows() {
    SnowflakeStreamingIngestChannel sdkChannel = sdkChannel(Long.MAX_VALUE);
    SnowflakeStreamingIngestClient client = mock(SnowflakeStreamingIngestClient.class);
    when(client.openChannel(channelName, null))
        .thenReturn(new OpenChannelResult(sdkChannel, statusWithoutToken(channelName)));
    SinkTaskConfig config = config(1);
    PartitionChannelManager channelManager = channelManager(realChannel(client, config));
    // The first batch finds the task under its cap. Afterwards the appended rows stay inflight.
    when(channelManager.sumInflightUsage())
        .thenReturn(InflightUsage.NONE)
        .thenReturn(new InflightUsage(100, 0));
    SnowflakeSinkServiceV2 service = service(config, channelManager);

    service.insert(records(0, 99));
    service.insert(records(100, 120));
    assertEquals(100, sinkTaskContext.offset(tp), "the capped batch rewinds to its first record");

    assertEquals(
        0,
        offsetKafkaConnectCommits(service),
        "rows 0-99 are appended but not committed in Snowflake, so Kafka must not commit past 0");
  }

  @Test
  void reopenOfChannelWithoutTokenDoesNotCommitPastDroppedRows() {
    long failingOffset = 80;
    SnowflakeStreamingIngestChannel firstSdkChannel = sdkChannel(failingOffset);
    SnowflakeStreamingIngestChannel secondSdkChannel = sdkChannel(Long.MAX_VALUE);
    SnowflakeStreamingIngestClient client = mock(SnowflakeStreamingIngestClient.class);
    when(client.openChannel(channelName, null))
        .thenReturn(new OpenChannelResult(firstSdkChannel, statusWithoutToken(channelName)))
        .thenReturn(new OpenChannelResult(secondSdkChannel, statusWithoutToken(channelName)));
    SinkTaskConfig config = config(0);
    SnowflakeSinkServiceV2 service = service(config, channelManager(realChannel(client, config)));

    // Rows 0-79 are appended, row 80 fails and the channel reopens without flushing them.
    service.insert(records(0, 85));
    assertEquals(failingOffset, sinkTaskContext.offset(tp), "the batch rewinds to the failed row");

    assertEquals(
        0,
        offsetKafkaConnectCommits(service),
        "rows 0-79 were dropped with the old channel, so Kafka must not commit past 0");
  }

  /**
   * The offset {@code WorkerSinkTask.commitOffsets()} commits for {@link #tp}: what {@code
   * preCommit} returns, or else the last committed offset, which {@code rewind()} set to the
   * connector's latest rewind target.
   */
  private long offsetKafkaConnectCommits(SnowflakeSinkServiceV2 service) {
    Map<TopicPartition, Long> preCommit = service.getCommittedOffsets(Set.of(tp));
    return preCommit.getOrDefault(tp, sinkTaskContext.offset(tp));
  }

  private SinkTaskConfig config(long maxTaskInflightRows) {
    return SinkTaskConfigTestBuilder.builder()
        .connectorName(connectorName)
        .taskId("0")
        .validation(SnowflakeValidation.SERVER_SIDE)
        .maxTaskInflightRows(maxTaskInflightRows)
        .build();
  }

  private SnowflakeStreamingIngestChannel sdkChannel(long failingOffset) {
    SnowflakeStreamingIngestChannel channel = mock(SnowflakeStreamingIngestChannel.class);
    when(channel.isClosed()).thenReturn(false);
    when(channel.getChannelName()).thenReturn(channelName);
    when(channel.getFullyQualifiedChannelName()).thenReturn(channelName);
    doAnswer(
            invocation -> {
              if (String.valueOf(failingOffset).equals(invocation.getArgument(1))) {
                throw new SFException("ChannelInvalidated", "simulated invalidation", 0, "");
              }
              return null;
            })
        .when(channel)
        .appendRow(any(), any());
    return channel;
  }

  private SnowpipeStreamingPartitionChannel realChannel(
      SnowflakeStreamingIngestClient client, SinkTaskConfig config) {
    Consumer<Long> onOffsetReset = offset -> pendingOffsetResets.put(tp, offset);
    PartitionOffsetTracker offsetTracker = new PartitionOffsetTracker(channelName, onOffsetReset);
    SnowflakeTelemetryChannelStatus telemetryStatus =
        new SnowflakeTelemetryChannelStatus(
            TOPIC,
            connectorName,
            channelName,
            System.currentTimeMillis(),
            Optional.empty(),
            offsetTracker.persistedOffsetRef(),
            offsetTracker.processedOffsetRef(),
            offsetTracker.consumerGroupOffsetRef(),
            offsetTracker.offsetGapCountRef(),
            offsetTracker.offsetGapMissingRecordCountRef());
    SnowpipeStreamingPartitionChannel channel =
        new SnowpipeStreamingPartitionChannel(
            TOPIC,
            channelName,
            PIPE,
            client,
            invalidClient -> {
              throw new UnsupportedOperationException("client recreation is not exercised");
            },
            ioExecutor,
            mock(SnowflakeTelemetryService.class),
            telemetryStatus,
            offsetTracker,
            config,
            mock(StreamingErrorHandler.class),
            TaskMetrics.noop(),
            false,
            false,
            mock(SnowflakeConnectionService.class),
            Optional.empty());
    channel.awaitInitialization();
    return channel;
  }

  private PartitionChannelManager channelManager(SnowpipeStreamingPartitionChannel channel) {
    PartitionChannelManager manager = mock(PartitionChannelManager.class);
    // A cached sum at every cap, so each cap decision reads every channel through sumInflightUsage.
    when(manager.sumInflightUsageReadingAppended(any()))
        .thenReturn(new InflightUsage(Long.MAX_VALUE, Long.MAX_VALUE));
    when(manager.getChannel(tp)).thenReturn(Optional.of(channel));
    when(manager.getChannel(channelName)).thenReturn(Optional.of(channel));
    Map<String, TopicPartitionChannel> channels = new ConcurrentHashMap<>();
    channels.put(channelName, channel);
    when(manager.getPartitionChannels()).thenReturn(channels);
    when(manager.drainPendingOffsetResets(any()))
        .thenAnswer(
            invocation -> {
              Set<TopicPartition> assignment = invocation.getArgument(0);
              Map<TopicPartition, Long> drained = new HashMap<>();
              for (TopicPartition key : pendingOffsetResets.keySet()) {
                Long offset = pendingOffsetResets.remove(key);
                if (offset != null && assignment.contains(key)) {
                  drained.put(key, offset);
                }
              }
              return drained;
            });
    return manager;
  }

  private SnowflakeSinkServiceV2 service(
      SinkTaskConfig config, PartitionChannelManager channelManager) {
    SnowflakeConnectionService connection = mock(SnowflakeConnectionService.class);
    when(connection.isClosed()).thenReturn(false);
    BatchOffsetFetcher fetcher =
        new BatchOffsetFetcher(connectorName, "0", config, fetchExecutor, TaskMetrics.noop());
    SnowflakeSinkServiceV2 service =
        new SnowflakeSinkServiceV2(
            connection,
            config,
            sinkTaskContext,
            Optional.empty(),
            () -> fetcher,
            () -> channelManager,
            TaskMetrics.noop());
    AdvancingClock clock = new AdvancingClock();
    service.clock = clock;
    service.sleeper = clock::advance;
    return service;
  }

  private static ChannelStatus statusWithoutToken(String channelName) {
    return new ChannelStatus(
        "db",
        "schema",
        PIPE,
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

  private static List<SinkRecord> records(long fromOffset, long toOffset) {
    JsonConverter converter = new JsonConverter();
    converter.configure(Collections.singletonMap("schemas.enable", "false"), false);
    List<SinkRecord> records = new ArrayList<>();
    for (long offset = fromOffset; offset <= toOffset; offset++) {
      SchemaAndValue value =
          converter.toConnectData(TOPIC, "{\"name\": \"test\"}".getBytes(StandardCharsets.UTF_8));
      records.add(
          SinkRecordBuilder.forTopicPartition(TOPIC, PARTITION)
              .withSchemaAndValue(value)
              .withOffset(offset)
              .build());
    }
    return records;
  }

  /** A clock that only moves when the service sleeps, so the cap wait ends without real time. */
  private static final class AdvancingClock extends Clock {
    private Instant now = Instant.parse("2026-01-01T00:00:00Z");

    void advance(java.time.Duration duration) {
      now = now.plus(duration);
    }

    @Override
    public Instant instant() {
      return now;
    }

    @Override
    public ZoneId getZone() {
      return ZoneOffset.UTC;
    }

    @Override
    public Clock withZone(ZoneId zone) {
      return this;
    }
  }
}
