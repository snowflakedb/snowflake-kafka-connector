package com.snowflake.kafka.connector.internal.streaming;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.snowflake.ingest.streaming.ChannelStatus;
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
import com.snowflake.kafka.connector.internal.streaming.channel.TopicPartitionChannel;
import com.snowflake.kafka.connector.internal.streaming.telemetry.SnowflakeTelemetryChannelStatus;
import com.snowflake.kafka.connector.internal.streaming.v2.SnowpipeStreamingPartitionChannel;
import com.snowflake.kafka.connector.internal.streaming.v2.channel.PartitionOffsetTracker;
import com.snowflake.kafka.connector.internal.streaming.v2.service.BatchOffsetFetcher;
import com.snowflake.kafka.connector.internal.streaming.v2.service.PartitionChannelManager;
import com.snowflake.kafka.connector.internal.streaming.v2.service.ThreadPools;
import com.snowflake.kafka.connector.internal.telemetry.SnowflakeTelemetryService;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Integration test demonstrating the recovery offset overwrite bug.
 *
 * <p>When appendRow throws SFException, the recovery path ({@code reopenChannel} → {@code
 * resetAfterRecovery}) calls {@code sinkTaskContext.offset(tp, committedOffset + 1)} to rewind
 * Kafka to the correct resume point. However, after recovery returns, {@code
 * SnowflakeSinkServiceV2.insert(Collection)} adds the failed record's offset to {@code
 * offsetsOfFirstSkippedRecord}, which overwrites the recovery offset at the end of the batch. This
 * causes records between the committed offset and the failed record's offset to be permanently
 * lost.
 *
 * <p>This test should <b>FAIL</b> on master and <b>PASS</b> on the fix branch that moves offset
 * resets to the task thread via {@code pendingOffsetResets}.
 */
class SnowflakeSinkServiceV2RecoveryOffsetTest {

  private static final String TOPIC = "test_topic";
  private static final String CONNECTOR_NAME = "test_connector";
  private static final int PARTITION = 0;

  private ExecutorService ioExecutor;

  @BeforeEach
  void setUp() {
    ioExecutor = Executors.newSingleThreadExecutor();
  }

  @AfterEach
  void tearDown() {
    ioExecutor.shutdownNow();
    ThreadPools.closeForTask(CONNECTOR_NAME);
  }

  /**
   * Scenario:
   *
   * <ol>
   *   <li>Channel is opened with committed offset 50 in Snowflake.
   *   <li>Records 51–85 arrive in a single batch.
   *   <li>Records 51–79 are successfully appended.
   *   <li>Record 80 triggers a non-retryable SFException (channel invalidation).
   *   <li>Recovery: channel is reopened, committed offset is still 50 → {@code
   *       resetAfterRecovery(50)} calls {@code sinkTaskContext.offset(tp, 51)}.
   *   <li>SSV2 adds offset 80 to {@code offsetsOfFirstSkippedRecord}.
   *   <li>End of batch: {@code sinkTaskContext.offset(tp, 80)} overwrites the recovery offset.
   *   <li>Batch 2 (records 80–85): on master, records process normally — records 51–79 are lost.
   * </ol>
   *
   * <p>Expected: after two batches, the effective offset should be 51 (the recovery offset), not
   * 80. Records 51–79 must be replayed.
   */
  @Test
  void recoveryOffsetShouldNotBeOverwrittenBySkippedRecordOffset() {
    TopicPartition tp = new TopicPartition(TOPIC, PARTITION);
    String channelName = PartitionChannelManager.makeChannelName(CONNECTOR_NAME, TOPIC, PARTITION);
    String pipeName = "test_pipe";
    long committedOffset = 50L;
    long failingOffset = 80L;

    InMemorySinkTaskContext sinkTaskContext = new InMemorySinkTaskContext(Set.of(tp));

    // --- SDK mocks ---

    // First SDK channel: appendRow succeeds for offsets < failingOffset, throws at failingOffset
    SnowflakeStreamingIngestChannel firstSdkChannel = mock(SnowflakeStreamingIngestChannel.class);
    when(firstSdkChannel.isClosed()).thenReturn(false);
    when(firstSdkChannel.getChannelName()).thenReturn(channelName);
    when(firstSdkChannel.getFullyQualifiedChannelName()).thenReturn(channelName);
    doAnswer(
            invocation -> {
              String offsetToken = invocation.getArgument(1);
              if (String.valueOf(failingOffset).equals(offsetToken)) {
                throw new SFException(
                    "ChannelInvalidated", "simulated channel invalidation", 0, "");
              }
              return null;
            })
        .when(firstSdkChannel)
        .appendRow(any(), any());

    // Second SDK channel (after recovery): appendRow always succeeds
    SnowflakeStreamingIngestChannel secondSdkChannel = mock(SnowflakeStreamingIngestChannel.class);
    when(secondSdkChannel.isClosed()).thenReturn(false);
    when(secondSdkChannel.getChannelName()).thenReturn(channelName);
    when(secondSdkChannel.getFullyQualifiedChannelName()).thenReturn(channelName);

    // Both openChannel calls return committed offset = 50
    ChannelStatus statusWithCommittedOffset =
        new ChannelStatus(
            "db",
            "schema",
            pipeName,
            channelName,
            "SUCCESS",
            String.valueOf(committedOffset),
            Instant.now(),
            0,
            0,
            0,
            null,
            null,
            null,
            null,
            Instant.now());

    SnowflakeStreamingIngestClient mockClient = mock(SnowflakeStreamingIngestClient.class);
    when(mockClient.openChannel(channelName, null))
        .thenReturn(new OpenChannelResult(firstSdkChannel, statusWithCommittedOffset))
        .thenReturn(new OpenChannelResult(secondSdkChannel, statusWithCommittedOffset));

    // --- Build configuration ---

    SinkTaskConfig taskConfig =
        SinkTaskConfigTestBuilder.builder()
            .connectorName(CONNECTOR_NAME)
            .taskId("0")
            .validation(SnowflakeValidation.SERVER_SIDE)
            .build();

    SnowflakeTelemetryService mockTelemetry = mock(SnowflakeTelemetryService.class);
    StreamingErrorHandler mockErrorHandler = mock(StreamingErrorHandler.class);

    // --- Build a real SSPC with mock SDK ---

    PartitionOffsetTracker offsetTracker =
        new PartitionOffsetTracker(tp, sinkTaskContext, channelName);
    SnowflakeTelemetryChannelStatus telemetryStatus =
        new SnowflakeTelemetryChannelStatus(
            TOPIC,
            CONNECTOR_NAME,
            channelName,
            System.currentTimeMillis(),
            Optional.empty(),
            offsetTracker.persistedOffsetRef(),
            offsetTracker.processedOffsetRef(),
            offsetTracker.consumerGroupOffsetRef());

    SnowpipeStreamingPartitionChannel realChannel =
        new SnowpipeStreamingPartitionChannel(
            TOPIC,
            channelName,
            pipeName,
            mockClient,
            ioExecutor,
            mockTelemetry,
            telemetryStatus,
            offsetTracker,
            taskConfig,
            mockErrorHandler,
            TaskMetrics.noop(),
            false,
            null,
            Optional.empty());

    realChannel.awaitInitialization();

    // After initialization, offset should be committedOffset + 1
    assertEquals(
        committedOffset + 1,
        sinkTaskContext.offset(tp),
        "Initialization should set offset to committedOffset + 1");

    // --- Mock PartitionChannelManager to return the real channel ---

    PartitionChannelManager mockChannelManager = mock(PartitionChannelManager.class);
    when(mockChannelManager.getChannel(tp)).thenReturn(Optional.of(realChannel));
    when(mockChannelManager.getChannel(channelName)).thenReturn(Optional.of(realChannel));
    Map<String, TopicPartitionChannel> channelMap = new ConcurrentHashMap<>();
    channelMap.put(channelName, realChannel);
    when(mockChannelManager.getPartitionChannels()).thenReturn(channelMap);

    // --- Wire SSV2 ---

    SnowflakeConnectionService mockConn = mock(SnowflakeConnectionService.class);
    when(mockConn.isClosed()).thenReturn(false);

    SnowflakeSinkServiceV2 service =
        new SnowflakeSinkServiceV2(
            mockConn,
            taskConfig,
            sinkTaskContext,
            Optional.empty(),
            () -> mock(BatchOffsetFetcher.class),
            () -> mockChannelManager,
            TaskMetrics.noop());

    // --- Batch 0: verify init offset was applied ---
    // On master, initializeFromSnowflake calls sinkTaskContext.offset(tp, 51) directly.
    // On fix branch, it enqueues in pendingOffsetResets and this empty batch drains it.
    service.insert(Collections.emptyList());
    assertEquals(committedOffset + 1, sinkTaskContext.offset(tp), "Init should set offset");

    // --- Batch 1: records 51–85, appendRow fails at offset 80 ---
    //
    // Records 51–79 are appended successfully.
    // Record 80 triggers SFException → recovery → resetAfterRecovery(50) calls
    //   sinkTaskContext.offset(tp, 51) directly.
    // insertRecord returns false for record 80 → SSV2 puts {tp: 80} in offsetsOfFirstSkippedRecord.
    // Records 81–85 are skipped (tp already in offsetsOfFirstSkippedRecord).
    // End of batch: sinkTaskContext.offset(tp, 80) OVERWRITES the recovery offset.

    service.insert(buildRecordBatch(51, 85));

    // --- Batch 2: records 80–85 (what Kafka would re-deliver from offset 80) ---
    //
    // On master: needToSkipCurrentBatch is reset on first record (isFirstRowInBatch=true),
    //            all records process normally at offsets 80–85. No offset rewind occurs.
    //            Offset stays at 80. Records 51–79 are permanently lost.
    //
    // On fix branch: pendingOffsetResets contains {tp: 51} from recovery, which is drained
    //                into offsetsToRewindTo at the start of the batch. All records are
    //                skipped. sinkTaskContext.offset({tp: 51}) is called.

    service.insert(buildRecordBatch(80, 85));

    // --- Assertion ---

    long expectedRecoveryOffset = committedOffset + 1; // 51
    assertEquals(
        expectedRecoveryOffset,
        sinkTaskContext.offset(tp),
        "After recovery, the effective offset should be the recovery offset ("
            + expectedRecoveryOffset
            + "), not the failed record's offset ("
            + failingOffset
            + "). If this assertion fails, records "
            + expectedRecoveryOffset
            + "–"
            + (failingOffset - 1)
            + " are permanently lost because resetAfterRecovery's offset was overwritten by"
            + " offsetsOfFirstSkippedRecord.");
  }

  private List<SinkRecord> buildRecordBatch(long fromOffset, long toOffset) {
    List<SinkRecord> records = new ArrayList<>();
    JsonConverter jsonConverter = new JsonConverter();
    jsonConverter.configure(Collections.singletonMap("schemas.enable", "false"), false);
    for (long offset = fromOffset; offset <= toOffset; offset++) {
      SchemaAndValue schemaAndValue =
          jsonConverter.toConnectData(
              TOPIC, "{\"name\": \"test\"}".getBytes(StandardCharsets.UTF_8));
      records.add(
          SinkRecordBuilder.forTopicPartition(TOPIC, PARTITION)
              .withSchemaAndValue(schemaAndValue)
              .withOffset(offset)
              .build());
    }
    return records;
  }
}
