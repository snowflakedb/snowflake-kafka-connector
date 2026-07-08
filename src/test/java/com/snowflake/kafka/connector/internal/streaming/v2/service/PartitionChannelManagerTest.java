package com.snowflake.kafka.connector.internal.streaming.v2.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.snowflake.kafka.connector.StaticTopicToTableResolver;
import com.snowflake.kafka.connector.config.SinkTaskConfig;
import com.snowflake.kafka.connector.config.SinkTaskConfigTestBuilder;
import com.snowflake.kafka.connector.internal.streaming.channel.TopicPartitionChannel;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class PartitionChannelManagerTest {

  private static final String CONNECTOR_NAME = "test_connector";
  private static final String TASK_ID = "0";
  private static final String TOPIC = "test_topic";

  private PartitionChannelManager manager;
  private Map<TopicPartition, TopicPartitionChannel> createdChannels;

  @BeforeEach
  void setUp() {
    createdChannels = new HashMap<>();

    PartitionChannelManager.PartitionChannelBuilder trackingBuilder =
        (topicPartition, tableName, channelName, pipeName) -> {
          TopicPartitionChannel channel = mock(TopicPartitionChannel.class);
          when(channel.getChannelName()).thenReturn(channelName);
          when(channel.getPipeName()).thenReturn(pipeName);
          when(channel.closeChannelAsync()).thenReturn(CompletableFuture.completedFuture(null));
          when(channel.waitForLastProcessedRecordCommitted())
              .thenReturn(CompletableFuture.completedFuture(null));
          createdChannels.put(topicPartition, channel);
          return channel;
        };

    manager = new PartitionChannelManager(testConfig(Collections.emptyMap()), trackingBuilder);
  }

  // --- makeChannelName ---

  @Test
  void makeChannelNameConcatenatesWithUnderscores() {
    assertEquals(
        "myConnector_myTopic_3",
        PartitionChannelManager.makeChannelName("myConnector", "myTopic", 3));
  }

  // --- startPartitions ---

  @Test
  void startPartitionsRegistersChannelsInMap() {
    TopicPartition tp0 = new TopicPartition(TOPIC, 0);
    TopicPartition tp1 = new TopicPartition(TOPIC, 1);
    Map<String, String> tableToPipe = new HashMap<>();
    tableToPipe.put(TOPIC, "pipe_" + TOPIC);

    manager.startPartitions(Arrays.asList(tp0, tp1), tableToPipe);

    assertEquals(2, manager.getPartitionChannels().size());
    assertTrue(manager.getChannel(tp0).isPresent());
    assertTrue(manager.getChannel(tp1).isPresent());
  }

  @Test
  void startPartitionsPassesCorrectNamesToBuilder() {
    Map<String, String> capturedArgs = new HashMap<>();
    PartitionChannelManager.PartitionChannelBuilder capturingBuilder =
        (topicPartition, tableName, channelName, pipeName) -> {
          capturedArgs.put("tableName", tableName);
          capturedArgs.put("channelName", channelName);
          capturedArgs.put("pipeName", pipeName);
          TopicPartitionChannel channel = mock(TopicPartitionChannel.class);
          when(channel.getChannelName()).thenReturn(channelName);
          return channel;
        };

    PartitionChannelManager capturingManager =
        new PartitionChannelManager(testConfig(Collections.emptyMap()), capturingBuilder);

    TopicPartition tp = new TopicPartition(TOPIC, 7);
    Map<String, String> tableToPipe = new HashMap<>();
    tableToPipe.put(TOPIC, "pipe_" + TOPIC);

    capturingManager.startPartitions(Collections.singletonList(tp), tableToPipe);

    String expectedChannelName = PartitionChannelManager.makeChannelName(CONNECTOR_NAME, TOPIC, 7);
    assertEquals(TOPIC, capturedArgs.get("tableName"));
    assertEquals(expectedChannelName, capturedArgs.get("channelName"));
    assertEquals("pipe_" + TOPIC, capturedArgs.get("pipeName"));
  }

  @Test
  void startPartitionsUsesTopicToTableMapForTableName() {
    Map<String, String> topicToTable = new HashMap<>();
    topicToTable.put("raw_topic", "mapped_table");

    Map<String, String> capturedArgs = new HashMap<>();
    PartitionChannelManager.PartitionChannelBuilder capturingBuilder =
        (topicPartition, tableName, channelName, pipeName) -> {
          capturedArgs.put("tableName", tableName);
          capturedArgs.put("pipeName", pipeName);
          TopicPartitionChannel channel = mock(TopicPartitionChannel.class);
          when(channel.getChannelName()).thenReturn(channelName);
          return channel;
        };

    PartitionChannelManager managerWithMapping =
        new PartitionChannelManager(testConfig(topicToTable), capturingBuilder);

    TopicPartition tp = new TopicPartition("raw_topic", 0);
    Map<String, String> tableToPipe = new HashMap<>();
    tableToPipe.put("mapped_table", "pipe_mapped_table");

    managerWithMapping.startPartitions(Collections.singletonList(tp), tableToPipe);

    assertEquals("mapped_table", capturedArgs.get("tableName"));
    assertEquals("pipe_mapped_table", capturedArgs.get("pipeName"));
  }

  // --- getChannel ---

  @Test
  void getChannelByTopicPartitionReturnsChannel() {
    TopicPartition tp = new TopicPartition(TOPIC, 0);
    startSinglePartition(tp);

    Optional<TopicPartitionChannel> result = manager.getChannel(tp);

    assertTrue(result.isPresent());
    assertSame(createdChannels.get(tp), result.get());
  }

  @Test
  void getChannelByStringReturnsChannel() {
    TopicPartition tp = new TopicPartition(TOPIC, 0);
    startSinglePartition(tp);

    String channelName = PartitionChannelManager.makeChannelName(CONNECTOR_NAME, TOPIC, 0);
    Optional<TopicPartitionChannel> result = manager.getChannel(channelName);

    assertTrue(result.isPresent());
    assertSame(createdChannels.get(tp), result.get());
  }

  @Test
  void getChannelReturnsEmptyForUnknownPartition() {
    TopicPartition unknown = new TopicPartition("no_such_topic", 99);
    assertFalse(manager.getChannel(unknown).isPresent());
  }

  @Test
  void getChannelByStringReturnsEmptyForUnknownName() {
    assertFalse(manager.getChannel("nonexistent_channel").isPresent());
  }

  // --- close (subset) ---

  @Test
  void closeRemovesOnlyRequestedPartitions() {
    TopicPartition tp0 = new TopicPartition(TOPIC, 0);
    TopicPartition tp1 = new TopicPartition(TOPIC, 1);
    TopicPartition tp2 = new TopicPartition(TOPIC, 2);
    startPartitions(tp0, tp1, tp2);

    manager.close(Collections.singletonList(tp1));

    assertFalse(manager.getChannel(tp1).isPresent());
    assertTrue(manager.getChannel(tp0).isPresent());
    assertTrue(manager.getChannel(tp2).isPresent());
    assertEquals(2, manager.getPartitionChannels().size());
  }

  @Test
  void closeCallsCloseChannelAsyncOnRequestedPartitions() {
    TopicPartition tp0 = new TopicPartition(TOPIC, 0);
    TopicPartition tp1 = new TopicPartition(TOPIC, 1);
    startPartitions(tp0, tp1);

    manager.close(Collections.singletonList(tp0));

    verify(createdChannels.get(tp0)).closeChannelAsync();
    verify(createdChannels.get(tp1), never()).closeChannelAsync();
  }

  @Test
  void closeHandlesUnknownPartitionsGracefully() {
    TopicPartition tp0 = new TopicPartition(TOPIC, 0);
    startSinglePartition(tp0);

    TopicPartition unknown = new TopicPartition("unknown", 99);
    manager.close(Arrays.asList(tp0, unknown));

    assertFalse(manager.getChannel(tp0).isPresent());
    assertEquals(0, manager.getPartitionChannels().size());
  }

  @Test
  void closeWithEmptyCollectionIsNoop() {
    TopicPartition tp0 = new TopicPartition(TOPIC, 0);
    startSinglePartition(tp0);

    manager.close(Collections.emptyList());

    assertTrue(manager.getChannel(tp0).isPresent());
    assertEquals(1, manager.getPartitionChannels().size());
  }

  // --- closeAll ---

  @Test
  void closeAllClosesAllChannelsAndClearsMap() {
    TopicPartition tp0 = new TopicPartition(TOPIC, 0);
    TopicPartition tp1 = new TopicPartition(TOPIC, 1);
    startPartitions(tp0, tp1);

    manager.closeAll();

    assertTrue(manager.getPartitionChannels().isEmpty());
    verify(createdChannels.get(tp0)).closeChannelAsync();
    verify(createdChannels.get(tp1)).closeChannelAsync();
  }

  @Test
  void closeAllOnEmptyManagerIsNoop() {
    manager.closeAll();
    assertTrue(manager.getPartitionChannels().isEmpty());
  }

  // --- waitForAllChannelsToCommitData ---

  @Test
  void waitForAllChannelsCallsFlushOnEveryChannel() {
    TopicPartition tp0 = new TopicPartition(TOPIC, 0);
    TopicPartition tp1 = new TopicPartition(TOPIC, 1);
    startPartitions(tp0, tp1);

    manager.waitForAllChannelsToCommitData();

    verify(createdChannels.get(tp0)).waitForLastProcessedRecordCommitted();
    verify(createdChannels.get(tp1)).waitForLastProcessedRecordCommitted();
  }

  @Test
  void waitForAllChannelsOnEmptyManagerIsNoop() {
    manager.waitForAllChannelsToCommitData();
    assertTrue(manager.getPartitionChannels().isEmpty());
  }

  // --- drainPendingOffsetResets ---

  @Test
  void drainPendingOffsetResetsReturnsEmptyMapWhenNothingPending() {
    assertTrue(manager.drainPendingOffsetResets(Set.of()).isEmpty());
  }

  @Test
  void drainPendingOffsetResetsReturnsAndClearsPendingEntries() {
    TopicPartition tp0 = new TopicPartition(TOPIC, 0);
    TopicPartition tp1 = new TopicPartition(TOPIC, 1);

    manager.submitPendingOffsetReset(tp0, 51L);
    manager.submitPendingOffsetReset(tp1, 101L);

    Map<TopicPartition, Long> drained = manager.drainPendingOffsetResets(Set.of(tp0, tp1));

    assertEquals(Map.of(tp0, 51L, tp1, 101L), drained);
    assertTrue(
        manager.drainPendingOffsetResets(Set.of(tp0, tp1)).isEmpty(),
        "Second drain should be empty");
  }

  @Test
  void drainPendingOffsetResetsLastWriteWinsForSamePartition() {
    TopicPartition tp = new TopicPartition(TOPIC, 0);

    manager.submitPendingOffsetReset(tp, 51L);
    manager.submitPendingOffsetReset(tp, 71L);

    Map<TopicPartition, Long> drained = manager.drainPendingOffsetResets(Set.of(tp));

    assertEquals(71L, drained.get(tp), "Later recovery offset should win");
    assertEquals(1, drained.size());
  }

  @Test
  void drainPendingOffsetResetsDropsResetsForUnassignedPartitions() {
    TopicPartition assigned = new TopicPartition(TOPIC, 0);
    TopicPartition revoked = new TopicPartition(TOPIC, 1);

    manager.submitPendingOffsetReset(assigned, 51L);
    manager.submitPendingOffsetReset(revoked, 101L);

    // The revoked partition is no longer in the task's assignment (a rebalance revoked it after the
    // reset was enqueued). Its stale reset must be dropped, not returned -- rewinding it would seek
    // a partition the consumer no longer owns -> IllegalStateException: No current assignment for
    // partition (SNOW-3647384).
    Map<TopicPartition, Long> drained = manager.drainPendingOffsetResets(Set.of(assigned));

    assertEquals(Map.of(assigned, 51L), drained);
    assertTrue(
        manager.drainPendingOffsetResets(Set.of(assigned, revoked)).isEmpty(),
        "Dropped reset must be removed from the pending map, not left for a later drain");
  }

  @Test
  void closeClearsPendingOffsetResetsForClosedPartitions() {
    TopicPartition tp0 = new TopicPartition(TOPIC, 0);
    TopicPartition tp1 = new TopicPartition(TOPIC, 1);
    startPartitions(tp0, tp1);

    manager.submitPendingOffsetReset(tp0, 51L);
    manager.submitPendingOffsetReset(tp1, 101L);

    manager.close(Collections.singletonList(tp0));

    Map<TopicPartition, Long> drained = manager.drainPendingOffsetResets(Set.of(tp0, tp1));
    assertFalse(drained.containsKey(tp0), "Closed partition should not appear in drain");
    assertEquals(101L, drained.get(tp1));
  }

  // --- helpers ---

  private void startSinglePartition(TopicPartition topicPartition) {
    startPartitions(topicPartition);
  }

  private void startPartitions(TopicPartition... partitions) {
    Map<String, String> tableToPipe = new HashMap<>();
    for (TopicPartition topicPartition : partitions) {
      String tableName = topicPartition.topic();
      tableToPipe.putIfAbsent(tableName, "pipe_" + tableName);
    }
    manager.startPartitions(Arrays.asList(partitions), tableToPipe);
  }

  private static SinkTaskConfig testConfig(Map<String, String> topicToTableMap) {
    return SinkTaskConfigTestBuilder.builder()
        .connectorName(CONNECTOR_NAME)
        .taskId(TASK_ID)
        .topicToTableResolver(new StaticTopicToTableResolver(topicToTableMap))
        .enableSanitization(false)
        .build();
  }
}
