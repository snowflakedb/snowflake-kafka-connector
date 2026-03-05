package com.snowflake.kafka.connector.internal.streaming;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.snowflake.kafka.connector.ConnectorConfigTools;
import com.snowflake.kafka.connector.builder.SinkRecordBuilder;
import com.snowflake.kafka.connector.internal.streaming.channel.TopicPartitionChannel;
import com.snowflake.kafka.connector.internal.streaming.v2.service.BatchOffsetFetcher;
import com.snowflake.kafka.connector.internal.streaming.v2.service.PartitionChannelManager;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class SnowflakeSinkServiceV2Test {

  private static final String TOPIC = "test_topic";

  private PartitionChannelManager mockChannelManager;
  private BatchOffsetFetcher mockBatchOffsetFetcher;
  private SinkTaskContext mockSinkTaskContext;
  private SnowflakeSinkServiceV2 service;

  @BeforeEach
  void setUp() {
    mockChannelManager = mock(PartitionChannelManager.class);
    mockBatchOffsetFetcher = mock(BatchOffsetFetcher.class);
    mockSinkTaskContext = mock(SinkTaskContext.class);

    service =
        new SnowflakeSinkServiceV2(
            mockChannelManager,
            mockBatchOffsetFetcher,
            mockSinkTaskContext,
            ConnectorConfigTools.BehaviorOnNullValues.DEFAULT,
            false);
  }

  // --- insert() skip logic ---

  @Test
  void insertSkipsRecordsForInitializingPartitions() {
    TopicPartition tp = new TopicPartition(TOPIC, 0);
    TopicPartitionChannel channel = mockChannel("ch_0", true);
    when(mockChannelManager.getChannel(tp)).thenReturn(Optional.of(channel));

    SinkRecord record = recordFor(TOPIC, 0, 10);
    service.insert(Collections.singletonList(record));

    verify(channel, never()).insertRecord(any(), anyBoolean());
    verify(mockSinkTaskContext).offset(tp, 10);
  }

  @Test
  void insertProcessesRecordsForReadyPartitions() {
    TopicPartition tp = new TopicPartition(TOPIC, 0);
    TopicPartitionChannel channel = mockChannel("ch_0", false);
    when(mockChannelManager.getChannel(tp)).thenReturn(Optional.of(channel));

    SinkRecord record = recordFor(TOPIC, 0, 5);
    service.insert(Collections.singletonList(record));

    verify(channel).insertRecord(record, true);
    verify(mockSinkTaskContext, never()).offset(any(TopicPartition.class), any(Long.class));
  }

  @Test
  void insertHandlesMixOfInitializingAndReadyPartitions() {
    TopicPartition tpInit = new TopicPartition(TOPIC, 0);
    TopicPartition tpReady = new TopicPartition(TOPIC, 1);

    TopicPartitionChannel initChannel = mockChannel("ch_0", true);
    TopicPartitionChannel readyChannel = mockChannel("ch_1", false);

    when(mockChannelManager.getChannel(tpInit)).thenReturn(Optional.of(initChannel));
    when(mockChannelManager.getChannel(tpReady)).thenReturn(Optional.of(readyChannel));

    List<SinkRecord> records = Arrays.asList(recordFor(TOPIC, 0, 100), recordFor(TOPIC, 1, 200));

    service.insert(records);

    verify(initChannel, never()).insertRecord(any(), anyBoolean());
    verify(readyChannel).insertRecord(records.get(1), true);
    verify(mockSinkTaskContext).offset(tpInit, 100);
    verify(mockSinkTaskContext, never()).offset(tpReady, 200);
  }

  @Test
  void insertResetsToFirstSkippedOffset() {
    TopicPartition tp = new TopicPartition(TOPIC, 0);
    TopicPartitionChannel channel = mockChannel("ch_0", true);
    when(mockChannelManager.getChannel(tp)).thenReturn(Optional.of(channel));

    List<SinkRecord> records =
        Arrays.asList(recordFor(TOPIC, 0, 5), recordFor(TOPIC, 0, 6), recordFor(TOPIC, 0, 7));

    service.insert(records);

    verify(mockSinkTaskContext).offset(tp, 5);
    verify(mockSinkTaskContext, never()).offset(tp, 6);
    verify(mockSinkTaskContext, never()).offset(tp, 7);
  }

  // --- getCommittedOffsets() skip logic ---

  @Test
  @SuppressWarnings("unchecked")
  void getCommittedOffsetsExcludesInitializingPartitions() {
    TopicPartition tpInit = new TopicPartition(TOPIC, 0);
    TopicPartition tpReady = new TopicPartition(TOPIC, 1);

    TopicPartitionChannel initChannel = mockChannel("ch_0", true);
    TopicPartitionChannel readyChannel = mockChannel("ch_1", false);

    when(mockChannelManager.getChannel(tpInit)).thenReturn(Optional.of(initChannel));
    when(mockChannelManager.getChannel(tpReady)).thenReturn(Optional.of(readyChannel));

    Map<TopicPartition, Long> expectedOffsets = new HashMap<>();
    expectedOffsets.put(tpReady, 42L);
    when(mockBatchOffsetFetcher.getCommittedOffsets(any(), any(Function.class)))
        .thenReturn(expectedOffsets);

    Set<TopicPartition> allPartitions = new HashSet<>(Arrays.asList(tpInit, tpReady));
    Map<TopicPartition, Long> result = service.getCommittedOffsets(allPartitions);

    assertEquals(expectedOffsets, result);

    ArgumentCaptor<Set<TopicPartition>> captor = ArgumentCaptor.forClass(Set.class);
    verify(mockBatchOffsetFetcher).getCommittedOffsets(captor.capture(), any(Function.class));
    Set<TopicPartition> passedPartitions = captor.getValue();
    assertEquals(1, passedPartitions.size());
    assertTrue(passedPartitions.contains(tpReady));
  }

  @Test
  @SuppressWarnings("unchecked")
  void getCommittedOffsetsReturnsEmptyWhenAllInitializing() {
    TopicPartition tp0 = new TopicPartition(TOPIC, 0);
    TopicPartition tp1 = new TopicPartition(TOPIC, 1);

    TopicPartitionChannel ch0 = mockChannel("ch_0", true);
    TopicPartitionChannel ch1 = mockChannel("ch_1", true);
    when(mockChannelManager.getChannel(tp0)).thenReturn(Optional.of(ch0));
    when(mockChannelManager.getChannel(tp1)).thenReturn(Optional.of(ch1));

    when(mockBatchOffsetFetcher.getCommittedOffsets(any(), any(Function.class)))
        .thenReturn(Collections.emptyMap());

    Set<TopicPartition> allPartitions = new HashSet<>(Arrays.asList(tp0, tp1));
    Map<TopicPartition, Long> result = service.getCommittedOffsets(allPartitions);

    assertTrue(result.isEmpty());

    ArgumentCaptor<Set<TopicPartition>> captor = ArgumentCaptor.forClass(Set.class);
    verify(mockBatchOffsetFetcher).getCommittedOffsets(captor.capture(), any(Function.class));
    assertTrue(captor.getValue().isEmpty());
  }

  // --- transition from initializing to ready ---

  @Test
  void insertProcessesRecordsAfterChannelTransitionsFromInitializingToReady() {
    TopicPartition tp = new TopicPartition(TOPIC, 0);
    TopicPartitionChannel channel = mockChannel("ch_0", true);
    when(mockChannelManager.getChannel(tp)).thenReturn(Optional.of(channel));

    SinkRecord record1 = recordFor(TOPIC, 0, 10);
    service.insert(Collections.singletonList(record1));

    verify(channel, never()).insertRecord(any(), anyBoolean());
    verify(mockSinkTaskContext).offset(tp, 10);

    // Channel finishes initializing
    when(channel.isInitializing()).thenReturn(false);

    SinkRecord record2 = recordFor(TOPIC, 0, 10);
    service.insert(List.of(record1, record2));

    verify(channel).insertRecord(record2, true);
  }

  // --- helpers ---

  private static TopicPartitionChannel mockChannel(String channelName, boolean initializing) {
    TopicPartitionChannel channel = mock(TopicPartitionChannel.class);
    when(channel.getChannelName()).thenReturn(channelName);
    when(channel.isInitializing()).thenReturn(initializing);
    when(channel.isChannelClosed()).thenReturn(false);
    return channel;
  }

  private static SinkRecord recordFor(String topic, int partition, long offset) {
    return SinkRecordBuilder.forTopicPartition(topic, partition).withOffset(offset).build();
  }
}
