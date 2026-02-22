package com.snowflake.kafka.connector.internal.streaming;

import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.NAME;
import static com.snowflake.kafka.connector.internal.streaming.channel.TopicPartitionChannel.NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.snowflake.ingest.streaming.ChannelStatus;
import com.snowflake.ingest.streaming.ChannelStatusBatch;
import com.snowflake.ingest.streaming.SFException;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.internal.SnowflakeKafkaConnectorException;
import com.snowflake.kafka.connector.internal.streaming.channel.TopicPartitionChannel;
import com.snowflake.kafka.connector.internal.streaming.v2.client.StreamingClientFactory;
import com.snowflake.kafka.connector.internal.streaming.v2.client.StreamingClientSupplier;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link BatchOffsetFetcher}. */
class BatchOffsetFetcherTest {

  private static final String TASK_ID = "0";

  private String connectorName;
  private BatchOffsetFetcher fetcher;
  private Map<TopicPartition, TopicPartitionChannel> channels;
  private CountingClientSupplier clientSupplier;

  @BeforeEach
  void setUp() {
    // Unique name per test to avoid StreamingClientPools caching across tests
    connectorName = "test_connector_" + UUID.randomUUID().toString().substring(0, 8);

    Map<String, String> config = new HashMap<>();
    config.put(NAME, connectorName);
    config.put(Utils.TASK_ID, TASK_ID);

    fetcher = new BatchOffsetFetcher(connectorName, TASK_ID, config, false);
    channels = new HashMap<>();

    clientSupplier = new CountingClientSupplier();
    StreamingClientFactory.setStreamingClientSupplier(clientSupplier);
  }

  @AfterEach
  void tearDown() {
    StreamingClientFactory.resetStreamingClientSupplier();
  }

  @Test
  void emptyPartitionsReturnsEmptyMap() {
    assertTrue(fetcher.getCommittedOffsets(Collections.emptySet(), channelLookup()).isEmpty());
  }

  @Test
  void groupsByPipeAndBatchesCalls() {
    TopicPartition tp0 = new TopicPartition("topicA", 0);
    TopicPartition tp1 = new TopicPartition("topicA", 1);
    TopicPartition tp2 = new TopicPartition("topicB", 0);
    TopicPartition tp3 = new TopicPartition("topicB", 1);

    registerChannel(tp0, "pipeA", "chA0", 10L);
    registerChannel(tp1, "pipeA", "chA1", 20L);
    registerChannel(tp2, "pipeB", "chB0", 30L);
    registerChannelWithNoOffset(tp3, "pipeB", "chB1");

    Map<TopicPartition, Long> result =
        fetcher.getCommittedOffsets(Set.of(tp0, tp1, tp2, tp3), channelLookup());

    assertEquals(3, result.size());
    assertEquals(11L, result.get(tp0));
    assertEquals(21L, result.get(tp1));
    assertEquals(31L, result.get(tp2));
    assertTrue(!result.containsKey(tp3), "Channel with no offset should be excluded");

    // Two pipes, so exactly 2 batch calls
    assertEquals(2, clientSupplier.getBatchCallCount());
  }

  @Test
  void uninitializedPartitionsAreSkipped() {
    TopicPartition initialized = new TopicPartition("topicA", 0);
    TopicPartition uninitialized = new TopicPartition("topicA", 1);

    registerChannel(initialized, "pipeA", "ch0", 5L);

    Map<TopicPartition, Long> result =
        fetcher.getCommittedOffsets(Set.of(initialized, uninitialized), channelLookup());

    assertEquals(1, result.size());
    assertEquals(6L, result.get(initialized));
  }

  @Test
  void sfExceptionForOnePipeDoesNotAffectOthers() {
    TopicPartition tp0 = new TopicPartition("topicA", 0);
    TopicPartition tp1 = new TopicPartition("topicB", 0);

    registerChannel(tp0, "pipeA", "ch0", 10L);
    registerChannel(tp1, "pipeB", "ch1", 20L);

    clientSupplier.setFailingPipe("pipeA");

    Map<TopicPartition, Long> result =
        fetcher.getCommittedOffsets(Set.of(tp0, tp1), channelLookup());

    assertEquals(1, result.size());
    assertEquals(21L, result.get(tp1));
  }

  @Test
  void connectorExceptionPropagates() {
    TopicPartition tp0 = new TopicPartition("topicA", 0);

    TopicPartitionChannel mockChannel = mock(TopicPartitionChannel.class);
    when(mockChannel.getChannelNameFormatV1()).thenReturn("ch0");
    when(mockChannel.getPipeName()).thenReturn("pipeA");
    when(mockChannel.processChannelStatus(any(ChannelStatus.class), anyBoolean()))
        .thenThrow(new SnowflakeKafkaConnectorException("ingestion error", "5030"));

    channels.put(tp0, mockChannel);
    clientSupplier.setChannelOffset("ch0", "pipeA", "10");

    assertThrows(
        SnowflakeKafkaConnectorException.class,
        () -> fetcher.getCommittedOffsets(Set.of(tp0), channelLookup()));
  }

  @Test
  void partitionsByTopicGroupsCorrectly() {
    TopicPartition tpA0 = new TopicPartition("topicA", 0);
    TopicPartition tpA1 = new TopicPartition("topicA", 1);
    TopicPartition tpB0 = new TopicPartition("topicB", 0);
    TopicPartition tpB1 = new TopicPartition("topicB", 1);

    TopicPartitionChannel chA0 = mockChannel("pipeA");
    TopicPartitionChannel chB0 = mockChannel("pipeB");

    // tpA0 and tpB0 have channels; tpA1 and tpB1 do not
    Map<TopicPartition, TopicPartitionChannel> lookup = Map.of(tpA0, chA0, tpB0, chB0);

    BatchOffsetFetcher.PartitionsByTopic result =
        BatchOffsetFetcher.PartitionsByTopic.groupByTopic(
            Set.of(tpA0, tpA1, tpB0, tpB1), tp -> Optional.ofNullable(lookup.get(tp)));

    // Initialized channels grouped by pipe
    assertEquals(2, result.pipeNameToChannels.size());
    assertEquals(Map.of(tpA0, chA0), result.pipeNameToChannels.get("pipeA"));
    assertEquals(Map.of(tpB0, chB0), result.pipeNameToChannels.get("pipeB"));

    // Uninitialized partitions grouped by topic
    assertEquals(2, result.topicToPartitionsWithoutChannels.size());
    assertEquals(Set.of(tpA1), result.topicToPartitionsWithoutChannels.get("topicA"));
    assertEquals(Set.of(tpB1), result.topicToPartitionsWithoutChannels.get("topicB"));
  }

  private static TopicPartitionChannel mockChannel(String pipeName) {
    TopicPartitionChannel ch = mock(TopicPartitionChannel.class);
    when(ch.getPipeName()).thenReturn(pipeName);
    return ch;
  }

  // -- helpers --

  private Function<TopicPartition, Optional<TopicPartitionChannel>> channelLookup() {
    return tp -> Optional.ofNullable(channels.get(tp));
  }

  private void registerChannel(
      TopicPartition topicPartition, String pipeName, String channelName, long committedOffset) {
    TopicPartitionChannel mockChannel = mock(TopicPartitionChannel.class);
    when(mockChannel.getChannelNameFormatV1()).thenReturn(channelName);
    when(mockChannel.getPipeName()).thenReturn(pipeName);
    when(mockChannel.processChannelStatus(any(ChannelStatus.class), anyBoolean()))
        .thenReturn(committedOffset + 1);

    channels.put(topicPartition, mockChannel);
    clientSupplier.setChannelOffset(channelName, pipeName, String.valueOf(committedOffset));
  }

  private void registerChannelWithNoOffset(
      TopicPartition topicPartition, String pipeName, String channelName) {
    TopicPartitionChannel mockChannel = mock(TopicPartitionChannel.class);
    when(mockChannel.getChannelNameFormatV1()).thenReturn(channelName);
    when(mockChannel.getPipeName()).thenReturn(pipeName);
    when(mockChannel.processChannelStatus(any(ChannelStatus.class), anyBoolean()))
        .thenReturn(NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE);

    channels.put(topicPartition, mockChannel);
  }

  /**
   * A StreamingClientSupplier that creates mock clients with configurable channel statuses and
   * tracks batch call counts.
   */
  static class CountingClientSupplier implements StreamingClientSupplier {
    private final AtomicInteger batchCallCount = new AtomicInteger(0);
    // pipeName -> (channelName -> offsetToken)
    private final Map<String, Map<String, String>> pipeChannelOffsets = new ConcurrentHashMap<>();
    private volatile String failingPipe = null;

    void setChannelOffset(String channelName, String pipeName, String offsetToken) {
      pipeChannelOffsets
          .computeIfAbsent(pipeName, k -> new ConcurrentHashMap<>())
          .put(channelName, offsetToken);
    }

    void setFailingPipe(String pipeName) {
      this.failingPipe = pipeName;
    }

    int getBatchCallCount() {
      return batchCallCount.get();
    }

    @Override
    public SnowflakeStreamingIngestClient get(
        String clientName,
        String dbName,
        String schemaName,
        String pipeName,
        Map<String, String> connectorConfig,
        StreamingClientProperties streamingClientProperties) {
      SnowflakeStreamingIngestClient client = mock(SnowflakeStreamingIngestClient.class);
      when(client.getChannelStatus(any()))
          .thenAnswer(
              invocation -> {
                batchCallCount.incrementAndGet();
                if (pipeName.equals(failingPipe)) {
                  throw new SFException(
                      "TestError", "Simulated batch failure", 500, "Internal Server Error");
                }
                List<String> names = invocation.getArgument(0);
                Map<String, ChannelStatus> statusMap = new HashMap<>();
                Map<String, String> offsets =
                    pipeChannelOffsets.getOrDefault(pipeName, Collections.emptyMap());
                for (String name : names) {
                  statusMap.put(
                      name,
                      new ChannelStatus(
                          "db",
                          "schema",
                          pipeName,
                          name,
                          "SUCCESS",
                          offsets.get(name),
                          Instant.now(),
                          0,
                          0,
                          0,
                          null,
                          null,
                          null,
                          null,
                          Instant.now()));
                }
                return new ChannelStatusBatch(statusMap);
              });
      return client;
    }
  }
}
