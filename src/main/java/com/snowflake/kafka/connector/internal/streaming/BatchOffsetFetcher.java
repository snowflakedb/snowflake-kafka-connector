package com.snowflake.kafka.connector.internal.streaming;

import static com.snowflake.kafka.connector.internal.streaming.channel.TopicPartitionChannel.NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE;

import com.snowflake.ingest.streaming.ChannelStatus;
import com.snowflake.ingest.streaming.ChannelStatusBatch;
import com.snowflake.ingest.streaming.SFException;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import com.snowflake.kafka.connector.internal.KCLogger;
import com.snowflake.kafka.connector.internal.streaming.channel.TopicPartitionChannel;
import com.snowflake.kafka.connector.internal.streaming.v2.client.StreamingClientPools;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.kafka.common.TopicPartition;

/**
 * Fetches committed offsets for topic partitions in batches, grouped by pipe. Makes at most one
 * network call per SDK client (i.e. per pipe), regardless of the number of partitions.
 */
public class BatchOffsetFetcher {

  private static final KCLogger LOGGER = new KCLogger(BatchOffsetFetcher.class.getName());

  private final String connectorName;
  private final String taskId;
  private final Map<String, String> connectorConfig;
  private final StreamingClientProperties streamingClientProperties;
  private final boolean tolerateErrors;

  public BatchOffsetFetcher(
      String connectorName,
      String taskId,
      Map<String, String> connectorConfig,
      boolean tolerateErrors) {
    this.connectorName = connectorName;
    this.taskId = taskId;
    this.connectorConfig = connectorConfig;
    this.streamingClientProperties = new StreamingClientProperties(connectorConfig);
    this.tolerateErrors = tolerateErrors;
  }

  /**
   * Fetches committed offsets for the given partitions using the SDK's batch channel-status API.
   * Makes at most one network call per pipe, regardless of partition count.
   *
   * @param partitions the partitions to query
   * @param channelLookup function to look up the TopicPartitionChannel for a given partition
   * @return map of TopicPartition to the offset safe to commit to Kafka (committed + 1), only
   *     containing entries where a valid offset was found
   */
  public Map<TopicPartition, Long> getCommittedOffsets(
      Collection<TopicPartition> partitions,
      Function<TopicPartition, Optional<TopicPartitionChannel>> channelLookup) {

    PartitionsByTopic grouped = PartitionsByTopic.groupByTopic(partitions, channelLookup);

    grouped.topicToPartitionsWithoutChannels.forEach(
        (topic, uninitializedPartitions) ->
            LOGGER.warn(
                "Topic: {} has partition(s) not yet initialized to get offset: {}",
                topic,
                uninitializedPartitions));

    Map<TopicPartition, Long> result = new HashMap<>();

    grouped.pipeNameToChannels.forEach(
        (pipeName, channelsByPartition) -> {
          try {
            result.putAll(getCommittedOffsetsForPipe(pipeName, channelsByPartition));
          } catch (SFException e) {
            LOGGER.error(
                "Failed to fetch committed offsets for pipe: {}, skipping {} channel(s)",
                pipeName,
                channelsByPartition.size(),
                e);
          }
        });
    return result;
  }

  private Map<TopicPartition, Long> getCommittedOffsetsForPipe(
      String pipeName, Map<TopicPartition, TopicPartitionChannel> channelsByPartition) {
    List<String> channelNames =
        channelsByPartition.values().stream()
            .map(TopicPartitionChannel::getChannelNameFormatV1)
            .collect(Collectors.toList());

    SnowflakeStreamingIngestClient client =
        StreamingClientPools.getClient(
            connectorName, taskId, pipeName, connectorConfig, streamingClientProperties);
    ChannelStatusBatch batch = client.getChannelStatus(channelNames);

    Map<TopicPartition, Long> result = new HashMap<>();
    channelsByPartition.forEach(
        (topicPartition, channel) -> {
          String channelName = channel.getChannelNameFormatV1();

          ChannelStatus status = batch.getChannelStatusBatch().get(channelName);
          if (status == null) {
            LOGGER.warn("No status returned for channel: {}", channelName);
            return;
          }
          long offset = channel.processChannelStatus(status, tolerateErrors);
          LOGGER.info(
              "Fetched snowflake committed offset: [{}] for channel [{}]", offset, channelName);
          if (offset != NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE) {
            result.put(topicPartition, offset);
          }
        });
    return result;
  }

  static class PartitionsByTopic {
    /** Partitions with initialized channels, grouped by pipe name */
    final Map<String, Map<TopicPartition, TopicPartitionChannel>> pipeNameToChannels;

    /** Partitions without an initialized channel, grouped by topic */
    final Map<String, Set<TopicPartition>> topicToPartitionsWithoutChannels;

    PartitionsByTopic(
        Map<String, Map<TopicPartition, TopicPartitionChannel>> pipeNameToChannels,
        Map<String, Set<TopicPartition>> topicToPartitionsWithoutChannels) {
      this.pipeNameToChannels = pipeNameToChannels;
      this.topicToPartitionsWithoutChannels = topicToPartitionsWithoutChannels;
    }

    static PartitionsByTopic groupByTopic(
        Collection<TopicPartition> partitions,
        Function<TopicPartition, Optional<TopicPartitionChannel>> channelLookup) {
      Map<String, Map<TopicPartition, TopicPartitionChannel>> pipeNameToChannels = new HashMap<>();
      Map<String, Set<TopicPartition>> topicToPartitionsWithoutChannels = new HashMap<>();
      for (TopicPartition topicPartition : partitions) {
        channelLookup
            .apply(topicPartition)
            .ifPresentOrElse(
                channel ->
                    pipeNameToChannels
                        .computeIfAbsent(channel.getPipeName(), k -> new LinkedHashMap<>())
                        .put(topicPartition, channel),
                () ->
                    topicToPartitionsWithoutChannels
                        .computeIfAbsent(topicPartition.topic(), k -> new HashSet<>())
                        .add(topicPartition));
      }
      return new PartitionsByTopic(pipeNameToChannels, topicToPartitionsWithoutChannels);
    }
  }
}
