package com.snowflake.kafka.connector.internal.streaming.channel;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import net.snowflake.ingest.utils.SFException;
import org.apache.kafka.connect.sink.SinkRecord;

public interface TopicPartitionChannel extends ExposingInternalsTopicPartitionChannel {
  long NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE = -1L;

  /**
   * This is the new channel Name format that was created. New channel name prefixes connector name
   * in old format. Please note, we will not open channel with new format. We will run a migration
   * function from this new channel format to old channel format and drop new channel format.
   *
   * @param channelNameFormatV1 Original format used.
   * @param connectorName connector name used in SF config JSON.
   * @return new channel name introduced as part of @see <a
   *     href="https://github.com/snowflakedb/snowflake-kafka-connector/commit/3bf9106b22510c62068f7d2f7137b9e57989274c">
   *     this change (released in version 2.1.0) </a>
   */
  static String generateChannelNameFormatV2(String channelNameFormatV1, String connectorName) {
    return connectorName + "_" + channelNameFormatV1;
  }

  /**
   * Inserts the record into buffer
   *
   * <p>Step 1: Initializes this channel by fetching the offsetToken from Snowflake for the first
   * time this channel/partition has received offset after start/restart.
   *
   * <p>Step 2: Decides whether given offset from Kafka needs to be processed and whether it
   * qualifies for being added into buffer.
   *
   * @param kafkaSinkRecord input record from Kafka
   * @param isFirstRowPerPartitionInBatch indicates whether the given record is the first record per
   *     partition in a batch
   */
  void insertRecord(SinkRecord kafkaSinkRecord, boolean isFirstRowPerPartitionInBatch);

  /**
   * Get committed offset from Snowflake. It does an HTTP call internally to find out what was the
   * last offset inserted.
   *
   * <p>If committedOffset fetched from Snowflake is null, we would return -1(default value of
   * committedOffset) back to original call. (-1) would return an empty Map of partition and offset
   * back to kafka.
   *
   * <p>Else, we will convert this offset and return the offset which is safe to commit inside Kafka
   * (+1 of this returned value).
   *
   * <p>Check {@link com.snowflake.kafka.connector.SnowflakeSinkTask#preCommit(Map)}
   *
   * <p>Note:
   *
   * <p>If we cannot fetch offsetToken from snowflake even after retries and reopening the channel,
   * we will throw app
   *
   * @return (offsetToken present in Snowflake + 1), else -1
   */
  long getOffsetSafeToCommitToKafka();

  /**
   * Close channel associated to this partition Not rethrowing connect exception because the
   * connector will stop. Channel will eventually be reopened.
   *
   * @deprecated use {@link #closeChannelAsync()} instead.
   */
  @Deprecated
  void closeChannel();

  /**
   * Asynchronously closes a channel associated to this partition. Any {@link SFException} occurred
   * is swallowed and a successful {@link CompletableFuture} is returned instead.
   */
  CompletableFuture<Void> closeChannelAsync();

  /* Return true is channel is closed. Caller should handle the logic for reopening the channel if it is closed. */
  boolean isChannelClosed();

  String getChannelNameFormatV1();

  void setLatestConsumerGroupOffset(long consumerOffset);
}
