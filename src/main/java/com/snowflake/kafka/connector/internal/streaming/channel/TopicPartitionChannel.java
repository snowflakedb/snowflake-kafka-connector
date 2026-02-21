package com.snowflake.kafka.connector.internal.streaming.channel;

import com.google.common.annotations.VisibleForTesting;
import com.snowflake.ingest.streaming.ChannelStatus;
import com.snowflake.ingest.streaming.SFException;
import com.snowflake.kafka.connector.internal.streaming.telemetry.SnowflakeTelemetryChannelStatus;
import dev.failsafe.Fallback;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.connect.sink.SinkRecord;

public interface TopicPartitionChannel {
  long NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE = -1L;

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
  default long getOffsetSafeToCommitToKafka() {
    final long committedOffsetInSnowflake = fetchOffsetTokenWithRetry();
    if (committedOffsetInSnowflake == NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE) {
      return NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE;
    } else {
      // Return an offset which is + 1 of what was present in snowflake.
      // Idea of sending + 1 back to Kafka is that it should start sending offsets after task
      // restart from this offset
      return committedOffsetInSnowflake + 1;
    }
  }

  /**
   * Asynchronously closes a channel associated to this partition. Any {@link SFException} occurred
   * is swallowed and a successful {@link CompletableFuture} is returned instead.
   */
  CompletableFuture<Void> closeChannelAsync();

  /* Return true is channel is closed. Caller should handle the logic for reopening the channel if it is closed. */
  boolean isChannelClosed();

  String getChannelNameFormatV1();

  void setLatestConsumerGroupOffset(long consumerOffset);

  void checkChannelStatusAndLogErrors(boolean tolerateErrors);

  /**
   * Processes an externally-fetched channel status: logs it, checks for errors, updates offset
   * tracking, and returns the offset safe to commit to Kafka.
   *
   * @param status the channel status fetched via the batch API
   * @param tolerateErrors whether to tolerate ingestion errors
   * @return (offsetToken + 1) safe to commit to Kafka, or {@link
   *     #NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE} if no offset is available
   */
  long processChannelStatus(ChannelStatus status, boolean tolerateErrors);

  /** Returns the pipe name associated with this channel's SDK client. */
  String getPipeName();

  default CompletableFuture<Void> waitForLastProcessedRecordCommitted() {
    return CompletableFuture.completedFuture(null);
  }

  @VisibleForTesting
  SnowflakeTelemetryChannelStatus getSnowflakeTelemetryChannelStatus();

  /**
   * Fetches the offset token from Snowflake.
   *
   * <p>It uses <a href="https://github.com/failsafe-lib/failsafe">Failsafe library </a> which
   * implements retries, fallbacks and circuit breaker.
   *
   * <p>Here is how Failsafe is implemented.
   *
   * <p>Fetches offsetToken from Snowflake (Streaming API)
   *
   * <p>If it returns a valid offset number, that number is returned back to caller.
   *
   * <p>If {@link com.snowflake.ingest.streaming.SFException} is thrown, we will retry for max 3
   * times. (Including the original try)
   *
   * <p>Upon reaching the limit of maxRetries, we will {@link Fallback} to opening a channel and
   * fetching offsetToken again.
   *
   * <p>Please note, upon executing fallback, we might throw an exception too. However, in that case
   * we will not retry.
   *
   * @return long offset token present in snowflake for this channel/partition.
   */
  @VisibleForTesting
  long fetchOffsetTokenWithRetry();
}
