package com.snowflake.kafka.connector.internal.streaming.channel;

import com.google.common.annotations.VisibleForTesting;
import com.snowflake.ingest.streaming.ChannelStatus;
import com.snowflake.ingest.streaming.SFException;
import com.snowflake.kafka.connector.internal.streaming.telemetry.SnowflakeTelemetryChannelStatus;
import dev.failsafe.Fallback;
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
   * Asynchronously closes a channel associated to this partition. Any {@link SFException} occurred
   * is swallowed and a successful {@link CompletableFuture} is returned instead.
   */
  CompletableFuture<Void> closeChannelAsync();

  /* Return true is channel is closed. Caller should handle the logic for reopening the channel if it is closed. */
  boolean isChannelClosed();

  String getChannelNameFormatV1();

  void setLatestConsumerGroupOffset(long consumerOffset);

  /**
   * Processes a channel status: logs it, checks for ingestion errors, updates offset tracking, and
   * returns the offset safe to commit to Kafka.
   *
   * <p>If the committed offset token is null (no data committed yet), returns {@link
   * #NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE}. Otherwise returns (committedOffset + 1) so that
   * Kafka resumes from the next record after a restart.
   *
   * <p>When {@code tolerateErrors} is false and new ingestion errors are detected, throws a
   * connector exception to fail the task.
   *
   * @param status the channel status, typically from a batch status call
   * @param tolerateErrors whether to tolerate ingestion errors (maps to {@code errors.tolerance})
   * @return the offset safe to commit to Kafka, or {@link #NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE}
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
