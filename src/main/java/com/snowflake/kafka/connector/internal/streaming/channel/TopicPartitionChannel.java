package com.snowflake.kafka.connector.internal.streaming.channel;

import com.google.common.annotations.VisibleForTesting;
import com.snowflake.ingest.streaming.ChannelStatus;
import com.snowflake.ingest.streaming.SFException;
import com.snowflake.kafka.connector.internal.streaming.telemetry.SnowflakeTelemetryChannelStatus;
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
   * @return true if the record was processed (or legitimately skipped as a duplicate), false if
   *     recovery was triggered and the caller should stop feeding records to this partition for the
   *     remainder of the batch
   */
  boolean insertRecord(SinkRecord kafkaSinkRecord);

  /**
   * Asynchronously closes a channel associated to this partition. Any {@link SFException} occurred
   * is swallowed and a successful {@link CompletableFuture} is returned instead.
   */
  CompletableFuture<Void> closeChannelAsync();

  /** A channel which is initializing will be skipped in put and preCommit. */
  default boolean isInitializing() {
    return false;
  }

  /** Blocks until channel initialization is complete. */
  default void awaitInitialization() {}

  /* Return true is channel is closed. Caller should handle the logic for reopening the channel if it is closed. */
  boolean isChannelClosed();

  /**
   * Triggers an asynchronous channel reopen (and SDK client recreation) outside of the {@code
   * appendRow} path.
   *
   * <p>Normally a client-invalid error (e.g. {@code InvalidClientError} after a pipe failover)
   * surfaces on the next {@code appendRow} and drives recovery from there. But if the client is
   * invalidated while there are appended-but-uncommitted records and no new records arrive, {@code
   * appendRow} is never called again, so recovery never starts: {@code preCommit} keeps skipping
   * the failed offset fetch and the task is stuck (it neither ingests the last records nor fails).
   * The {@code preCommit} offset-fetch path calls this when it detects the client is invalid so
   * recovery starts even without new data.
   *
   * <p>No-op by default.
   */
  default void triggerReopenForInvalidClient() {}

  /** Returns the fully qualified channel name in the format of "db.schema.channel". */
  String getChannelNameFormatV1();

  /** Returns the simple (unqualified) channel name, as expected by the SDK batch status API. */
  String getChannelName();

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

  /**
   * Records that this channel finished recovering while the current batch contained only records
   * past its resume offset for this channel (the PROD-538073 data-loss interleaving). Default is a
   * no-op for channel implementations that do not track it.
   */
  default void incRecoverySkipConflictCount() {}

  @VisibleForTesting
  SnowflakeTelemetryChannelStatus getSnowflakeTelemetryChannelStatus();
}
