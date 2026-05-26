package com.snowflake.kafka.connector.internal.streaming.v2.channel;

import static com.snowflake.kafka.connector.internal.streaming.channel.TopicPartitionChannel.NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE;

import com.snowflake.kafka.connector.internal.KCLogger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Tracks all offset state for a single partition channel. This is a passive state holder -- it
 * makes no network calls and no Kafka consumer calls. Offsets are updated during channel
 * init/recovery, record processing in `put`, and when processing channel statuses in `preCommit`.
 *
 * <h3>Threading model</h3>
 *
 * Most methods are called from the Kafka Connect task thread, which is single-threaded per
 * partition ({@link #shouldProcess}, {@link #recordProcessed}, {@link #recordAppended}, {@link
 * #initializeFromSnowflake}, {@link #resetAfterRecovery}).
 *
 * <p>{@link #setLatestConsumerGroupOffset} may be called from a different thread, so its
 * set-if-greater logic uses a CAS loop for atomicity. The three AtomicLong fields use atomic types
 * for two reasons: (1) their refs are exposed for telemetry reads from other threads, and (2)
 * {@code currentConsumerGroupOffset} is written by both the task thread and {@link
 * #setLatestConsumerGroupOffset}.
 *
 * <h3>Kafka consumer interaction</h3>
 *
 * This class does NOT call {@code SinkTaskContext.offset()} or any other Kafka consumer API.
 * Callers (via the {@code onChannelReady} callback in {@link
 * com.snowflake.kafka.connector.internal.streaming.v2.SnowpipeStreamingPartitionChannel}) are
 * responsible for issuing the Kafka seek and resume on the task thread.
 */
public class PartitionOffsetTracker {

  private static final KCLogger LOGGER = new KCLogger(PartitionOffsetTracker.class.getName());

  private final String channelName;

  // Offset persisted in Snowflake, determined from the insertRows API / fetchOffsetToken calls.
  private final AtomicLong offsetPersistedInSnowflake =
      new AtomicLong(NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE);

  // KC-side processed offset. On creation set to Snowflake's committed offset, then updated on
  // each new row from KC. Ensures exactly-once semantics.
  private final AtomicLong processedOffset =
      new AtomicLong(NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE);

  // Consumer group offset -- used for telemetry and as a fallback during recovery when Snowflake
  // has no committed offset.
  private final AtomicLong currentConsumerGroupOffset =
      new AtomicLong(NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE);

  // Last offset passed to appendRow -- used by flush to know when all data is committed.
  private long lastAppendRowsOffset = NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE;

  public PartitionOffsetTracker(String channelName) {
    this.channelName = channelName;
  }

  /**
   * Sets both persisted and processed offsets from the initial channel open.
   *
   * @return the Kafka offset to seek to (committedOffset + 1), or {@link
   *     #NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE} if there is no committed offset and the caller
   *     should rely on Kafka's consumer group offset
   */
  public long initializeFromSnowflake(long committedOffset) {
    LOGGER.info(
        "Initializing offsetPersistedInSnowflake=[{}], channel=[{}]", committedOffset, channelName);
    this.offsetPersistedInSnowflake.set(committedOffset);

    LOGGER.info("Initializing processedOffset=[{}], channel=[{}]", committedOffset, channelName);
    this.processedOffset.set(committedOffset);

    if (committedOffset != NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE) {
      return committedOffset + 1L;
    } else {
      LOGGER.info(
          "TopicPartitionChannel:{}, offset token is NULL, will rely on Kafka to send us the"
              + " correct offset instead",
          channelName);
      return NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE;
    }
  }

  /**
   * Determines whether the given kafka offset should be processed.
   *
   * @return true if the record should be ingested, false if it should be skipped (duplicate)
   */
  public boolean shouldProcess(long kafkaOffset) {
    if (currentConsumerGroupOffset.compareAndSet(
        NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE, kafkaOffset)) {
      LOGGER.trace(
          "Setting currentConsumerGroupOffset=[{}], channel=[{}]", kafkaOffset, channelName);
    }

    long currentProcessedOffset = this.processedOffset.get();
    if (currentProcessedOffset == NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE
        || kafkaOffset >= currentProcessedOffset + 1) {
      return true;
    }

    LOGGER.warn(
        "Channel {} - skipping current record - expected offset {} but received {}. The"
            + " current offset stored in Snowflake: {}",
        channelName,
        currentProcessedOffset,
        kafkaOffset,
        offsetPersistedInSnowflake.get());
    return false;
  }

  /** Called after a record has been fully processed (inserted or reported as broken). */
  public void recordProcessed(long kafkaOffset) {
    this.processedOffset.set(kafkaOffset);
    LOGGER.trace("Setting processedOffset=[{}], channel=[{}]", kafkaOffset, channelName);
  }

  /** Called after a row has been successfully passed to appendRow. */
  public void recordAppended(long kafkaOffset) {
    this.lastAppendRowsOffset = kafkaOffset;
  }

  /**
   * Resets offset state after a channel recovery (reopen).
   *
   * <p>If we don't get a valid offset token (because of a table recreation or channel inactivity),
   * we will rely on Kafka to send us the correct offset.
   *
   * <p>The offset reset in Kafka is set to (offsetRecoveredFromSnowflake + 1) so that Kafka sends
   * offsets starting from the next unprocessed record, avoiding data loss.
   *
   * @param offsetRecoveredFromSnowflake the offset recovered from Snowflake after reopening
   * @return the Kafka offset to seek to, or {@link #NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE} if
   *     there is nothing to seek to
   */
  public long resetAfterRecovery(long offsetRecoveredFromSnowflake) {
    long consumerGroupOffset = currentConsumerGroupOffset.get();
    final long offsetToResetInKafka =
        offsetRecoveredFromSnowflake == NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE
            ? consumerGroupOffset
            : offsetRecoveredFromSnowflake + 1L;

    this.offsetPersistedInSnowflake.set(offsetRecoveredFromSnowflake);
    LOGGER.info(
        "Reset channel metadata after recovery offsetPersistedInSnowflake=[{}], channel=[{}]",
        offsetRecoveredFromSnowflake,
        channelName);
    this.processedOffset.set(offsetRecoveredFromSnowflake);

    return offsetToResetInKafka;
  }

  public void setLatestConsumerGroupOffset(long consumerOffset) {
    long current;
    do {
      current = this.currentConsumerGroupOffset.get();
      if (consumerOffset <= current) {
        LOGGER.trace(
            "Not setting currentConsumerGroupOffset because consumerOffset=[{}] is <="
                + " currentConsumerGroupOffset=[{}] for channel=[{}]",
            consumerOffset,
            current,
            channelName);
        return;
      }
    } while (!this.currentConsumerGroupOffset.compareAndSet(current, consumerOffset));
    LOGGER.trace(
        "Setting currentConsumerGroupOffset=[{}], channel=[{}]", consumerOffset, channelName);
  }

  /** For future: allows an external batch service to push a committed offset. */
  public void updatePersistedOffset(long offset) {
    this.offsetPersistedInSnowflake.set(offset);
  }

  public long getPersistedOffset() {
    return offsetPersistedInSnowflake.get();
  }

  public long getProcessedOffset() {
    return processedOffset.get();
  }

  public long getLastAppendRowsOffset() {
    return lastAppendRowsOffset;
  }

  // Expose AtomicLong refs for telemetry binding
  public AtomicLong persistedOffsetRef() {
    return offsetPersistedInSnowflake;
  }

  public AtomicLong processedOffsetRef() {
    return processedOffset;
  }

  public AtomicLong consumerGroupOffsetRef() {
    return currentConsumerGroupOffset;
  }
}
