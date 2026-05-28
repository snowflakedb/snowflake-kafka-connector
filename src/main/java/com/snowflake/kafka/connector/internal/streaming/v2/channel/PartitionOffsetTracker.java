package com.snowflake.kafka.connector.internal.streaming.v2.channel;

import static com.snowflake.kafka.connector.internal.streaming.channel.TopicPartitionChannel.NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE;

import com.snowflake.kafka.connector.internal.KCLogger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * Tracks all offset state for a single partition channel. Offsets are updated during channel
 * init/recovery, record processing in `put`, and when processing channel statuses in `preCommit`.
 *
 * <p>When {@link #initializeFromSnowflake} or {@link #resetAfterRecovery} compute a resume offset,
 * the tracker fires the {@code onOffsetReset} callback supplied at construction. This callback
 * writes to a {@code ConcurrentHashMap} in {@code PartitionChannelManager} — it does not call
 * {@code SinkTaskContext} directly for thread safety.
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
 * #setLatestConsumerGroupOffset}. The remaining field ({@code lastAppendRowsOffset}) is only
 * accessed from the task thread and needs no synchronization.
 */
public class PartitionOffsetTracker {

  private static final KCLogger LOGGER = new KCLogger(PartitionOffsetTracker.class.getName());

  private final String channelName;

  /**
   * Fired when init or recovery computes a resume offset. Writes to the pending-offset-resets map
   * in PartitionChannelManager; the task thread drains that map and calls sinkTaskContext.offset().
   */
  private final Consumer<Long> onOffsetReset;

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

  // When true, leftover rows in the current batch are skipped because the channel was
  // invalidated and offsets were reset in Kafka.
  private boolean needToSkipCurrentBatch = false;

  public PartitionOffsetTracker(String channelName, Consumer<Long> onOffsetReset) {
    this.channelName = channelName;
    this.onOffsetReset = onOffsetReset;
  }

  /**
   * Sets both persisted and processed offsets from the Snowflake-committed offset. If the committed
   * offset is valid, fires {@link #onOffsetReset} with {@code committedOffset + 1}.
   */
  public void initializeFromSnowflake(long committedOffset) {
    LOGGER.info(
        "Initializing offsetPersistedInSnowflake=[{}], channel=[{}]", committedOffset, channelName);
    this.offsetPersistedInSnowflake.set(committedOffset);

    LOGGER.info("Initializing processedOffset=[{}], channel=[{}]", committedOffset, channelName);
    this.processedOffset.set(committedOffset);

    if (committedOffset != NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE) {
      onOffsetReset.accept(committedOffset + 1L);
    } else {
      LOGGER.info(
          "TopicPartitionChannel:{}, offset token is NULL, will rely on Kafka to send us the"
              + " correct offset instead",
          channelName);
    }
  }

  /**
   * Determines whether the given kafka offset should be processed, and manages batch-skip state.
   *
   * @return true if the record should be ingested, false if it should be skipped
   */
  public boolean shouldProcess(long kafkaOffset, boolean isFirstRowInBatch) {
    if (currentConsumerGroupOffset.compareAndSet(
        NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE, kafkaOffset)) {
      LOGGER.trace(
          "Setting currentConsumerGroupOffset=[{}], channel=[{}]", kafkaOffset, channelName);
    }

    if (isFirstRowInBatch) {
      needToSkipCurrentBatch = false;
    }

    if (needToSkipCurrentBatch) {
      LOGGER.info(
          "Ignore inserting offset:{} for channel:{} because we recently reset offset in"
              + " Kafka. currentProcessedOffset:{}",
          kafkaOffset,
          channelName,
          processedOffset.get());
      return false;
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
   * Resets offset state after a channel recovery (reopen). Fires {@link #onOffsetReset} with the
   * computed resume offset unless both the Snowflake offset and the consumer group offset are
   * unknown.
   *
   * @param offsetRecoveredFromSnowflake the offset recovered from Snowflake after reopening
   */
  public void resetAfterRecovery(long offsetRecoveredFromSnowflake) {
    long consumerGroupOffset = currentConsumerGroupOffset.get();
    final long offsetToResetInKafka =
        offsetRecoveredFromSnowflake == NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE
            ? consumerGroupOffset
            : offsetRecoveredFromSnowflake + 1L;

    if (offsetToResetInKafka == NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE) {
      return;
    }

    onOffsetReset.accept(offsetToResetInKafka);

    this.offsetPersistedInSnowflake.set(offsetRecoveredFromSnowflake);
    LOGGER.info(
        "Reset channel metadata after recovery offsetPersistedInSnowflake=[{}], channel=[{}]",
        offsetRecoveredFromSnowflake,
        channelName);
    this.processedOffset.set(offsetRecoveredFromSnowflake);

    // TODO(SNOW-3574225): dead code - needToSkipCurrentBatch is never cleared because the
    // batch-level skip is now handled by offsetsOfFirstSkippedRecord in SSV2.insert(Collection).
    // Remove together with isFirstRowInBatch in shouldProcess().
    needToSkipCurrentBatch = true;
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
