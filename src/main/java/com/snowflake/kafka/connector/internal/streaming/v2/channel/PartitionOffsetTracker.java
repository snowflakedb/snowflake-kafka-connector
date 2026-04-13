package com.snowflake.kafka.connector.internal.streaming.v2.channel;

import static com.snowflake.kafka.connector.internal.streaming.channel.TopicPartitionChannel.NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE;

import com.snowflake.kafka.connector.internal.KCLogger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkTaskContext;

/**
 * Tracks all offset state for a single partition channel. This is a passive state holder -- it
 * makes no network calls. Offsets are updated during channel init/recovery, record processing in
 * `put`, and when processing channel statuses in `preCommit`.
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
 * #setLatestConsumerGroupOffset}. The remaining fields ({@code lastAppendRowsOffset}, {@code
 * needToSkipCurrentBatch}) are only accessed from the task thread and need no synchronization.
 */
public class PartitionOffsetTracker {

  private static final KCLogger LOGGER = new KCLogger(PartitionOffsetTracker.class.getName());

  private final TopicPartition topicPartition;
  private final SinkTaskContext sinkTaskContext;
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

  // When true, leftover rows in the current batch are skipped because the channel was
  // invalidated and offsets were reset in Kafka.
  private boolean needToSkipCurrentBatch = false;

  // Recovery floor: the highest offset that `resetAfterRecovery` has rewound to. While set
  // (not NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE), `shouldProcess` refuses to accept offsets
  // <= this floor + 1 until Kafka delivers a record at or below (floor + 1), proving the
  // seek has propagated. `recordProcessed` refuses to advance `processedOffset` past this
  // floor. Cleared when a record at or below (floor + 1) is observed in `shouldProcess`.
  //
  // Volatile: written by the IO thread in `resetAfterRecovery` (inside the reopen future's
  // `thenApply`), read by the task thread in `shouldProcess` and `recordProcessed`. The
  // `getChannel().join()` happens-before provides visibility for in-flight writes, but this
  // field is read BEFORE `join()` in `shouldProcess`, so volatile is required.
  private volatile long recoveryFloor = NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE;

  // Counter: batches (first-row observations) since the floor was set without clearing.
  // Used to detect a stuck floor — e.g., `floor+1` was compacted away in a compacted topic
  // and Kafka can never deliver it. Task-thread only; no synchronization needed.
  private int recoveryFloorBatches = 0;

  /**
   * Maximum number of batch-first-row observations to skip while waiting for the floor to
   * clear. After this many, we assume the floor is unreachable (e.g., compacted out) and
   * clear it to avoid deadlocking the partition.
   */
  static final int MAX_RECOVERY_FLOOR_BATCHES = 10;

  public PartitionOffsetTracker(
      TopicPartition topicPartition, SinkTaskContext sinkTaskContext, String channelName) {
    this.topicPartition = topicPartition;
    this.sinkTaskContext = sinkTaskContext;
    this.channelName = channelName;
  }

  /**
   * Sets both persisted and processed offsets, resets the Kafka consumer position, and seeds
   * the recovery floor.
   *
   * <p>Setting the floor here handles the case where the channel is being REBUILT after a
   * failed async reopen (not a cold start): Kafka may have pre-fetched records past the
   * committed offset before the seek takes effect, and without the floor those would advance
   * `processedOffset` past the redelivery point. For genuine cold starts, the first record
   * Kafka delivers will be at or below `committedOffset + 1` (because we just seeked there
   * and the consumer hadn't pre-fetched anything else), which immediately clears the floor.
   */
  public void initializeFromSnowflake(long committedOffset) {
    LOGGER.info(
        "Initializing offsetPersistedInSnowflake=[{}], channel=[{}]", committedOffset, channelName);
    this.offsetPersistedInSnowflake.set(committedOffset);

    LOGGER.info("Initializing processedOffset=[{}], channel=[{}]", committedOffset, channelName);
    this.processedOffset.set(committedOffset);

    resetKafkaOffset(committedOffset);

    if (committedOffset != NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE) {
      this.recoveryFloor = committedOffset;
      this.recoveryFloorBatches = 0;
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

    // Recovery floor: if set, skip records > floor + 1 (Kafka hasn't caught up to the seek
    // `resetAfterRecovery` requested). Clear once we see a record at or below floor + 1,
    // proving the seek has propagated. This is the cross-batch guard Peter Popov described
    // in SNOW-3248350 — `needToSkipCurrentBatch` is a within-batch guard only.
    //
    // Watchdog for compacted topics: if the floor persists across multiple batches without
    // Kafka ever delivering floor+1 (because it was compacted away), the floor would
    // deadlock the partition. After MAX_FLOOR_BATCHES first-row checks without clearing,
    // we assume the floor is unreachable and clear it.
    long floor = this.recoveryFloor;
    if (floor != NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE) {
      if (kafkaOffset <= floor + 1) {
        LOGGER.info(
            "Kafka caught up to recovery floor for channel {} at offset {} (floor={}), clearing",
            channelName,
            kafkaOffset,
            floor);
        this.recoveryFloor = NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE;
        this.recoveryFloorBatches = 0;
        // Fall through — this record IS at or below the floor and should be processed.
      } else if (isFirstRowInBatch
          && ++this.recoveryFloorBatches >= MAX_RECOVERY_FLOOR_BATCHES) {
        LOGGER.warn(
            "Recovery floor {} for channel {} persisted across {} batches without reaching"
                + " floor+1 (first record in batch is {}). Assuming compacted/unavailable;"
                + " clearing to avoid deadlock.",
            floor,
            channelName,
            recoveryFloorBatches,
            kafkaOffset);
        this.recoveryFloor = NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE;
        this.recoveryFloorBatches = 0;
        // Fall through.
      } else {
        LOGGER.info(
            "Skipping offset {} for channel {}: above recovery floor {} (Kafka seek not"
                + " yet propagated)",
            kafkaOffset,
            channelName,
            floor);
        return false;
      }
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
    long floor = this.recoveryFloor;
    if (floor != NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE && kafkaOffset > floor) {
      // A record slipped past `shouldProcess` before `resetAfterRecovery` ran (the race
      // Peter described in SNOW-3248350). Do NOT advance `processedOffset` past the floor
      // — that would cause the records Kafka is about to redeliver to be skipped as
      // duplicates on redelivery, silently losing them.
      LOGGER.info(
          "Refusing to advance processedOffset past recovery floor {} (kafkaOffset={}) for"
              + " channel {}",
          floor,
          kafkaOffset,
          channelName);
      return;
    }
    this.processedOffset.set(kafkaOffset);
    LOGGER.trace("Setting processedOffset=[{}], channel=[{}]", kafkaOffset, channelName);
  }

  /** Called after a row has been successfully passed to appendRow. */
  public void recordAppended(long kafkaOffset) {
    long floor = this.recoveryFloor;
    if (floor != NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE && kafkaOffset > floor) {
      // Same race as `recordProcessed`: a record slipped past `shouldProcess` before
      // `resetAfterRecovery` set the floor, and got `appendRow`'d on the new channel.
      // `lastAppendRowsOffset` is used by `waitForLastProcessedRecordCommitted` as the
      // flush target — advancing it past the floor would make us wait for a Snowflake
      // commit that will never come (the record's data was lost in the pre-recovery
      // flush), causing a flush timeout.
      LOGGER.info(
          "Refusing to advance lastAppendRowsOffset past recovery floor {} (kafkaOffset={}) for"
              + " channel {}",
          floor,
          kafkaOffset,
          channelName);
      return;
    }
    this.lastAppendRowsOffset = kafkaOffset;
  }

  /**
   * Resets offset state after a channel recovery (reopen). Resets the Kafka consumer position and
   * marks the current batch for skipping so leftover rows are discarded.
   *
   * <p>If we don't get a valid offset token (because of a table recreation or channel inactivity),
   * we will rely on Kafka to send us the correct offset.
   *
   * <p>The offset reset in Kafka is set to (offsetRecoveredFromSnowflake + 1) so that Kafka sends
   * offsets starting from the next unprocessed record, avoiding data loss.
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

    sinkTaskContext.offset(topicPartition, offsetToResetInKafka);

    this.offsetPersistedInSnowflake.set(offsetRecoveredFromSnowflake);
    LOGGER.info(
        "Reset channel metadata after recovery offsetPersistedInSnowflake=[{}], channel=[{}]",
        offsetRecoveredFromSnowflake,
        channelName);
    this.processedOffset.set(offsetRecoveredFromSnowflake);

    // Cross-batch recovery guard: any record with offset > this floor is rejected by
    // `shouldProcess` until Kafka delivers a record at/below floor+1. Prevents records
    // that Kafka pre-fetched past the seek from advancing `processedOffset` and causing
    // the redelivered records to be skipped as duplicates. Set AFTER processedOffset to
    // ensure the volatile write publishes both together.
    this.recoveryFloor = offsetRecoveredFromSnowflake;
    this.recoveryFloorBatches = 0;

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

  /** Returns true if a recovery floor is currently set (and thus gating `shouldProcess`). */
  public boolean hasActiveRecoveryFloor() {
    return this.recoveryFloor != NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE;
  }

  private void resetKafkaOffset(long committedOffset) {
    if (committedOffset != NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE) {
      sinkTaskContext.offset(topicPartition, committedOffset + 1L);
    } else {
      LOGGER.info(
          "TopicPartitionChannel:{}, offset token is NULL, will rely on Kafka to send us the"
              + " correct offset instead",
          channelName);
    }
  }
}
