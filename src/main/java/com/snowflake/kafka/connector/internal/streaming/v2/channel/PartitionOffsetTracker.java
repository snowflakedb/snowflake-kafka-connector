package com.snowflake.kafka.connector.internal.streaming.v2.channel;

import static com.snowflake.kafka.connector.internal.streaming.channel.TopicPartitionChannel.NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE;

import com.snowflake.kafka.connector.internal.KCLogger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkTaskContext;

/**
 * Tracks all offset state for a single partition channel. This is a passive state holder -- it
 * makes no network calls. Offsets are updated by the channel during init/recovery, and in the
 * future by shared batch services.
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

  public PartitionOffsetTracker(
      TopicPartition topicPartition, SinkTaskContext sinkTaskContext, String channelName) {
    this.topicPartition = topicPartition;
    this.sinkTaskContext = sinkTaskContext;
    this.channelName = channelName;
  }

  /** Sets both persisted and processed offsets, and resets the Kafka consumer position. */
  public void initializeFromSnowflake(long committedOffset) {
    LOGGER.info(
        "Initializing offsetPersistedInSnowflake=[{}], channel=[{}]", committedOffset, channelName);
    this.offsetPersistedInSnowflake.set(committedOffset);

    LOGGER.info("Initializing processedOffset=[{}], channel=[{}]", committedOffset, channelName);
    this.processedOffset.set(committedOffset);

    resetKafkaOffset(committedOffset);
  }

  /**
   * Determines whether the given kafka offset should be processed, and manages batch-skip state.
   *
   * @return true if the record should be ingested, false if it should be skipped
   */
  public boolean shouldProcess(long kafkaOffset, boolean isFirstRowInBatch) {
    if (currentConsumerGroupOffset.get() == NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE) {
      this.currentConsumerGroupOffset.set(kafkaOffset);
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

    needToSkipCurrentBatch = true;
  }

  public void setLatestConsumerGroupOffset(long consumerOffset) {
    if (consumerOffset > this.currentConsumerGroupOffset.get()) {
      this.currentConsumerGroupOffset.set(consumerOffset);
      LOGGER.trace(
          "Setting currentConsumerGroupOffset=[{}], channel=[{}]", consumerOffset, channelName);
    } else {
      LOGGER.trace(
          "Not setting currentConsumerGroupOffset=[{}] because consumerOffset=[{}] is <="
              + " currentConsumerGroupOffset for channel=[{}]",
          this.currentConsumerGroupOffset.get(),
          consumerOffset,
          channelName);
    }
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
