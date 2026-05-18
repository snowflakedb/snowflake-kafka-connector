package com.snowflake.kafka.connector.internal.streaming.channel;

/**
 * Outcome of attempting to insert one record into a {@link TopicPartitionChannel}, or — when
 * returned by the batch loop's pre-check — the reason the record was not even attempted.
 *
 * <p>Each value names a distinct reason so the batch loop can decide whether to rewind Kafka (add
 * to the skipped-offsets map) and whether to trigger backpressure cooldown, without having to
 * reason about two-writer races between {@code sinkTaskContext.offset} callers.
 */
public enum InsertResult {
  /** appendRow succeeded and the record is in the SDK buffer. */
  PROCESSED,

  /**
   * Tracker has a Kafka seek in flight (either from this call's fallback, or from an earlier
   * batch's recovery that hasn't landed yet). The tracker owns the rewind; do NOT add to the skip
   * map — doing so would overwrite the authoritative seek.
   */
  RECOVERY_IN_FLIGHT,

  /** Channel has not finished opening. Rewind so Kafka redelivers once it's ready. */
  CHANNEL_INITIALIZING,

  /** Prior batch triggered backpressure; we're still in cooldown. Rewind to try later. */
  IN_BACKPRESSURE_COOLDOWN,

  /**
   * Earlier record in this batch was added to the rewind map for this partition (because of {@link
   * #CHANNEL_INITIALIZING}, {@link #IN_BACKPRESSURE_COOLDOWN}, or {@link #BACKPRESSURE_TRIGGERED});
   * skip the rest. Does NOT apply to {@link #RECOVERY_IN_FLIGHT} — that partition is never added to
   * the rewind map in the first place, so each record is independently classified as
   * RECOVERY_IN_FLIGHT by the in-channel check.
   */
  PREVIOUSLY_SKIPPED_IN_BATCH,

  /** appendRow returned a retryable backpressure error. Rewind + start cooldown. */
  BACKPRESSURE_TRIGGERED
}
