package com.snowflake.kafka.connector.internal.streaming.channel;

/** Outcome of {@link TopicPartitionChannel#insertRecord}. */
public enum InsertResult {
  /** Record was ingested or legitimately skipped (duplicate, broken-but-tolerated). */
  PROCESSED,

  /**
   * Recovery was triggered for this channel: the channel was (or will be) reopened and Kafka was
   * (or will be) seeked to the recovery point. The batch loop must abort the rest of this batch
   * without rewinding any partition — recovery's seek is the authoritative position.
   */
  RECOVERY_COMPLETED;
}
