package com.snowflake.kafka.connector.internal.streaming.v2.channel;

import static com.snowflake.kafka.connector.internal.streaming.channel.TopicPartitionChannel.NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class PartitionOffsetTrackerTest {

  private PartitionOffsetTracker newTracker() {
    return new PartitionOffsetTracker("test-channel", offset -> {});
  }

  @Test
  void contiguousOffsetsProduceNoGap() {
    PartitionOffsetTracker tracker = newTracker();
    tracker.initializeFromSnowflake(9);

    process(tracker, 10);
    process(tracker, 11);
    process(tracker, 12);

    assertEquals(0, tracker.getOffsetGapCount());
    assertEquals(0, tracker.getOffsetGapMissingRecordCount());
  }

  @Test
  void forwardJumpIsCountedAsOneGapWithMissingRows() {
    PartitionOffsetTracker tracker = newTracker();
    tracker.initializeFromSnowflake(9);

    process(tracker, 10); // contiguous
    process(tracker, 15); // gap: missing 11,12,13,14 -> 4 rows

    assertEquals(1, tracker.getOffsetGapCount());
    assertEquals(4, tracker.getOffsetGapMissingRecordCount());
  }

  @Test
  void multipleGapsAccumulate() {
    PartitionOffsetTracker tracker = newTracker();
    tracker.initializeFromSnowflake(0);

    process(tracker, 1); // contiguous
    process(tracker, 5); // gap of 3 (2,3,4)
    process(tracker, 6); // contiguous
    process(tracker, 10); // gap of 3 (7,8,9)

    assertEquals(2, tracker.getOffsetGapCount());
    assertEquals(6, tracker.getOffsetGapMissingRecordCount());
  }

  @Test
  void firstRecordOnUninitializedChannelIsNotAGap() {
    PartitionOffsetTracker tracker = newTracker();
    // processedOffset starts at NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE: a topic that does not
    // start at offset 0 must not be flagged as a gap.
    process(tracker, 100);

    assertEquals(0, tracker.getOffsetGapCount());
    assertEquals(0, tracker.getOffsetGapMissingRecordCount());
  }

  @Test
  void redeliveryAfterRecoveryFromCommittedPlusOneIsNotAGap() {
    PartitionOffsetTracker tracker = newTracker();
    tracker.initializeFromSnowflake(49);
    process(tracker, 50); // contiguous

    // Channel invalidated and reopened at committed=49; Kafka redelivers from 50.
    tracker.resetAfterRecovery(49);
    process(tracker, 50); // first redelivered record, contiguous with committed+1

    assertEquals(0, tracker.getOffsetGapCount());
    assertEquals(0, tracker.getOffsetGapMissingRecordCount());
  }

  @Test
  void uninitializedTrackerStartsAtZeroGaps() {
    PartitionOffsetTracker tracker = newTracker();
    assertEquals(0, tracker.getOffsetGapCount());
    assertEquals(0, tracker.getOffsetGapMissingRecordCount());
    assertEquals(NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE, tracker.getProcessedOffset());
  }

  /** Mirrors the connector's gate-then-advance flow: shouldProcess, then recordProcessed. */
  private void process(PartitionOffsetTracker tracker, long offset) {
    if (tracker.shouldProcess(offset)) {
      tracker.recordProcessed(offset);
    }
  }
}
