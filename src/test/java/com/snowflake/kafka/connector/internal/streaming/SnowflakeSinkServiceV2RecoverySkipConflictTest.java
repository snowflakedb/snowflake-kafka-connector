package com.snowflake.kafka.connector.internal.streaming;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

/** Unit tests for the recovery-skip conflict detection used by {@link SnowflakeSinkServiceV2}. */
class SnowflakeSinkServiceV2RecoverySkipConflictTest {

  private static final String TOPIC = "t";

  @Test
  void batchEntirelyPastResumeOffsetIsAConflict() {
    TopicPartition tp = new TopicPartition(TOPIC, 0);
    Map<TopicPartition, Long> pendingResets = Map.of(tp, 51L);
    // Resume point is 51 but the batch starts at 80 -- records 51-79 are unreachable (data loss).
    Collection<SinkRecord> records = batch(tp, 80, 85);

    Set<TopicPartition> conflicts =
        SnowflakeSinkServiceV2.detectRecoverySkipConflicts(pendingResets, records);

    assertEquals(Set.of(tp), conflicts);
  }

  @Test
  void contiguousBatchIncludingResumeOffsetIsNotAConflict() {
    TopicPartition tp = new TopicPartition(TOPIC, 0);
    Map<TopicPartition, Long> pendingResets = Map.of(tp, 51L);
    // Normal post-reset / channel-init delivery: batch includes the resume record 51, so nothing
    // between the committed offset and the batch is dropped. Must NOT be flagged (false positive).
    Collection<SinkRecord> records = batch(tp, 51, 53);

    Set<TopicPartition> conflicts =
        SnowflakeSinkServiceV2.detectRecoverySkipConflicts(pendingResets, records);

    assertTrue(conflicts.isEmpty());
  }

  @Test
  void recordAtExactlyResetOffsetIsNotAConflict() {
    TopicPartition tp = new TopicPartition(TOPIC, 0);
    Map<TopicPartition, Long> pendingResets = Map.of(tp, 51L);
    // Resume point is 51; a record at 51 is the next record to re-deliver, not lost data.
    Collection<SinkRecord> records = batch(tp, 51, 51);

    Set<TopicPartition> conflicts =
        SnowflakeSinkServiceV2.detectRecoverySkipConflicts(pendingResets, records);

    assertTrue(conflicts.isEmpty());
  }

  @Test
  void noPendingResetMeansNoConflict() {
    TopicPartition tp = new TopicPartition(TOPIC, 0);
    Collection<SinkRecord> records = batch(tp, 80, 85);

    Set<TopicPartition> conflicts =
        SnowflakeSinkServiceV2.detectRecoverySkipConflicts(Map.of(), records);

    assertTrue(conflicts.isEmpty());
  }

  @Test
  void pendingResetWithNoRecordsForThatPartitionIsNoConflict() {
    TopicPartition recovering = new TopicPartition(TOPIC, 0);
    TopicPartition other = new TopicPartition(TOPIC, 1);
    Map<TopicPartition, Long> pendingResets = Map.of(recovering, 51L);
    Collection<SinkRecord> records = batch(other, 80, 85);

    Set<TopicPartition> conflicts =
        SnowflakeSinkServiceV2.detectRecoverySkipConflicts(pendingResets, records);

    assertTrue(conflicts.isEmpty());
  }

  @Test
  void onlyPartitionsDeliveredEntirelyPastResetAreFlagged() {
    TopicPartition tp1 = new TopicPartition(TOPIC, 0);
    TopicPartition tp2 = new TopicPartition(TOPIC, 1);
    Map<TopicPartition, Long> pendingResets = Map.of(tp1, 51L, tp2, 10L);
    // tp1's batch is entirely past 51; tp2's batch includes its resume record at 10.
    List<SinkRecord> records = new ArrayList<>();
    records.addAll(batch(tp1, 80, 81));
    records.addAll(batch(tp2, 10, 12));

    Set<TopicPartition> conflicts =
        SnowflakeSinkServiceV2.detectRecoverySkipConflicts(pendingResets, records);

    assertEquals(Set.of(tp1), conflicts);
  }

  private static List<SinkRecord> batch(TopicPartition tp, long from, long to) {
    List<SinkRecord> records = new ArrayList<>();
    for (long o = from; o <= to; o++) {
      records.add(new SinkRecord(tp.topic(), tp.partition(), null, null, null, null, o));
    }
    return records;
  }
}
