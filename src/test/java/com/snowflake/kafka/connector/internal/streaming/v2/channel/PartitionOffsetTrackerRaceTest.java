package com.snowflake.kafka.connector.internal.streaming.v2.channel;

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.snowflake.kafka.connector.internal.streaming.InMemorySinkTaskContext;
import java.util.Set;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

/**
 * Reproduces the data-loss race between {@link PartitionOffsetTracker#shouldProcess} (task thread)
 * and {@link PartitionOffsetTracker#resetAfterRecovery} (IO thread).
 *
 * <p>See design/recovery-race-bug.md for the full write-up.
 */
class PartitionOffsetTrackerRaceTest {

  private static final TopicPartition TP = new TopicPartition("topic", 0);

  /**
   * Simulates the exact interleaving that occurs in production when a non-retryable channel failure
   * happens mid-batch.
   *
   * <p><b>Call sequence that triggers the bug:</b>
   *
   * <ol>
   *   <li>Task thread: {@code shouldProcess(N+1, false)} — reads stale {@code processedOffset=N-1}
   *       before the IO thread has run {@code resetAfterRecovery}. Returns {@code true}.
   *   <li>IO thread: {@code resetAfterRecovery(M)} — sets {@code processedOffset=M}, {@code
   *       needToSkipCurrentBatch=true}, and calls {@code sinkTaskContext.offset(tp, M+1)}.
   *   <li>Task thread: {@code recordProcessed(N+1)} — overwrites {@code processedOffset} back to
   *       {@code N+1}, undoing the reset.
   * </ol>
   *
   * <p>Kafka is told to re-deliver from {@code M+1} but {@code processedOffset=N+1} causes {@link
   * PartitionOffsetTracker#shouldProcess} to reject every offset in the range {@code M+1..N}.
   * Those records were never committed to Snowflake and are silently dropped.
   *
   * <p>The two {@code assertTrue} calls at the end of this test <b>fail with the current code</b>,
   * proving the bug. They should pass once the race is fixed.
   */
  @Test
  void recoveryRace_processedOffsetAdvancesPastReset_silentlyDropsUncommittedRecords() {
    InMemorySinkTaskContext ctx = new InMemorySinkTaskContext(Set.of(TP));
    PartitionOffsetTracker tracker = new PartitionOffsetTracker(TP, ctx, "ch");

    // Snowflake has durably committed through offset 5.
    tracker.initializeFromSnowflake(5L);

    // ── Normal processing ──────────────────────────────────────────────────────────────────────

    // Record 6: processed and appended normally.
    assertTrue(tracker.shouldProcess(6L, /* isFirstRowInBatch= */ true));
    tracker.recordProcessed(6L);

    // Record 7: appendRow throws a non-retryable SFException.
    // reopenChannel() starts async recovery. recordProcessed(7) is NOT called.
    assertTrue(tracker.shouldProcess(7L, /* isFirstRowInBatch= */ false));
    // (record 7 failed — recordProcessed intentionally omitted to match production behaviour)

    // ── Race window ────────────────────────────────────────────────────────────────────────────
    //
    // In production, the task thread calls shouldProcess(8) BEFORE getChannel().join() blocks.
    // The IO thread has not yet run resetAfterRecovery, so:
    //   processedOffset        = 6
    //   needToSkipCurrentBatch = false
    // → shouldProcess returns true.
    assertTrue(tracker.shouldProcess(8L, /* isFirstRowInBatch= */ false));

    // IO thread (inside reopenChannel()'s thenApply, concurrent with getChannel().join()):
    // resetAfterRecovery sets processedOffset=5, needToSkipCurrentBatch=true,
    // and calls sinkTaskContext.offset(TP, 6) so Kafka will re-deliver from 6.
    // The old channel buffer was closed without flushing, so records 6 and 7 are NOT in Snowflake.
    tracker.resetAfterRecovery(5L);

    // Task thread unblocks after join(). appendRow(8) succeeds on the new channel.
    // recordProcessed(8) overwrites the processedOffset=5 that resetAfterRecovery just set.
    tracker.recordProcessed(8L);

    // ── State after the race ───────────────────────────────────────────────────────────────────
    //
    //   processedOffset        = 8   (from recordProcessed above)
    //   sinkTaskContext.offset = 6   (from resetAfterRecovery — Kafka re-delivers from 6)
    //
    // Records 6 and 7 were NEVER committed to Snowflake. They MUST be re-processed.

    // ── Re-delivery (next put() call) ─────────────────────────────────────────────────────────
    //
    // Kafka re-delivers from offset 6.
    //
    // BUG: shouldProcess(6, true) returns false because processedOffset=8 and 6 < 9.
    // The record that was never committed to Snowflake is silently dropped.
    assertTrue(
        tracker.shouldProcess(6L, /* isFirstRowInBatch= */ true),
        "Record 6 was never committed to Snowflake and must be re-processed, "
            + "but shouldProcess incorrectly returns false "
            + "(processedOffset=" + tracker.getProcessedOffset() + ")");

    // BUG: same for record 7.
    assertTrue(
        tracker.shouldProcess(7L, /* isFirstRowInBatch= */ false),
        "Record 7 was never committed to Snowflake and must be re-processed, "
            + "but shouldProcess incorrectly returns false");
  }
}
