# Recovery Race: `processedOffset` Advances Past `resetAfterRecovery`, Causing Silent Data Loss

**Component:** `PartitionOffsetTracker` / `SnowpipeStreamingPartitionChannel`
**Severity:** Data loss (silent — no exception, no log at ERROR)
**Status:** Open
**Repro test:** `PartitionOffsetTrackerRaceTest#recoveryRace_processedOffsetAdvancesPastReset_silentlyDropsUncommittedRecords`

---

## Background

The connector uses two components to guard against double-processing:

- `PartitionOffsetTracker.processedOffset` — the highest Kafka offset the task thread has finished processing. Updated by `recordProcessed()` after every successful record.
- `PartitionOffsetTracker.needToSkipCurrentBatch` — set `true` by `resetAfterRecovery()` to skip leftover records in the current batch after a channel reopen.

Together these are checked in `shouldProcess()` before every `insertRecord()` call. The invariant is:

> If Snowflake has only committed through offset `M`, every offset `> M` must pass `shouldProcess` so it can be appended to the channel.

This invariant is violated when a channel recovery and the processing of the next record in the same batch overlap.

---

## The Threading Model

`SnowpipeStreamingPartitionChannel` holds `this.channel` as a `volatile CompletableFuture`. When a non-retryable `SFException` is thrown by `appendRow`, the Failsafe fallback calls `reopenChannel()`, which replaces `this.channel` with a new future chain that runs on a dedicated IO executor thread:

```
this.channel = this.channel
    .thenAccept(old -> closeChannelWithoutFlushing(old))   // IO thread
    .thenApply(ignored -> {
        openChannelForTable(channelName);
        offsetTracker.resetAfterRecovery(M);               // IO thread ← writes needToSkipCurrentBatch
        return newChannel;
    });
```

The task thread reaches `appendRow` (and therefore `getChannel().join()`) only after already passing through `shouldProcess`. This creates the race window:

```
Task thread                            IO thread (thenApply)
────────────────────────────────────   ──────────────────────────────────────
shouldProcess(N+1, false)              (reopenChannel future is queued)
  reads needToSkipCurrentBatch = false
  reads processedOffset = N-1
  → returns true
transformAndSend(N+1)
  getChannel().join()  ← BLOCKS ──────→ closeChannelWithoutFlushing(old)
                                         openChannelForTable(...)
                                         resetAfterRecovery(M):
                                           processedOffset.set(M)
                                           needToSkipCurrentBatch = true
                                           sinkTaskContext.offset(tp, M+1)
                       ← UNBLOCKS ────
  appendRow(N+1) on new channel → OK
  recordProcessed(N+1)
    processedOffset.set(N+1)   ← OVERWRITES the M set by resetAfterRecovery
```

---

## The Bug Step by Step

Using concrete offsets: Snowflake has committed through **5**, the batch contains records **6, 7, 8**.

| Step | Thread | Action | `processedOffset` | `needToSkipCurrentBatch` |
|------|--------|--------|--------------------|--------------------------|
| 1 | Task | `initializeFromSnowflake(5)` | 5 | false |
| 2 | Task | `shouldProcess(6, true)` → **true** | 5 | false |
| 3 | Task | `recordProcessed(6)` | **6** | false |
| 4 | Task | `shouldProcess(7, false)` → **true** | 6 | false |
| 5 | Task | `appendRow(7)` → `SFException` → `reopenChannel()` starts | 6 | false |
| 6 | Task | `shouldProcess(8, false)` → **true** (race window — IO thread not yet run) | 6 | false |
| 7 | Task | `getChannel().join()` **BLOCKS** | 6 | false |
| 8 | IO | `closeChannelWithoutFlushing()` — buffer discarded, records 6–7 lost from channel | 6 | false |
| 9 | IO | `resetAfterRecovery(5)` → `processedOffset=5`, `sinkTaskContext.offset(tp, 6)` | **5** | **true** |
| 10 | Task | `getChannel().join()` **UNBLOCKS** | 5 | true |
| 11 | Task | `appendRow(8)` on new channel → OK | 5 | true |
| 12 | Task | `recordProcessed(8)` | **8** | true |

**End state:**
- `processedOffset = 8`
- `sinkTaskContext` tells Kafka to re-deliver `tp` from offset **6**
- Records 6 and 7 were **never committed to Snowflake** (old channel buffer discarded without flush)

**Re-delivery (next `put()`):**

```
shouldProcess(6, isFirstRowInBatch=true):
  needToSkipCurrentBatch = false  (reset because isFirstRowInBatch=true)
  processedOffset = 8
  6 >= 8+1?  →  false  →  SKIP  ← DATA LOSS
```

Records 6 and 7 are silently discarded. No exception is raised, no ERROR is logged. From the operator's perspective the connector is healthy.

---

## Why `needToSkipCurrentBatch` Doesn't Save Us

`needToSkipCurrentBatch` is designed to skip records in the batch **after** a recovery. It is set to `true` by `resetAfterRecovery` (step 9 above). However, `shouldProcess` resets it back to `false` whenever `isFirstRowInBatch = true`:

```java
if (isFirstRowInBatch) {
    needToSkipCurrentBatch = false;  // ← always clears the flag for the first record of a new batch
}
```

On re-delivery, the first record of the next batch has `isFirstRowInBatch = true`, which clears the flag before it is checked. This is correct behaviour for re-delivery — the flag is a within-batch guard, not a cross-batch guard. The cross-batch guard is `processedOffset`, which is now wrong.

Additionally, even within the same batch, `needToSkipCurrentBatch` is read from the task thread **without synchronisation** — it is a plain `boolean` written by the IO thread. The class-level comment acknowledges this incorrectly:

```java
// The remaining fields (lastAppendRowsOffset, needToSkipCurrentBatch) are only
// accessed from the task thread and need no synchronization.
```

This comment is wrong. `resetAfterRecovery` is called from the IO thread's `thenApply` and writes `needToSkipCurrentBatch`. Even if `processedOffset` were fixed, reads of `needToSkipCurrentBatch` in `shouldProcess` happen before `getChannel().join()`, so there is no happens-before to guarantee visibility of the IO thread's write.

---

## Root Cause

There are two separate issues that combine to produce data loss:

1. **`processedOffset` is overwritten after recovery.** `recordProcessed(N+1)` runs after `resetAfterRecovery(M)` within the same task-thread invocation. Because `resetAfterRecovery` sets `processedOffset = M` and then `recordProcessed(N+1)` sets `processedOffset = N+1 > M`, the recovery's reset is undone.

2. **`needToSkipCurrentBatch` is not safely published.** It is a non-volatile plain `boolean` written by the IO thread (inside `thenApply`) and read by the task thread (inside `shouldProcess`) with no intervening synchronisation point. There is a happens-before only after `getChannel().join()` returns — but `shouldProcess` runs before `join()`.

---

## Reproducing the Bug

`PartitionOffsetTrackerRaceTest#recoveryRace_processedOffsetAdvancesPastReset_silentlyDropsUncommittedRecords` reproduces the exact call sequence without requiring thread timing. It calls the methods in the order that the race produces and asserts that re-delivered records must be accepted by `shouldProcess`. Both assertions **fail** with the current code.

```
Record 6 was never committed to Snowflake and must be re-processed,
but shouldProcess incorrectly returns false (processedOffset=8)
```

---

## Possible Fixes

### Option A — Refuse to advance `processedOffset` past the recovery point

After `resetAfterRecovery(M)` sets `processedOffset = M`, any subsequent `recordProcessed(X)` call where `X > M` should be rejected (or the record should not have been allowed to proceed past `shouldProcess` in the first place).

The cleanest enforcement: after `resetAfterRecovery`, record which offset recovery set (`recoveryProcessedOffset`) and gate `recordProcessed`:

```java
public void recordProcessed(long kafkaOffset) {
    if (recoveryProcessedOffset != NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE
        && kafkaOffset > recoveryProcessedOffset) {
        // This record slipped through shouldProcess before recovery ran.
        // Do not advance processedOffset past the recovery point.
        return;
    }
    this.processedOffset.set(kafkaOffset);
}
```

### Option B — Make `needToSkipCurrentBatch` volatile and check it after `getChannel().join()`

Declare `needToSkipCurrentBatch` as `volatile`. Move the `shouldProcess` call to after `getChannel()` so the happens-before from the future's completion covers the flag read. This requires restructuring `insertRecord` / `transformAndSend`.

### Option C — Complete the recovery synchronously on the task thread

Instead of running `resetAfterRecovery` inside `thenApply` (IO thread), run it on the task thread by blocking on the channel future immediately after triggering recovery. This eliminates the race entirely at the cost of a small synchronous delay on the task thread to close and reopen the channel. The consumer poll loop would still be blocked during this window, but only for the duration of the channel open (milliseconds), which is acceptable compared to the current 5-second retry.

---

## Impact Assessment

| Condition | Probability |
|-----------|-------------|
| Non-retryable `SFException` on any record in a batch | Low — requires SDK channel invalidation |
| More records for the same partition later in the same batch | Moderate — depends on batch ordering |
| Recovery completes **during** `getChannel().join()` rather than before `shouldProcess` | Moderate — depends on executor scheduling |
| Combined probability per batch | Low-moderate |

The bug is silent (no exception, no ERROR log), so it can go undetected for a long time. The data loss is proportional to how many records followed the failing record in the batch and were not yet committed to Snowflake.
