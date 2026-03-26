# SNOW-3248350: Replace Busy-Wait 429 Retry with Offset-Based Backoff

## Problem

`AppendRowWithRetryAndFallbackPolicy` uses a 5s fixed-delay busy-wait retry (`withMaxAttempts(-1)`) when the SDK returns 429 (`MemoryThresholdExceededInContainer`, `MemoryThresholdExceeded`, `ReceiverSaturated`) or 503 (`HttpRetryableClientError`). This blocks the `SinkTask.put()` thread, preventing `consumer.poll()`. With `session.timeout.ms=10000`, two consecutive retries cause the consumer to proactively leave the group, triggering rebalance storms (71 leave events, 121 channel opens for 32 partitions observed in benchmarks).

Vicious cycle: 429 retry -> blocked put() -> consumer can't poll -> session timeout -> rebalance -> channel churn (open/close) -> native memory ratchet -> still 429.

## Solution

Remove the retry loop entirely. On 429/503, throw `BackpressureException` from the Failsafe policy. Catch it at the batch level in `SnowflakeSinkServiceV2.insert()`, rewind all partitions using existing offset tracker state, and return from `put()`. The Kafka Connect framework re-delivers the same records on the next poll cycle, keeping the consumer poll loop alive.

## Design Decisions

### Batch-level vs per-partition handling

When backpressure hits one partition, we stop processing the entire batch and rewind all partitions. Rationale:

- 429 is a global SDK memory pressure signal — other partitions will likely also fail.
- Per-partition handling would fire up to ~500 doomed `appendRow()` calls (default `max.poll.records`) before the batch drains.
- Batch-level handling stops immediately with one clean catch + break.

### All retryable errors use offset-based backoff

All four retryable error types (`MemoryThresholdExceeded`, `MemoryThresholdExceededInContainer`, `ReceiverSaturated`, `HttpRetryableClientError`) are handled the same way — no busy-wait retry for any of them. A 503 transient error also resolves between poll cycles without needing to block `put()`.

### Rewind offsets from PartitionOffsetTracker, not pre-computed

Instead of pre-computing first offsets per partition from the batch, we use `PartitionOffsetTracker.processedOffset + 1` per partition. This gives exact rewind offsets without an extra iteration pass:

- Fully processed partitions: rewind to `processedOffset + 1` (effectively a no-op — where Kafka would deliver next anyway).
- Failed partition: rewind to the exact record that failed (`processedOffset` was not updated for it).
- Untouched partitions: rewind to their correct next offset.

## Architecture

### Signal Flow

```
SnowflakeSinkServiceV2.insert(records)
  for each record:
    channel.insertRecord(record)
      transformAndSend(record)
        insertRowWithFallback(row, offset)
          AppendRowWithRetryAndFallbackPolicy.executeWithFallback(...)
            channel.appendRow() -> throws SFException (429/503)
            isRetryableError(e) -> true
            throws BackpressureException            <-- NEW
        BackpressureException propagates (not caught by existing catch)
      BackpressureException propagates
    catch BackpressureException in insert() loop    <-- NEW
    rewindAllPartitions()                           <-- NEW
    break
```

### Changes to AppendRowWithRetryAndFallbackPolicy

- Remove `memoryBackpressureRetryPolicy` (the `RetryPolicy` builder, `RETRY_DELAY`, `RETRYABLE_ERROR_CODE_NAMES`, `isRetryableError()`).
- In the fallback handler: before entering channel-reopen logic, check if the error is retryable. If so, throw `BackpressureException` instead of reopening — the channel is still valid, it's just memory pressure.
- Rename method from `executeWithRetryAndFallback()` to `executeWithFallback()`.
- The Failsafe composition changes from `Failsafe.with(fallback).compose(retryPolicy).run(action)` to `Failsafe.with(fallback).run(action)`.

### BackpressureException (new class)

Simple unchecked exception in `streaming.v2` package. Wraps the original `SFException`. Contains the retryable error classification logic (the set of retryable error code names).

### Changes to SnowflakeSinkServiceV2.insert()

```java
for (SinkRecord record : records) {
    // ... existing initializing partition check ...
    try {
        insert(record);
    } catch (BackpressureException e) {
        LOGGER.warn("Backpressure detected, rewinding all partitions");
        rewindAllPartitions();
        break;
    }
}
```

New `rewindAllPartitions()` method:

```java
void rewindAllPartitions() {
    for (TopicPartition tp : partitions) {
        TopicPartitionChannel channel = channelManager.getChannel(tp);
        // processedOffset + 1, exposed via PartitionOffsetTracker through the channel
        long nextNeeded = channel.getOffsetSafeToRewindTo();
        sinkTaskContext.offset(tp, nextNeeded);
    }
}
```

### Unchanged Components

- `PartitionOffsetTracker.java` — no changes. Existing `processedOffset` state is sufficient.
- Fallback behavior for non-retryable `SFException` (channel invalidation -> reopen) — unchanged.
- `shouldProcess()` de-duplication on re-delivery — unchanged.

## Edge Cases

**Backpressure on first record of batch:** Nothing processed yet. `rewindAllPartitions()` rewinds every partition to `processedOffset + 1`, which is where they were before this batch. Kafka re-delivers the same batch on next poll.

**Mixed errors in same batch:** Non-retryable SFException (channel invalidation) triggers fallback reopen, throws `TopicPartitionChannelInsertionException`, caught and suppressed in `transformAndSend()`. Processing continues. Later 429 triggers `BackpressureException`, caught in `insert()`, rewind all, break. Both paths work independently.

**Partitions not in current batch:** `rewindAllPartitions()` iterates all assigned partitions. For partitions with no records in this batch, rewinding to `processedOffset + 1` is harmless — it's where Kafka would deliver next anyway.

**Channel still initializing:** Initializing partitions are already skipped before the `insert(record)` call by existing logic. No conflict.

**BackpressureException during channel reopen fallback:** Can't happen — retryable errors are checked first and throw `BackpressureException` before entering reopen logic.

## Files Changed

| File | Change |
|------|--------|
| `BackpressureException.java` (new) | Unchecked exception wrapping `SFException`, with retryable error code classification |
| `AppendRowWithRetryAndFallbackPolicy.java` | Remove retry policy. Guard fallback: retryable -> `BackpressureException`, non-retryable -> reopen channel. Rename to `executeWithFallback()` |
| `TopicPartitionChannel.java` | Add `getOffsetSafeToRewindTo()` to interface |
| `SnowpipeStreamingPartitionChannel.java` | Implement `getOffsetSafeToRewindTo()` — delegates to `offsetTracker.getProcessedOffset() + 1` |
| `SnowflakeSinkServiceV2.java` | Catch `BackpressureException` in `insert(Collection)` -> `rewindAllPartitions()` -> break. New `rewindAllPartitions()` method |
| `AppendRowWithRetryAndFallbackPolicyTest.java` | Update: retryable errors now throw `BackpressureException` instead of being retried |
| `SnowpipeStreamingPartitionChannelTest.java` | Update: backpressure test reflects new behavior |
| `SnowflakeSinkServiceV2Test.java` | New tests for batch-level backpressure handling |

## Testing Strategy

**AppendRowWithRetryAndFallbackPolicy unit tests:**
- Retryable error (429/503) -> throws `BackpressureException` (not retried, not reopened)
- Non-retryable `SFException` -> triggers fallback (channel reopen), existing behavior preserved
- Non-`SFException` -> propagates directly, existing behavior preserved

**SnowflakeSinkServiceV2 unit tests:**
- Backpressure on first record -> all partitions rewound, no records processed
- Backpressure mid-batch -> already-processed partitions rewound to `processedOffset + 1`, break
- No backpressure -> existing behavior unchanged
- Backpressure with initializing partitions -> both mechanisms coexist

**Existing tests updated:**
- `AppendRowWithRetryAndFallbackPolicyTest` — reflect removed retry policy
- `SnowpipeStreamingPartitionChannelTest` — backpressure test reflects new behavior
