# Backpressure Offset Rewind Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the busy-wait 429 retry loop with batch-level offset rewind so `put()` returns quickly and the Kafka consumer poll loop stays alive.

**Architecture:** On retryable SDK errors (429/503), throw `BackpressureException` from `AppendRowWithRetryAndFallbackPolicy`. Catch it in `SnowflakeSinkServiceV2.insert()`, rewind all partitions in the batch to `processedOffset + 1` via `sinkTaskContext.offset()`, and break. Kafka re-delivers records on the next poll cycle.

**Tech Stack:** Java 11+, Failsafe, Kafka Connect SPI, JUnit 5, Mockito

---

### Task 1: Create `BackpressureException` class with tests

**Files:**
- Create: `src/main/java/com/snowflake/kafka/connector/internal/streaming/v2/BackpressureException.java`
- Create: `src/test/java/com/snowflake/kafka/connector/internal/streaming/v2/BackpressureExceptionTest.java`

- [ ] **Step 1: Write the failing test**

```java
package com.snowflake.kafka.connector.internal.streaming.v2;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.snowflake.ingest.streaming.SFException;
import org.junit.jupiter.api.Test;

class BackpressureExceptionTest {

  @Test
  void wrapsOriginalSFException() {
    SFException cause = new SFException("MemoryThresholdExceeded", "test", 0, "");
    BackpressureException ex = new BackpressureException(cause);
    assertSame(cause, ex.getCause());
  }

  @Test
  void isRetryableError_recognizes429ErrorCodes() {
    assertTrue(
        BackpressureException.isRetryableError(
            new SFException("MemoryThresholdExceeded", "msg", 0, "")));
    assertTrue(
        BackpressureException.isRetryableError(
            new SFException("MemoryThresholdExceededInContainer", "msg", 0, "")));
    assertTrue(
        BackpressureException.isRetryableError(
            new SFException("ReceiverSaturated", "msg", 0, "")));
  }

  @Test
  void isRetryableError_recognizes503ErrorCode() {
    assertTrue(
        BackpressureException.isRetryableError(
            new SFException("HttpRetryableClientError", "msg", 0, "")));
  }

  @Test
  void isRetryableError_rejectsNonRetryableSFException() {
    assertFalse(
        BackpressureException.isRetryableError(
            new SFException("ChannelInvalidated", "msg", 0, "")));
  }

  @Test
  void isRetryableError_rejectsNonSFException() {
    assertFalse(BackpressureException.isRetryableError(new RuntimeException("not SF")));
  }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn test -pl . -Dtest=BackpressureExceptionTest -Dsurefire.failIfNoSpecifiedTests=false --no-transfer-progress -q 2>&1 | tail -20`
Expected: Compilation error — `BackpressureException` does not exist yet.

- [ ] **Step 3: Write minimal implementation**

```java
package com.snowflake.kafka.connector.internal.streaming.v2;

import com.snowflake.ingest.streaming.SFException;
import java.util.Set;

/**
 * Thrown when the SDK signals backpressure (429/503). Caught at the batch level in {@link
 * com.snowflake.kafka.connector.internal.streaming.SnowflakeSinkServiceV2} to rewind all partition
 * offsets and yield the poll loop.
 */
class BackpressureException extends RuntimeException {

  private static final Set<String> RETRYABLE_ERROR_CODE_NAMES =
      Set.of(
          "ReceiverSaturated",
          "MemoryThresholdExceeded",
          "MemoryThresholdExceededInContainer",
          "HttpRetryableClientError");

  BackpressureException(SFException cause) {
    super("SDK backpressure: " + cause.getErrorCodeName(), cause);
  }

  static boolean isRetryableError(Throwable e) {
    if (!(e instanceof SFException)) {
      return false;
    }
    return RETRYABLE_ERROR_CODE_NAMES.contains(((SFException) e).getErrorCodeName());
  }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn test -pl . -Dtest=BackpressureExceptionTest --no-transfer-progress -q 2>&1 | tail -10`
Expected: All 5 tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/main/java/com/snowflake/kafka/connector/internal/streaming/v2/BackpressureException.java src/test/java/com/snowflake/kafka/connector/internal/streaming/v2/BackpressureExceptionTest.java
git commit -m "SNOW-3248350 add BackpressureException with retryable error classification"
```

---

### Task 2: Update `AppendRowWithRetryAndFallbackPolicy` — remove retry, guard fallback

**Files:**
- Modify: `src/main/java/com/snowflake/kafka/connector/internal/streaming/v2/AppendRowWithRetryAndFallbackPolicy.java`
- Modify: `src/test/java/com/snowflake/kafka/connector/internal/streaming/v2/AppendRowWithRetryAndFallbackPolicyTest.java`
- Modify: `src/main/java/com/snowflake/kafka/connector/internal/streaming/v2/SnowpipeStreamingPartitionChannel.java` (call site rename)

- [ ] **Step 1: Update the tests**

Replace the full contents of `AppendRowWithRetryAndFallbackPolicyTest.java`:

```java
package com.snowflake.kafka.connector.internal.streaming.v2;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.snowflake.ingest.streaming.SFException;
import dev.failsafe.function.CheckedRunnable;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

public class AppendRowWithRetryAndFallbackPolicyTest {

  private final String channelName = "test_channel";

  @Test
  void shouldSucceedOnFirstAttempt() {
    CheckedRunnable supplier = () -> {};

    AppendRowWithRetryAndFallbackPolicy.executeWithFallback(
        supplier, failingFallback(), channelName);
  }

  @Test
  void shouldThrowBackpressureExceptionOnRetryableError() {
    SFException retryableException =
        new SFException("MemoryThresholdExceeded", "backpressure", 0, "");
    CheckedRunnable supplier =
        () -> {
          throw retryableException;
        };

    BackpressureException thrown =
        assertThrows(
            BackpressureException.class,
            () ->
                AppendRowWithRetryAndFallbackPolicy.executeWithFallback(
                    supplier, failingFallback(), channelName));

    assertSame(retryableException, thrown.getCause());
  }

  @Test
  void shouldThrowBackpressureExceptionForAllRetryableErrorCodes() {
    for (String errorCode :
        new String[] {
          "MemoryThresholdExceeded",
          "MemoryThresholdExceededInContainer",
          "ReceiverSaturated",
          "HttpRetryableClientError"
        }) {
      SFException exception = new SFException(errorCode, "msg", 0, "");
      CheckedRunnable supplier =
          () -> {
            throw exception;
          };

      assertThrows(
          BackpressureException.class,
          () ->
              AppendRowWithRetryAndFallbackPolicy.executeWithFallback(
                  supplier, failingFallback(), channelName),
          "Expected BackpressureException for error code: " + errorCode);
    }
  }

  @Test
  void shouldNotRetryRetryableErrors() {
    AtomicInteger attemptCounter = new AtomicInteger(0);
    CheckedRunnable supplier =
        () -> {
          attemptCounter.getAndIncrement();
          throw new SFException("MemoryThresholdExceeded", "backpressure", 0, "");
        };

    assertThrows(
        BackpressureException.class,
        () ->
            AppendRowWithRetryAndFallbackPolicy.executeWithFallback(
                supplier, failingFallback(), channelName));

    assertEquals(1, attemptCounter.get(), "Should attempt exactly once — no retries");
  }

  @Test
  void shouldFallbackOnNonRetryableSFException() {
    AtomicInteger attemptCounter = new AtomicInteger(0);
    SFException nonRetryableException =
        new SFException("NonRetryableError", "Some Message", 420, "Some Stacktrace");
    CheckedRunnable supplier =
        () -> {
          if (attemptCounter.getAndIncrement() == 0) {
            throw nonRetryableException;
          }
        };
    AtomicInteger fallbackCallCounter = new AtomicInteger(0);

    AppendRowWithRetryAndFallbackPolicy.executeWithFallback(
        supplier, countingFallbackSupplier(fallbackCallCounter), channelName);

    assertEquals(1, attemptCounter.get());
    assertEquals(1, fallbackCallCounter.get());
  }

  @Test
  void shouldNotRetryNorFallbackOnNonSFException() {
    AtomicInteger attemptCounter = new AtomicInteger(0);
    IllegalArgumentException nonRetryableException = new IllegalArgumentException("Non-retryable");
    CheckedRunnable supplier =
        () -> {
          attemptCounter.getAndIncrement();
          throw nonRetryableException;
        };

    IllegalArgumentException thrownException =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                AppendRowWithRetryAndFallbackPolicy.executeWithFallback(
                    supplier, failingFallback(), channelName));

    assertSame(nonRetryableException, thrownException);
    assertEquals(1, attemptCounter.get());
  }

  private AppendRowWithRetryAndFallbackPolicy.FallbackSupplierWithException failingFallback() {
    return exception -> {
      throw new RuntimeException("Test Scenario Failure");
    };
  }

  private AppendRowWithRetryAndFallbackPolicy.FallbackSupplierWithException
      countingFallbackSupplier(AtomicInteger callCounter) {
    return exception -> callCounter.getAndIncrement();
  }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `mvn test -pl . -Dtest=AppendRowWithRetryAndFallbackPolicyTest --no-transfer-progress -q 2>&1 | tail -20`
Expected: `shouldThrowBackpressureExceptionOnRetryableError` and others fail because the old code retries instead of throwing.

- [ ] **Step 3: Update the implementation**

Replace the full contents of `AppendRowWithRetryAndFallbackPolicy.java`:

```java
package com.snowflake.kafka.connector.internal.streaming.v2;

import com.snowflake.ingest.streaming.SFException;
import com.snowflake.kafka.connector.internal.KCLogger;
import dev.failsafe.Failsafe;
import dev.failsafe.Fallback;
import dev.failsafe.function.CheckedRunnable;
import java.time.Duration;

/**
 * Policy class that encapsulates Failsafe logic for insert row operations with channel reopening
 * fallback functionality.
 *
 * <p>On retryable errors (429/503), throws {@link BackpressureException} immediately so the caller
 * can rewind partition offsets and yield the poll loop. On non-retryable {@link SFException},
 * triggers the fallback (channel reopen).
 */
class AppendRowWithRetryAndFallbackPolicy {

  private static final KCLogger LOGGER =
      new KCLogger(AppendRowWithRetryAndFallbackPolicy.class.getName());

  /** Delay before fallback attempt (channel reopening). */
  private static final Duration FALLBACK_DELAY = Duration.ofMillis(500);

  /** Random jitter added to fallback delay to prevent retry storms. */
  private static final Duration JITTER_DURATION = Duration.ofMillis(200);

  private static void withDelay(CheckedRunnable action, String channelName) throws Throwable {
    try {
      long delayMs =
          FALLBACK_DELAY.toMillis() + (long) (Math.random() * JITTER_DURATION.toMillis());

      LOGGER.info("Delaying channel recovery by {}ms for channel: {}", delayMs, channelName);
      Thread.sleep(delayMs);

      LOGGER.info("Executing channel recovery for channel: {}", channelName);
      action.run();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } catch (SFException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Executes the provided append row action with fallback handling.
   *
   * <p>On retryable {@link SFException} (429/503), throws {@link BackpressureException} without
   * reopening the channel — the channel is still valid, only SDK memory is under pressure.
   *
   * <p>On non-retryable {@link SFException} (channel invalidated), executes the fallback supplier
   * to reopen the channel after a delay with jitter.
   *
   * @param appendRowAction the action to execute (typically channel.appendRow call)
   * @param fallbackSupplier the fallback action on non-retryable failure (channel reopen)
   * @param channelName the channel name for logging purposes
   */
  static void executeWithFallback(
      CheckedRunnable appendRowAction,
      FallbackSupplierWithException fallbackSupplier,
      String channelName) {

    Fallback<Void> reopenChannelFallbackExecutor =
        Fallback.<Void>builder(
                executionAttemptedEvent -> {
                  Throwable lastException = executionAttemptedEvent.getLastException();
                  if (BackpressureException.isRetryableError(lastException)) {
                    throw new BackpressureException((SFException) lastException);
                  }
                  withDelay(
                      () -> fallbackSupplier.execute(lastException),
                      channelName);
                })
            .handle(SFException.class)
            .onFailedAttempt(
                event ->
                    LOGGER.warn(
                        "Failed Attempt to invoke the appendRow API for channel: {}. Exception: {}",
                        channelName,
                        event.getLastException()))
            .onFailure(
                event ->
                    LOGGER.error(
                        "{} Failed to open Channel or fetching offsetToken for channel:{}."
                            + " Exception: {}",
                        "APPEND_ROW_FALLBACK",
                        channelName,
                        event.getException()))
            .build();

    Failsafe.with(reopenChannelFallbackExecutor).run(appendRowAction);
  }

  @FunctionalInterface
  interface FallbackSupplierWithException {
    void execute(Throwable exception) throws Exception;
  }
}
```

- [ ] **Step 4: Update the call site in `SnowpipeStreamingPartitionChannel.java`**

In `SnowpipeStreamingPartitionChannel.java`, change the method call in `insertRowWithFallback` (line 242):

Old:
```java
    AppendRowWithRetryAndFallbackPolicy.executeWithRetryAndFallback(
```

New:
```java
    AppendRowWithRetryAndFallbackPolicy.executeWithFallback(
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `mvn test -pl . -Dtest=AppendRowWithRetryAndFallbackPolicyTest --no-transfer-progress -q 2>&1 | tail -10`
Expected: All 7 tests pass.

- [ ] **Step 6: Commit**

```bash
git add src/main/java/com/snowflake/kafka/connector/internal/streaming/v2/AppendRowWithRetryAndFallbackPolicy.java src/test/java/com/snowflake/kafka/connector/internal/streaming/v2/AppendRowWithRetryAndFallbackPolicyTest.java src/main/java/com/snowflake/kafka/connector/internal/streaming/v2/SnowpipeStreamingPartitionChannel.java
git commit -m "SNOW-3248350 remove retry policy, throw BackpressureException on retryable errors"
```

---

### Task 3: Add `getOffsetSafeToRewindTo()` to interface and implementation

**Files:**
- Modify: `src/main/java/com/snowflake/kafka/connector/internal/streaming/channel/TopicPartitionChannel.java`
- Modify: `src/main/java/com/snowflake/kafka/connector/internal/streaming/v2/SnowpipeStreamingPartitionChannel.java`
- Modify: `src/test/java/com/snowflake/kafka/connector/internal/streaming/v2/SnowpipeStreamingPartitionChannelTest.java`

- [ ] **Step 1: Write the failing test**

Add to the end of `SnowpipeStreamingPartitionChannelTest.java` (before the inner classes):

```java
  @Test
  void getOffsetSafeToRewindTo_returnsProcessedOffsetPlusOne() {
    SnowpipeStreamingPartitionChannel channel = createPartitionChannel();
    channel.getChannel(); // wait for init

    // Insert a record so processedOffset is updated
    channel.insertRecord(buildValidRecord(5), true);

    assertEquals(6, channel.getOffsetSafeToRewindTo());
  }

  @Test
  void getOffsetSafeToRewindTo_returnsNoOffsetWhenNothingProcessed() {
    SnowpipeStreamingPartitionChannel channel = createPartitionChannel();
    channel.getChannel(); // wait for init (processedOffset set to Snowflake's offset, which is -1)

    assertEquals(NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE, channel.getOffsetSafeToRewindTo());
  }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `mvn test -pl . -Dtest=SnowpipeStreamingPartitionChannelTest#getOffsetSafeToRewindTo_returnsProcessedOffsetPlusOne+getOffsetSafeToRewindTo_returnsNoOffsetWhenNothingProcessed --no-transfer-progress -q 2>&1 | tail -20`
Expected: Compilation error — `getOffsetSafeToRewindTo` does not exist.

- [ ] **Step 3: Add to `TopicPartitionChannel` interface**

Add the following method to `TopicPartitionChannel.java` after the `getPipeName()` method (after line 71):

```java
  /**
   * Returns the offset that this partition should be rewound to during backpressure handling.
   *
   * <p>Returns (processedOffset + 1), i.e. the next record the channel needs. Returns {@link
   * #NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE} if no records have been processed yet, meaning no
   * rewind is needed.
   */
  default long getOffsetSafeToRewindTo() {
    return NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE;
  }
```

- [ ] **Step 4: Implement in `SnowpipeStreamingPartitionChannel`**

Add the following method to `SnowpipeStreamingPartitionChannel.java` after the `getPipeName()` method (after line 698):

```java
  @Override
  public long getOffsetSafeToRewindTo() {
    long processed = offsetTracker.getProcessedOffset();
    if (processed == NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE) {
      return NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE;
    }
    return processed + 1;
  }
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `mvn test -pl . -Dtest=SnowpipeStreamingPartitionChannelTest#getOffsetSafeToRewindTo_returnsProcessedOffsetPlusOne+getOffsetSafeToRewindTo_returnsNoOffsetWhenNothingProcessed --no-transfer-progress -q 2>&1 | tail -10`
Expected: Both tests pass.

- [ ] **Step 6: Commit**

```bash
git add src/main/java/com/snowflake/kafka/connector/internal/streaming/channel/TopicPartitionChannel.java src/main/java/com/snowflake/kafka/connector/internal/streaming/v2/SnowpipeStreamingPartitionChannel.java src/test/java/com/snowflake/kafka/connector/internal/streaming/v2/SnowpipeStreamingPartitionChannelTest.java
git commit -m "SNOW-3248350 add getOffsetSafeToRewindTo to TopicPartitionChannel interface"
```

---

### Task 4: Add backpressure handling to `SnowflakeSinkServiceV2.insert()`

**Files:**
- Modify: `src/main/java/com/snowflake/kafka/connector/internal/streaming/SnowflakeSinkServiceV2.java`
- Modify: `src/test/java/com/snowflake/kafka/connector/internal/streaming/SnowflakeSinkServiceV2Test.java`

- [ ] **Step 1: Write the failing tests**

Add the following tests to `SnowflakeSinkServiceV2Test.java`. First, add these imports at the top:

```java
import static org.mockito.Mockito.doThrow;
import com.snowflake.ingest.streaming.SFException;
import com.snowflake.kafka.connector.internal.streaming.v2.BackpressureException;
```

Note: `BackpressureException` is package-private in `streaming.v2`. The test is in `streaming`. To make it accessible, change the class visibility of `BackpressureException` from package-private to `public` (update `class BackpressureException` to `public class BackpressureException` in the file created in Task 1).

Then add these test methods:

```java
  // --- backpressure handling ---

  @Test
  void insertRewindsAllPartitionsOnBackpressure() {
    TopicPartition tp0 = new TopicPartition(TOPIC, 0);
    TopicPartition tp1 = new TopicPartition(TOPIC, 1);

    TopicPartitionChannel channel0 = mockChannel("ch_0", false);
    TopicPartitionChannel channel1 = mockChannel("ch_1", false);

    doThrow(
            new BackpressureException(
                new SFException("MemoryThresholdExceeded", "backpressure", 0, "")))
        .when(channel0)
        .insertRecord(any(), anyBoolean());

    when(channel0.getOffsetSafeToRewindTo()).thenReturn(10L);
    when(channel1.getOffsetSafeToRewindTo()).thenReturn(20L);

    when(mockChannelManager.getChannel(tp0)).thenReturn(Optional.of(channel0));
    when(mockChannelManager.getChannel(tp1)).thenReturn(Optional.of(channel1));

    List<SinkRecord> records = Arrays.asList(recordFor(TOPIC, 0, 10), recordFor(TOPIC, 1, 20));

    service.insert(records);

    // channel0 threw BackpressureException — channel1 should not have been attempted
    verify(channel0).insertRecord(any(), anyBoolean());
    verify(channel1, never()).insertRecord(any(), anyBoolean());

    // Both partitions rewound
    verify(mockSinkTaskContext).offset(tp0, 10L);
    verify(mockSinkTaskContext).offset(tp1, 20L);
  }

  @Test
  void insertRewindsOnBackpressureMidBatch() {
    TopicPartition tp0 = new TopicPartition(TOPIC, 0);
    TopicPartition tp1 = new TopicPartition(TOPIC, 1);

    TopicPartitionChannel channel0 = mockChannel("ch_0", false);
    TopicPartitionChannel channel1 = mockChannel("ch_1", false);

    // channel1 throws on insertRecord
    doThrow(
            new BackpressureException(
                new SFException("ReceiverSaturated", "backpressure", 0, "")))
        .when(channel1)
        .insertRecord(any(), anyBoolean());

    when(channel0.getOffsetSafeToRewindTo()).thenReturn(11L);
    when(channel1.getOffsetSafeToRewindTo()).thenReturn(20L);

    when(mockChannelManager.getChannel(tp0)).thenReturn(Optional.of(channel0));
    when(mockChannelManager.getChannel(tp1)).thenReturn(Optional.of(channel1));

    // tp0 record processed successfully, tp1 record triggers backpressure
    List<SinkRecord> records = Arrays.asList(recordFor(TOPIC, 0, 10), recordFor(TOPIC, 1, 20));

    service.insert(records);

    // channel0 processed its record before backpressure
    verify(channel0).insertRecord(any(), anyBoolean());
    // channel1 attempted but threw
    verify(channel1).insertRecord(any(), anyBoolean());

    // Both partitions rewound — channel0 to 11 (processedOffset+1), channel1 to 20
    verify(mockSinkTaskContext).offset(tp0, 11L);
    verify(mockSinkTaskContext).offset(tp1, 20L);
  }

  @Test
  void insertRewindsOnBackpressureWithInitializingPartitions() {
    TopicPartition tpInit = new TopicPartition(TOPIC, 0);
    TopicPartition tpReady = new TopicPartition(TOPIC, 1);

    TopicPartitionChannel initChannel = mockChannel("ch_0", true);
    TopicPartitionChannel readyChannel = mockChannel("ch_1", false);

    doThrow(
            new BackpressureException(
                new SFException("MemoryThresholdExceeded", "backpressure", 0, "")))
        .when(readyChannel)
        .insertRecord(any(), anyBoolean());

    when(readyChannel.getOffsetSafeToRewindTo()).thenReturn(200L);

    when(mockChannelManager.getChannel(tpInit)).thenReturn(Optional.of(initChannel));
    when(mockChannelManager.getChannel(tpReady)).thenReturn(Optional.of(readyChannel));

    List<SinkRecord> records = Arrays.asList(recordFor(TOPIC, 0, 100), recordFor(TOPIC, 1, 200));

    service.insert(records);

    // Initializing partition skipped (never inserted)
    verify(initChannel, never()).insertRecord(any(), anyBoolean());
    // Ready partition hit backpressure
    verify(readyChannel).insertRecord(any(), anyBoolean());

    // Initializing partition rewound via existing logic (first skipped offset)
    verify(mockSinkTaskContext).offset(tpInit, 100L);
    // Ready partition rewound via backpressure logic
    verify(mockSinkTaskContext).offset(tpReady, 200L);
  }

  @Test
  void insertSkipsRewindForPartitionsWithNoOffset() {
    TopicPartition tp0 = new TopicPartition(TOPIC, 0);

    TopicPartitionChannel channel0 = mockChannel("ch_0", false);

    doThrow(
            new BackpressureException(
                new SFException("MemoryThresholdExceeded", "backpressure", 0, "")))
        .when(channel0)
        .insertRecord(any(), anyBoolean());

    // No offset registered — should skip rewind for this partition
    when(channel0.getOffsetSafeToRewindTo()).thenReturn(-1L);

    when(mockChannelManager.getChannel(tp0)).thenReturn(Optional.of(channel0));

    service.insert(Collections.singletonList(recordFor(TOPIC, 0, 5)));

    verify(mockSinkTaskContext, never()).offset(any(TopicPartition.class), any(Long.class));
  }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `mvn test -pl . -Dtest=SnowflakeSinkServiceV2Test#insertRewindsAllPartitionsOnBackpressure+insertRewindsOnBackpressureMidBatch+insertRewindsOnBackpressureWithInitializingPartitions+insertSkipsRewindForPartitionsWithNoOffset --no-transfer-progress -q 2>&1 | tail -20`
Expected: Tests fail — `BackpressureException` is not caught in `insert()` so it propagates as an unhandled exception.

- [ ] **Step 3: Update `BackpressureException` to be public**

In `BackpressureException.java`, change:

Old:
```java
class BackpressureException extends RuntimeException {
```

New:
```java
public class BackpressureException extends RuntimeException {
```

Also change `isRetryableError` to public:

Old:
```java
  static boolean isRetryableError(Throwable e) {
```

New:
```java
  public static boolean isRetryableError(Throwable e) {
```

- [ ] **Step 4: Add backpressure handling to `SnowflakeSinkServiceV2.insert()`**

In `SnowflakeSinkServiceV2.java`, add this import:

```java
import com.snowflake.kafka.connector.internal.streaming.v2.BackpressureException;
```

Replace the `insert(Collection<SinkRecord>)` method (lines 304-342) with:

```java
  @Override
  public void insert(final Collection<SinkRecord> records) {
    channelsVisitedPerBatch.clear();

    // Skip partitions for which the partition-channel bridge is currently being initialized.
    Set<TopicPartition> partitions =
        records.stream()
            .map(record -> new TopicPartition(record.topic(), record.kafkaPartition()))
            .collect(Collectors.toSet());

    Set<TopicPartition> initializingPartitions = currentlyInitializing(partitions);
    if (!initializingPartitions.isEmpty()) {
      LOGGER.debug(
          "Skipping put for {}/{} partitions that are currently being initialized: {}",
          initializingPartitions.size(),
          partitions.size(),
          initializingPartitions);
    }

    Map<TopicPartition, Long> offsetsOfFirstSkippedRecord = new HashMap<>();
    for (SinkRecord record : records) {
      // check if it needs to handle null value records
      if (shouldSkipNullValue(record)) {
        continue;
      }

      TopicPartition tp = new TopicPartition(record.topic(), record.kafkaPartition());
      if (initializingPartitions.contains(tp)) {
        offsetsOfFirstSkippedRecord.putIfAbsent(tp, record.kafkaOffset());
        continue;
      }

      try {
        insert(record);
      } catch (BackpressureException e) {
        LOGGER.warn(
            "Backpressure detected on partition {}, rewinding all partitions in batch: {}",
            tp,
            e.getMessage());
        rewindPartitions(partitions);
        break;
      }
    }

    if (!offsetsOfFirstSkippedRecord.isEmpty()) {
      LOGGER.info("Rewinding offsets for initializing partitions: {}", offsetsOfFirstSkippedRecord);
      offsetsOfFirstSkippedRecord.forEach(sinkTaskContext::offset);
    }
  }

  private void rewindPartitions(Set<TopicPartition> partitions) {
    for (TopicPartition tp : partitions) {
      channelManager
          .getChannel(tp)
          .ifPresent(
              channel -> {
                if (channel.isInitializing()) {
                  return;
                }
                long offsetToRewind = channel.getOffsetSafeToRewindTo();
                if (offsetToRewind
                    != TopicPartitionChannel.NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE) {
                  sinkTaskContext.offset(tp, offsetToRewind);
                }
              });
    }
  }
```

- [ ] **Step 5: Run new tests to verify they pass**

Run: `mvn test -pl . -Dtest=SnowflakeSinkServiceV2Test --no-transfer-progress -q 2>&1 | tail -10`
Expected: All tests pass (both new and existing).

- [ ] **Step 6: Commit**

```bash
git add src/main/java/com/snowflake/kafka/connector/internal/streaming/SnowflakeSinkServiceV2.java src/test/java/com/snowflake/kafka/connector/internal/streaming/SnowflakeSinkServiceV2Test.java src/main/java/com/snowflake/kafka/connector/internal/streaming/v2/BackpressureException.java
git commit -m "SNOW-3248350 catch BackpressureException in insert(), rewind all partitions"
```

---

### Task 5: Update `SnowpipeStreamingPartitionChannelTest` backpressure test

**Files:**
- Modify: `src/test/java/com/snowflake/kafka/connector/internal/streaming/v2/SnowpipeStreamingPartitionChannelTest.java`

- [ ] **Step 1: Update the backpressure test**

Replace the `insertRecordRetriesOnBackpressureWithoutReopeningChannel` test (lines 228-241) with:

```java
  @Test
  void insertRecordThrowsBackpressureExceptionOnRetryableError() {
    SnowpipeStreamingPartitionChannel partitionChannel = createPartitionChannel();
    partitionChannel.getChannel();
    assertEquals(1, trackingClientSupplier.getTotalChannelsCreated());

    // appendRow will throw MemoryThresholdExceeded — should now throw BackpressureException
    trackingClientSupplier.setRetryableAppendRowFailures(1);

    assertThrows(
        BackpressureException.class,
        () -> partitionChannel.insertRecord(buildValidRecord(0), true));

    // No channel reopening should have happened — the channel is still valid
    assertEquals(0, trackingClientSupplier.getCloseCallCount());
    assertEquals(1, trackingClientSupplier.getTotalChannelsCreated());
  }
```

- [ ] **Step 2: Run the updated test**

Run: `mvn test -pl . -Dtest=SnowpipeStreamingPartitionChannelTest#insertRecordThrowsBackpressureExceptionOnRetryableError --no-transfer-progress -q 2>&1 | tail -10`
Expected: Test passes.

- [ ] **Step 3: Run all tests in the modified test files**

Run: `mvn test -pl . -Dtest=BackpressureExceptionTest,AppendRowWithRetryAndFallbackPolicyTest,SnowpipeStreamingPartitionChannelTest,SnowflakeSinkServiceV2Test --no-transfer-progress -q 2>&1 | tail -15`
Expected: All tests pass.

- [ ] **Step 4: Commit**

```bash
git add src/test/java/com/snowflake/kafka/connector/internal/streaming/v2/SnowpipeStreamingPartitionChannelTest.java
git commit -m "SNOW-3248350 update backpressure test to expect BackpressureException"
```

---

### Task 6: Full test suite verification

- [ ] **Step 1: Run the full unit test suite**

Run: `mvn test --no-transfer-progress -q 2>&1 | tail -20`
Expected: All unit tests pass. No regressions.

- [ ] **Step 2: Verify no compilation warnings**

Run: `mvn compile --no-transfer-progress -q 2>&1 | tail -10`
Expected: Clean compilation with no errors.
