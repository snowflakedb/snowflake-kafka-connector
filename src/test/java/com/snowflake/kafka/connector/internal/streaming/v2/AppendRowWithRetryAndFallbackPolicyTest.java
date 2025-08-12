package com.snowflake.kafka.connector.internal.streaming.v2;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.snowflake.ingest.streaming.AppendResult;
import com.snowflake.ingest.streaming.SFException;
import dev.failsafe.function.CheckedSupplier;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class AppendRowWithRetryAndFallbackPolicyTest {

  @Mock private AppendResult appendResult;

  private final String channelName = "test_channel";

  @BeforeEach
  void setUp() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  void shouldReturnChannelOnFirstAttemptSuccess() {
    // Given
    CheckedSupplier<AppendResult> supplier = () -> appendResult;

    // When
    AppendResult result =
        AppendRowWithRetryAndFallbackPolicy.executeWithRetryAndFallback(
            supplier, failingFallback(), channelName);

    // Then
    assertSame(appendResult, result);
  }

  @Test
  void shouldRetryOnRetryableException() {
    // Given
    AtomicInteger attemptCounter = new AtomicInteger(0);
    CheckedSupplier<AppendResult> supplier =
        () -> {
          if (attemptCounter.getAndIncrement() < 2) {
            throw new SFException(
                "MemoryThresholdExceeded", "Some Message", "Some CorrelationId", "Some Stacktrace");
          }
          return appendResult;
        };

    // When
    AppendResult result =
        AppendRowWithRetryAndFallbackPolicy.executeWithRetryAndFallback(
            supplier, failingFallback(), channelName);

    // Then
    assertEquals(3, attemptCounter.get()); // Should retry thrice (1 initial + 2 retries)
    assertSame(appendResult, result);
  }

  @Test
  void shouldFallbackOnNonRetryableSFException() {
    // Given
    AtomicInteger attemptCounter = new AtomicInteger(0);
    SFException nonRetryableException =
        new SFException(
            "NonRetryableError", "Some Message", "Some CorrelationId", "Some Stacktrace");
    CheckedSupplier<AppendResult> supplier =
        () -> {
          if (attemptCounter.getAndIncrement() == 0) {
            throw nonRetryableException;
          }
          return appendResult;
        };
    AtomicInteger fallbackCallCounter = new AtomicInteger(0);

    // When/Then
    AppendResult result =
        AppendRowWithRetryAndFallbackPolicy.executeWithRetryAndFallback(
            supplier, countingFallbackSupplier(fallbackCallCounter), channelName);

    // Then
    assertEquals(1, attemptCounter.get()); // Should not retry
    assertEquals(1, fallbackCallCounter.get()); // Fallback should be called once
    assertNull(result);
  }

  @Test
  void shouldNotRetryNorFallbackOnNonSFException() {
    // Given
    AtomicInteger attemptCounter = new AtomicInteger(0);
    IllegalArgumentException nonRetryableException = new IllegalArgumentException("Non-retryable");
    CheckedSupplier<AppendResult> supplier =
        () -> {
          attemptCounter.getAndIncrement();
          throw nonRetryableException;
        };

    // When/Then
    IllegalArgumentException thrownException =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                AppendRowWithRetryAndFallbackPolicy.executeWithRetryAndFallback(
                    supplier, failingFallback(), channelName));

    assertSame(nonRetryableException, thrownException);
    assertEquals(1, attemptCounter.get()); // Should only attempt once
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
