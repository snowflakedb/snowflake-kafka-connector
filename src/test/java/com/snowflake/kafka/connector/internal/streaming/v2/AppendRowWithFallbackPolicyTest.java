package com.snowflake.kafka.connector.internal.streaming.v2;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.snowflake.ingest.streaming.SFException;
import dev.failsafe.function.CheckedRunnable;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockitoAnnotations;

public class AppendRowWithFallbackPolicyTest {

  private final String channelName = "test_channel";

  @BeforeEach
  void setUp() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  void shouldReturnChannelOnFirstAttemptSuccess() {
    // Given
    CheckedRunnable supplier = () -> {};

    // When
    AppendRowWithFallbackPolicy.executeWithFallback(supplier, failingFallback(), channelName);
  }

  @Test
  void shouldThrowBackpressureExceptionOnRetryableException() {
    // Given
    AtomicInteger attemptCounter = new AtomicInteger(0);
    SFException retryableException =
        new SFException("MemoryThresholdExceeded", "Some Message", 429, "Some Stacktrace");
    CheckedRunnable supplier =
        () -> {
          attemptCounter.getAndIncrement();
          throw retryableException;
        };

    // When/Then
    BackpressureException thrownException =
        assertThrows(
            BackpressureException.class,
            () ->
                AppendRowWithFallbackPolicy.executeWithFallback(
                    supplier, failingFallback(), channelName));

    // Then
    assertEquals(1, attemptCounter.get()); // Should only attempt once (no retry)
    assertSame(retryableException, thrownException.getCause());
    assertEquals("SDK backpressure: MemoryThresholdExceeded", thrownException.getMessage());
  }

  @Test
  void shouldThrowBackpressureExceptionForAllRetryableErrorCodes() {
    // Test ReceiverSaturated
    assertThrowsBackpressureException("ReceiverSaturated");

    // Test MemoryThresholdExceeded
    assertThrowsBackpressureException("MemoryThresholdExceeded");

    // Test MemoryThresholdExceededInContainer
    assertThrowsBackpressureException("MemoryThresholdExceededInContainer");

    // Test HttpRetryableClientError
    assertThrowsBackpressureException("HttpRetryableClientError");
  }

  private void assertThrowsBackpressureException(String errorCode) {
    // Given
    SFException sfException = new SFException(errorCode, "message", 429, "stack");
    CheckedRunnable supplier =
        () -> {
          throw sfException;
        };

    // When/Then
    BackpressureException exception =
        assertThrows(
            BackpressureException.class,
            () ->
                AppendRowWithFallbackPolicy.executeWithFallback(
                    supplier, failingFallback(), channelName));

    assertSame(sfException, exception.getCause());
    assertEquals("SDK backpressure: " + errorCode, exception.getMessage());
  }

  @Test
  void shouldFallbackOnNonRetryableSFException() {
    // Given
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

    // When/Then
    AppendRowWithFallbackPolicy.executeWithFallback(
        supplier, countingFallbackSupplier(fallbackCallCounter), channelName);

    // Then
    assertEquals(1, attemptCounter.get()); // Should not retry
    assertEquals(1, fallbackCallCounter.get()); // Fallback should be called once
  }

  @Test
  void shouldNotRetryNorFallbackOnNonSFException() {
    // Given
    AtomicInteger attemptCounter = new AtomicInteger(0);
    IllegalArgumentException nonRetryableException = new IllegalArgumentException("Non-retryable");
    CheckedRunnable supplier =
        () -> {
          attemptCounter.getAndIncrement();
          throw nonRetryableException;
        };

    // When/Then
    IllegalArgumentException thrownException =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                AppendRowWithFallbackPolicy.executeWithFallback(
                    supplier, failingFallback(), channelName));

    assertSame(nonRetryableException, thrownException);
    assertEquals(1, attemptCounter.get()); // Should only attempt once
  }

  private AppendRowWithFallbackPolicy.FallbackSupplierWithException failingFallback() {
    return exception -> {
      throw new RuntimeException("Test Scenario Failure");
    };
  }

  private AppendRowWithFallbackPolicy.FallbackSupplierWithException countingFallbackSupplier(
      AtomicInteger callCounter) {
    return exception -> callCounter.getAndIncrement();
  }
}
