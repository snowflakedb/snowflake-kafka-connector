package com.snowflake.kafka.connector.internal.streaming.v2;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.snowflake.ingest.streaming.SFException;
import dev.failsafe.function.CheckedRunnable;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockitoAnnotations;

public class AppendRowWithFallbackPolicyTest {

  private final String channelName = "test_channel";

  @BeforeEach
  void setUp() {
    MockitoAnnotations.initMocks(this);
    // Don't actually sleep in unit tests.
    AppendRowWithFallbackPolicy.FALLBACK_DELAY = Duration.ofMillis(1);
    AppendRowWithFallbackPolicy.JITTER_DURATION = Duration.ofMillis(1);
  }

  @AfterEach
  void tearDown() {
    AppendRowWithFallbackPolicy.FALLBACK_DELAY = Duration.ofSeconds(5);
    AppendRowWithFallbackPolicy.JITTER_DURATION = Duration.ofMillis(500);
  }

  @Test
  void shouldReturnChannelOnFirstAttemptSuccess() {
    // Given
    CheckedRunnable supplier = () -> {};

    // When
    boolean succeeded =
        AppendRowWithFallbackPolicy.executeWithFallback(supplier, failingFallback(), channelName);

    // Then
    assertTrue(succeeded, "Should return true on successful append");
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

    // When
    boolean succeeded =
        AppendRowWithFallbackPolicy.executeWithFallback(
            supplier, countingFallbackSupplier(fallbackCallCounter), channelName);

    // Then
    assertEquals(1, attemptCounter.get()); // Should not retry
    assertEquals(1, fallbackCallCounter.get()); // Fallback should be called once
    assertFalse(succeeded, "Should return false when fallback fired");
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

  @Test
  void shouldPropagateClientRecreationExceptionFromFallbackForInvalidClientError() {
    SFException invalidClientException =
        new SFException("InvalidClientError", "Client is invalid", 409, "Conflict");
    CheckedRunnable supplier =
        () -> {
          throw invalidClientException;
        };

    // The fallback supplier throws ClientRecreationException for client-invalid errors
    AppendRowWithFallbackPolicy.FallbackSupplierWithException fallback =
        exception -> {
          if (ClientRecreationException.isClientInvalidError(exception)) {
            throw new ClientRecreationException((SFException) exception);
          }
        };

    ClientRecreationException exception =
        assertThrows(
            ClientRecreationException.class,
            () -> AppendRowWithFallbackPolicy.executeWithFallback(supplier, fallback, channelName));

    assertSame(invalidClientException, exception.getCause());
  }

  @Test
  void shouldPropagateClientRecreationExceptionForSfApiPipeFailedOverError() {
    SFException failoverException =
        new SFException("SfApiPipeFailedOverError", "Pipe failed over", 400, "");
    CheckedRunnable supplier =
        () -> {
          throw failoverException;
        };

    AppendRowWithFallbackPolicy.FallbackSupplierWithException fallback =
        exception -> {
          if (ClientRecreationException.isClientInvalidError(exception)) {
            throw new ClientRecreationException((SFException) exception);
          }
        };

    ClientRecreationException exception =
        assertThrows(
            ClientRecreationException.class,
            () -> AppendRowWithFallbackPolicy.executeWithFallback(supplier, fallback, channelName));

    assertSame(failoverException, exception.getCause());
  }

  @Test
  void shouldCheckBackpressureBeforeClientInvalid() {
    // If an error code is both retryable AND client-invalid (hypothetically),
    // backpressure should win. In practice these sets are disjoint, but this
    // test verifies the ordering.
    SFException retryableException = new SFException("ReceiverSaturated", "Backpressure", 429, "");
    CheckedRunnable supplier =
        () -> {
          throw retryableException;
        };

    // Fallback that would throw ClientRecreationException if reached
    AppendRowWithFallbackPolicy.FallbackSupplierWithException fallback =
        exception -> {
          throw new ClientRecreationException(
              new SFException("InvalidClientError", "Should not reach here", 409, ""));
        };

    // Backpressure check should fire first
    assertThrows(
        BackpressureException.class,
        () -> AppendRowWithFallbackPolicy.executeWithFallback(supplier, fallback, channelName));
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
