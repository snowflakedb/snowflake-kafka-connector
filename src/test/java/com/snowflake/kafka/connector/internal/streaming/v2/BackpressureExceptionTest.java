package com.snowflake.kafka.connector.internal.streaming.v2;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.snowflake.ingest.streaming.SFException;
import org.junit.jupiter.api.Test;

public class BackpressureExceptionTest {

  @Test
  void shouldWrapSFExceptionWithCorrectMessage() {
    // Given
    SFException cause = new SFException("ReceiverSaturated", "Server overloaded", 429, "stack");

    // When
    BackpressureException exception = new BackpressureException(cause);

    // Then
    assertEquals("SDK backpressure: ReceiverSaturated", exception.getMessage());
    assertSame(cause, exception.getCause());
  }

  @Test
  void shouldRecognizeReceiverSaturatedAsRetryable() {
    // Given
    SFException sfException = new SFException("ReceiverSaturated", "message", 429, "stack");

    // When/Then
    assertTrue(BackpressureException.isRetryableError(sfException));
  }

  @Test
  void shouldRecognizeMemoryThresholdExceededAsRetryable() {
    // Given
    SFException sfException =
        new SFException("MemoryThresholdExceeded", "message", 429, "stack");

    // When/Then
    assertTrue(BackpressureException.isRetryableError(sfException));
  }

  @Test
  void shouldRecognizeMemoryThresholdExceededInContainerAsRetryable() {
    // Given
    SFException sfException =
        new SFException("MemoryThresholdExceededInContainer", "message", 429, "stack");

    // When/Then
    assertTrue(BackpressureException.isRetryableError(sfException));
  }

  @Test
  void shouldRecognizeHttpRetryableClientErrorAsRetryable() {
    // Given
    SFException sfException =
        new SFException("HttpRetryableClientError", "message", 503, "stack");

    // When/Then
    assertTrue(BackpressureException.isRetryableError(sfException));
  }

  @Test
  void shouldRejectNonRetryableSFException() {
    // Given
    SFException sfException = new SFException("SomeOtherError", "message", 500, "stack");

    // When/Then
    assertFalse(BackpressureException.isRetryableError(sfException));
  }

  @Test
  void shouldRejectNonSFException() {
    // Given
    IllegalArgumentException nonSFException = new IllegalArgumentException("not an SFException");

    // When/Then
    assertFalse(BackpressureException.isRetryableError(nonSFException));
  }

  @Test
  void shouldRejectNullException() {
    // When/Then
    assertFalse(BackpressureException.isRetryableError(null));
  }

  @Test
  void shouldRejectConstructionWithNonRetryableSFException() {
    // Given
    SFException nonRetryable = new SFException("SomeOtherError", "message", 500, "stack");

    // When/Then
    assertThrows(IllegalArgumentException.class, () -> new BackpressureException(nonRetryable));
  }
}
