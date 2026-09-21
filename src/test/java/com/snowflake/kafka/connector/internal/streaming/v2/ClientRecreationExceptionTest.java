package com.snowflake.kafka.connector.internal.streaming.v2;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.snowflake.ingest.streaming.SFException;
import org.junit.jupiter.api.Test;

public class ClientRecreationExceptionTest {

  @Test
  void shouldWrapSFExceptionWithCorrectMessage() {
    SFException cause = new SFException("InvalidClientError", "Client is invalid", 409, "Conflict");

    ClientRecreationException exception = new ClientRecreationException(cause);

    assertEquals("SDK client invalid: InvalidClientError", exception.getMessage());
    assertSame(cause, exception.getCause());
  }

  @Test
  void shouldRecognizeInvalidClientError() {
    SFException sfException =
        new SFException("InvalidClientError", "Client is invalid", 409, "Conflict");

    assertTrue(ClientRecreationException.isClientInvalidError(sfException));
    assertTrue(ClientRecreationException.isRetryableClientConstructionError(sfException));
  }

  @Test
  void shouldRecognizeSfApiPipeFailedOverError() {
    SFException sfException =
        new SFException("SfApiPipeFailedOverError", "HTTP 410 pipe failover", 400, "Bad Request");

    assertTrue(ClientRecreationException.isClientInvalidError(sfException));
  }

  @Test
  void shouldRecognizeClosedClientError() {
    SFException sfException =
        new SFException("ClosedClientError", "Client is closed", 409, "Conflict");

    assertTrue(ClientRecreationException.isClientInvalidError(sfException));
  }

  @Test
  void shouldNotRecognizeBackpressureErrors() {
    assertFalse(
        ClientRecreationException.isClientInvalidError(
            new SFException("ReceiverSaturated", "message", 429, "stack")));
    assertFalse(
        ClientRecreationException.isClientInvalidError(
            new SFException("MemoryThresholdExceeded", "message", 429, "stack")));
  }

  @Test
  void shouldNotRecognizeChannelLevelErrors() {
    assertFalse(
        ClientRecreationException.isClientInvalidError(
            new SFException("InvalidChannelError", "Channel invalid", 409, "Conflict")));
    assertFalse(
        ClientRecreationException.isClientInvalidError(
            new SFException("ClosedChannelError", "Channel closed", 409, "Conflict")));
  }

  @Test
  void shouldNotRecognizeOtherSFException() {
    SFException sfException = new SFException("SomeOtherError", "message", 500, "stack");

    assertFalse(ClientRecreationException.isClientInvalidError(sfException));
  }

  @Test
  void shouldNotRecognizeNonSFException() {
    IllegalArgumentException nonSFException = new IllegalArgumentException("not an SFException");

    assertFalse(ClientRecreationException.isClientInvalidError(nonSFException));
  }

  @Test
  void shouldNotRecognizeNull() {
    assertFalse(ClientRecreationException.isClientInvalidError(null));
  }

  @Test
  void shouldRejectConstructionWithNonClientInvalidSFException() {
    SFException nonClientInvalid = new SFException("SomeOtherError", "message", 500, "stack");

    assertThrows(
        IllegalArgumentException.class, () -> new ClientRecreationException(nonClientInvalid));
  }

  @Test
  void shouldRejectConstructionWithBackpressureSFException() {
    SFException backpressure = new SFException("ReceiverSaturated", "message", 429, "stack");

    assertThrows(IllegalArgumentException.class, () -> new ClientRecreationException(backpressure));
  }

  @Test
  void shouldNotRecognizeBodyless404AsClientInvalid() {
    SFException bodyless404 = new SFException("SfApiUserError", "", 404, "");

    assertFalse(ClientRecreationException.isClientInvalidError(bodyless404));
    assertTrue(ClientRecreationException.isRetryableClientConstructionError(bodyless404));
    assertThrows(
        IllegalArgumentException.class, () -> new ClientRecreationException(bodyless404));
    // getMessage() is always decorated; emptiness is on getDetailMessage().
    assertFalse(bodyless404.getMessage().isEmpty());
    assertTrue(bodyless404.getDetailMessage().isEmpty());
  }

  @Test
  void shouldNotRecognize404WithErrorMessageAsClientInvalid() {
    SFException notFound = new SFException("SfApiUserError", "pipe not found", 404, "Not Found");

    assertFalse(ClientRecreationException.isClientInvalidError(notFound));
    assertFalse(ClientRecreationException.isRetryableClientConstructionError(notFound));
    assertThrows(IllegalArgumentException.class, () -> new ClientRecreationException(notFound));
  }
}
