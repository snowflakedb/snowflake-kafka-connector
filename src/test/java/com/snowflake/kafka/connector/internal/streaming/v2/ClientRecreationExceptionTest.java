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

  // The SDK wraps a client-invalid root cause inside a "ChannelInvalidated" outer error on
  // appendRow, exposing the inner error only as a substring in getMessage(). Detect that.
  @Test
  void shouldRecognizeChannelInvalidatedWrappingInvalidClient() {
    SFException sfException =
        new SFException(
            "ChannelInvalidated",
            "Channel ... is in an invalid state. Inner Error: Client ... is in an invalid state."
                + " Please close and re-create the client. Inner Error: HTTP 410",
            400,
            "stack");

    assertTrue(ClientRecreationException.isClientInvalidError(sfException));
  }

  @Test
  void shouldRecognizeChannelInvalidatedWrappingPipeFailedOver() {
    SFException sfException =
        new SFException(
            "ChannelInvalidated",
            "Channel ... Inner Error: HTTP request failed with a pipe fail over error for API"
                + " get_channel_status_batch.",
            400,
            "stack");

    assertTrue(ClientRecreationException.isClientInvalidError(sfException));
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
}
