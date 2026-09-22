package com.snowflake.kafka.connector.internal.streaming.v2;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.snowflake.ingest.streaming.SFException;
import org.junit.jupiter.api.Test;

public class ClientRecreationExceptionTest {

  private static final String T1_ENVOY_NR_404 =
      "HTTP request failed with a non-retryable error for API get_subdomain_name."
          + " HTTP 404, error_code=, message=,"
          + " url=https://example.snowflakecomputing.com/v2/streaming/hostname?requestId=abc";

  private static final String OTHER_404 =
      "HTTP request failed with a non-retryable error for API get_subdomain_name."
          + " HTTP 404, error_code=002003, message=Object does not exist or not authorized,"
          + " url=https://example.snowflakecomputing.com/v2/streaming/hostname?requestId=abc";

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
  void shouldRecognizeT1EnvoyNr404() {
    SFException nr404 = new SFException("SfApiUserError", T1_ENVOY_NR_404, 400, "Bad Request");

    assertTrue(ClientRecreationException.isUnenvelopedNr404(nr404));
  }

  @Test
  void shouldNotRecognizeOther404() {
    SFException other404 = new SFException("SfApiUserError", OTHER_404, 400, "Bad Request");

    assertFalse(ClientRecreationException.isUnenvelopedNr404(other404));
  }

  @Test
  void shouldNotRecognizeAccessor404EvenWithNrDetail() {
    SFException accessor404 = new SFException("SfApiUserError", T1_ENVOY_NR_404, 404, "Not Found");

    assertFalse(ClientRecreationException.isUnenvelopedNr404(accessor404));
  }

  @Test
  void shouldNotRecognizeAccessor400WithEmptyDetail() {
    SFException empty400 = new SFException("SfApiUserError", "", 400, "Bad Request");

    assertFalse(ClientRecreationException.isUnenvelopedNr404(empty400));
  }
}
