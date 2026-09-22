package com.snowflake.kafka.connector.internal.streaming.v2;

import com.google.common.base.Preconditions;
import com.snowflake.ingest.streaming.SFException;
import java.util.Set;

/**
 * Unchecked exception thrown when the Snowflake SDK signals that the streaming client is in an
 * invalid state and must be recreated.
 *
 * <p>This exception wraps {@link SFException} instances with specific error codes indicating the
 * client itself is no longer usable. Unlike {@link BackpressureException} (where the channel
 * remains valid), this signals that the client and all its channels must be replaced.
 *
 * <p>Client-invalid error codes:
 *
 * <ul>
 *   <li>{@code InvalidClientError} - client marked invalid after a fatal internal error or pipe
 *       failover (409 Conflict)
 *   <li>{@code SfApiPipeFailedOverError} - HTTP 410 on any API call triggers client invalidation
 *   <li>{@code ClosedClientError} - client has been closed and cannot be reused (409 Conflict)
 * </ul>
 *
 * <p>An Envoy NR HTTP 404 (empty GS {@code error_code}/{@code message}) is not client-invalid. The
 * SDK already retries those on live APIs; if one reaches KC it is terminal except on first-time
 * {@code .build()}: see {@link #isBodyless404}.
 */
public class ClientRecreationException extends RuntimeException {

  private static final Set<String> CLIENT_INVALID_ERROR_CODE_NAMES =
      Set.of(
          // Client invalidated by SDK (pipe failover, auth refresh failure, etc.)
          "InvalidClientError",
          // HTTP 410 on open_channel, insert_rows, get_channel_status, or pipe refresh
          "SfApiPipeFailedOverError",
          // Client was closed
          "ClosedClientError");

  /** GS fields as the Rust SDK prints them when Envoy left the JSON envelope empty. */
  private static final String UNENVELOPED_404_GS_FIELDS = "error_code=, message=,";

  /**
   * Constructs a new {@code ClientRecreationException} wrapping the given {@link SFException}.
   *
   * @param cause the SDK exception indicating the client is invalid
   */
  public ClientRecreationException(SFException cause) {
    super(
        "SDK client invalid: " + Preconditions.checkNotNull(cause, "cause").getErrorCodeName(),
        cause);
    Preconditions.checkArgument(
        isClientInvalidError(cause),
        "ClientRecreationException requires a client-invalid SFException, got: %s",
        cause.getErrorCodeName());
  }

  /**
   * Wraps the given throwable as a {@code ClientRecreationException} if it is a client-invalid
   * {@link SFException}. Avoids the need for callers to cast to {@code SFException} manually.
   *
   * @param e the exception to wrap
   * @return a new {@code ClientRecreationException} wrapping the cause
   * @throws IllegalArgumentException if {@code e} is not a client-invalid {@link SFException}
   */
  public static ClientRecreationException wrap(Throwable e) {
    Preconditions.checkArgument(
        isClientInvalidError(e),
        "Cannot wrap non-client-invalid exception: %s",
        e.getClass().getName());
    return new ClientRecreationException((SFException) e);
  }

  /**
   * Checks if the given throwable represents a client-level invalidation error that requires client
   * recreation.
   *
   * @param e the exception to check (may be null)
   * @return {@code true} if {@code e} is an {@link SFException} with a client-invalid error code
   *     name; {@code false} otherwise
   */
  public static boolean isClientInvalidError(Throwable e) {
    if (!(e instanceof SFException)) {
      return false;
    }
    return CLIENT_INVALID_ERROR_CODE_NAMES.contains(((SFException) e).getErrorCodeName());
  }

  /**
   * HTTP 404 with empty GS {@code error_code}/{@code message}. Matches empty {@link
   * SFException#getDetailMessage()} (unit tests) and the live FFI sentence {@code "... HTTP 404,
   * error_code=, message=, ..."}. A 404 with a GS message is not NR. Uses the original detail
   * because {@link SFException#getMessage()} is always decorated.
   */
  public static boolean isBodyless404(Throwable e) {
    if (!(e instanceof SFException)) {
      return false;
    }
    SFException sfException = (SFException) e;
    if (sfException.getHttpStatusCode() != 404) {
      return false;
    }
    String detail = sfException.getDetailMessage();
    return detail == null || detail.isEmpty() || detail.contains(UNENVELOPED_404_GS_FIELDS);
  }
}
