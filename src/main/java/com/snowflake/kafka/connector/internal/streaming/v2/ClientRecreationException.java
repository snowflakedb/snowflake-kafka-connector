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
 *   <li>Body-less HTTP 404 - Envoy NR / no-route (empty error payload). T1 also returns this for an
 *       invalid account, so it may be a real config error; we treat it as client-invalid and let
 *       the create/recreate budget expire. A 404 with a Snowflake error detail is a real not-found
 *       and is not treated as client-invalid.
 * </ul>
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
   *     name, or a body-less HTTP 404; {@code false} otherwise
   */
  public static boolean isClientInvalidError(Throwable e) {
    if (!(e instanceof SFException)) {
      return false;
    }
    SFException sfException = (SFException) e;
    return CLIENT_INVALID_ERROR_CODE_NAMES.contains(sfException.getErrorCodeName())
        || isBodyless404(sfException);
  }

  /**
   * Envoy NR 404s have HTTP 404 and no Snowflake error payload. SSv2 1.4 leaves {@link
   * SFException#getMessage()} empty; 1.8 decorates an empty detail as {@code ErrorCode:} then
   * {@code (HTTP 404 ...)}.
   */
  private static boolean isBodyless404(SFException sfException) {
    String message = sfException.getMessage();
    return sfException.getHttpStatusCode() == 404
        && (message == null
            || message.isEmpty()
            || message.startsWith(sfException.getErrorCodeName() + ":  (HTTP 404"));
  }
}
