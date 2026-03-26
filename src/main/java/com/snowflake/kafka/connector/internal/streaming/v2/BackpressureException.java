package com.snowflake.kafka.connector.internal.streaming.v2;

import com.google.common.base.Preconditions;
import com.snowflake.ingest.streaming.SFException;
import java.util.Set;

/**
 * Unchecked exception thrown when the Snowflake SDK signals backpressure due to memory saturation
 * or receiver overload.
 *
 * <p>This exception wraps {@link SFException} instances with specific error codes indicating
 * transient memory pressure. It signals to the batch-level insert loop that the current batch
 * should be abandoned and offsets should be rewound, but the channel remains valid and does not
 * need to be reopened.
 *
 * <p>Retryable error codes:
 *
 * <ul>
 *   <li>{@code ReceiverSaturated} - 429 Too Many Requests
 *   <li>{@code MemoryThresholdExceeded} - 429 Too Many Requests
 *   <li>{@code MemoryThresholdExceededInContainer} - 429 Too Many Requests
 *   <li>{@code HttpRetryableClientError} - 503 Service Unavailable
 * </ul>
 */
public class BackpressureException extends RuntimeException {

  private static final Set<String> RETRYABLE_ERROR_CODE_NAMES =
      Set.of(
          // 429 Too Many Requests
          "ReceiverSaturated",
          "MemoryThresholdExceeded",
          "MemoryThresholdExceededInContainer",
          // 503 Service Unavailable
          "HttpRetryableClientError");

  /**
   * Constructs a new {@code BackpressureException} wrapping the given {@link SFException}.
   *
   * @param cause the SDK exception indicating backpressure
   */
  public BackpressureException(SFException cause) {
    super("SDK backpressure: " + cause.getErrorCodeName(), cause);
    Preconditions.checkArgument(
        isRetryableError(cause),
        "BackpressureException requires a retryable SFException, got: %s",
        cause.getErrorCodeName());
  }

  /**
   * Checks if the given throwable represents a retryable backpressure error.
   *
   * @param e the exception to check (may be null)
   * @return {@code true} if {@code e} is an {@link SFException} with a retryable error code name;
   *     {@code false} otherwise
   */
  public static boolean isRetryableError(Throwable e) {
    if (!(e instanceof SFException)) {
      return false;
    }
    return RETRYABLE_ERROR_CODE_NAMES.contains(((SFException) e).getErrorCodeName());
  }
}
