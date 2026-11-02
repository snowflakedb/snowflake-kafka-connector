package com.snowflake.kafka.connector.internal.streaming.v2.evolveschema;

import java.util.Set;

/**
 * An evolve-schema call that failed. Either Snowflake answered with a non-2xx status, or the call
 * could not complete (I/O error, retries exhausted, credential failure), in which case {@link
 * #getStatusCode()} is {@link #NO_STATUS}.
 */
public class EvolveSchemaException extends RuntimeException {

  public static final int NO_STATUS = -1;

  /** Error code of the 403 Snowflake returns when evolve-schema is off for the account. */
  public static final String ERR_EVOLVE_SCHEMA_NOT_ENABLED = "ERR_EVOLVE_SCHEMA_NOT_ENABLED";

  private static final int HTTP_FORBIDDEN = 403;
  private static final Set<Integer> TRANSIENT_STATUSES = Set.of(429, 500, 502, 503, 504);

  private final int statusCode;
  private final String errorCode;
  private final boolean retryable;

  public EvolveSchemaException(int statusCode, String errorCode, String message) {
    super(message);
    this.statusCode = statusCode;
    this.errorCode = errorCode == null ? "" : errorCode;
    this.retryable = isTransientStatus(statusCode);
  }

  public EvolveSchemaException(String message, Throwable cause) {
    this(message, cause, false);
  }

  /**
   * @param retryable whether the same call may succeed later, e.g. after an I/O error
   */
  public EvolveSchemaException(String message, Throwable cause, boolean retryable) {
    super(message, cause);
    this.statusCode = NO_STATUS;
    this.errorCode = "";
    this.retryable = retryable;
  }

  /** HTTP status Snowflake returned, or {@link #NO_STATUS}. */
  public int getStatusCode() {
    return statusCode;
  }

  /**
   * Snowflake error code from the response body, e.g. {@code
   * ERR_EVOLVE_SCHEMA_INCOMPATIBLE_COLUMN_TYPE}. Empty when the body carried none.
   */
  public String getErrorCode() {
    return errorCode;
  }

  /** Whether a call answered with {@code statusCode} may succeed if repeated. */
  static boolean isTransientStatus(int statusCode) {
    return TRANSIENT_STATUSES.contains(statusCode);
  }

  /**
   * Whether the same call may succeed later: a 429, 500, 502, 503, or 504 status, or an I/O error,
   * after the client's own retries.
   */
  public boolean isRetryable() {
    return retryable;
  }

  /** Whether Snowflake answered that evolve-schema is off for the account. */
  public boolean isNotEnabled() {
    return statusCode == HTTP_FORBIDDEN && ERR_EVOLVE_SCHEMA_NOT_ENABLED.equals(errorCode);
  }
}
