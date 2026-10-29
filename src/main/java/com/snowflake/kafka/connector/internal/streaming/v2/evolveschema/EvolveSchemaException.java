package com.snowflake.kafka.connector.internal.streaming.v2.evolveschema;

/**
 * An evolve-schema call that failed. Either Snowflake answered with a non-2xx status, or the call
 * could not complete (I/O error, retries exhausted, credential failure), in which case {@link
 * #getStatusCode()} is {@link #NO_STATUS}.
 */
public class EvolveSchemaException extends RuntimeException {

  public static final int NO_STATUS = -1;

  private final int statusCode;
  private final String errorCode;

  public EvolveSchemaException(int statusCode, String errorCode, String message) {
    super(message);
    this.statusCode = statusCode;
    this.errorCode = errorCode == null ? "" : errorCode;
  }

  public EvolveSchemaException(String message, Throwable cause) {
    super(message, cause);
    this.statusCode = NO_STATUS;
    this.errorCode = "";
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
}
