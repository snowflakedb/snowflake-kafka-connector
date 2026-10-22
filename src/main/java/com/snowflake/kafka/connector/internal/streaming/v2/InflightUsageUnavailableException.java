package com.snowflake.kafka.connector.internal.streaming.v2;

/**
 * Unchecked exception signalling that a channel's inflight rows or bytes could not be read for a
 * transient reason, such as the SDK gauge snapshot being unavailable, stale, or ambiguous. The
 * usage is unknown, not zero.
 */
public final class InflightUsageUnavailableException extends RuntimeException {

  public InflightUsageUnavailableException(String message, Throwable cause) {
    super(message, cause);
  }
}
