package com.snowflake.kafka.connector.internal.streaming.v2;

/**
 * Unchecked exception signalling that a channel's inflight rows or bytes could not be read, other
 * than because the bean is missing: the SDK bean failed to read the channel's counters or returned
 * an invalid value. The usage is unknown, not zero.
 */
public final class InflightUsageUnavailableException extends RuntimeException {

  public InflightUsageUnavailableException(String message, Throwable cause) {
    super(message, cause);
  }
}
