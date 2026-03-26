package com.snowflake.kafka.connector.config;

import java.util.Locale;

/**
 * Determines the connector validation mode for data ingestion. Controls whether the connector
 * performs client-side validation before sending data to Snowflake.
 */
public enum SnowflakeValidation {

  /**
   * Client-side validation is enabled. The connector validates data types and schema compatibility
   * before sending to Snowflake. Validation errors can be routed to a DLQ or abort the task.
   */
  CLIENT_SIDE,

  /**
   * Server-side validation. Client-side validation is disabled. Invalid records are handled by the
   * SSv2 Error Table. Use when throughput is critical and an Error Table is configured.
   */
  SERVER_SIDE;

  /** Parses a config string into a validation mode, case-insensitive. */
  public static SnowflakeValidation fromConfig(String value) {
    if (value == null || value.trim().isEmpty()) {
      return CLIENT_SIDE;
    }
    return valueOf(value.trim().toUpperCase(Locale.ROOT));
  }
}
