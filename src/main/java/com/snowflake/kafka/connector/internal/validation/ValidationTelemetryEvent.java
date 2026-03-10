/*
 * Copyright (c) 2026 Snowflake Computing Inc. All rights reserved.
 */

package com.snowflake.kafka.connector.internal.validation;

import java.util.HashMap;
import java.util.Map;

/**
 * Telemetry event for validation operations. Captures validation failures, schema evolution
 * triggers, and other validation-related events for observability.
 *
 * <p>Immutable value object safe for concurrent use.
 */
public class ValidationTelemetryEvent {
  private final String channelName;
  private final String tableName;
  private final String errorType; // "type_error", "structural_error", "schema_evolution"
  private final String columnName;
  private final String errorCode;
  private final long timestamp;
  private final String errorMessage;

  public ValidationTelemetryEvent(
      String channelName,
      String tableName,
      String errorType,
      String columnName,
      String errorCode,
      long timestamp,
      String errorMessage) {
    this.channelName = channelName;
    this.tableName = tableName;
    this.errorType = errorType;
    this.columnName = columnName;
    this.errorCode = errorCode;
    this.timestamp = timestamp;
    this.errorMessage = errorMessage;
  }

  /**
   * Convert to telemetry payload for reporting.
   *
   * @return Map representation of telemetry event
   */
  public Map<String, Object> toTelemetryPayload() {
    Map<String, Object> payload = new HashMap<>();
    payload.put("event_type", "validation_event");
    payload.put("channel_name", channelName);
    payload.put("table_name", tableName);
    payload.put("error_type", errorType);
    payload.put("column_name", columnName);
    payload.put("error_code", errorCode);
    payload.put("timestamp", timestamp);
    payload.put("error_message", errorMessage);
    return payload;
  }

  public String getChannelName() {
    return channelName;
  }

  public String getTableName() {
    return tableName;
  }

  public String getErrorType() {
    return errorType;
  }

  public String getColumnName() {
    return columnName;
  }

  public String getErrorCode() {
    return errorCode;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public String getErrorMessage() {
    return errorMessage;
  }

  @Override
  public String toString() {
    return "ValidationTelemetryEvent{"
        + "channelName='"
        + channelName
        + '\''
        + ", tableName='"
        + tableName
        + '\''
        + ", errorType='"
        + errorType
        + '\''
        + ", columnName='"
        + columnName
        + '\''
        + ", errorCode='"
        + errorCode
        + '\''
        + ", timestamp="
        + timestamp
        + ", errorMessage='"
        + errorMessage
        + '\''
        + '}';
  }
}
