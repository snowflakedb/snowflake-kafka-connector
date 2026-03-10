package com.snowflake.kafka.connector.internal.validation;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Map;
import org.junit.jupiter.api.Test;

/** Tests for ValidationTelemetryEvent */
public class ValidationTelemetryEventTest {

  @Test
  public void testTelemetryEventCreation() {
    ValidationTelemetryEvent event =
        new ValidationTelemetryEvent(
            "channel1",
            "table1",
            "type_error",
            "col1",
            "INVALID_FORMAT_ROW",
            System.currentTimeMillis(),
            "Invalid value");

    assertEquals("channel1", event.getChannelName());
    assertEquals("table1", event.getTableName());
    assertEquals("type_error", event.getErrorType());
    assertEquals("col1", event.getColumnName());
    assertEquals("INVALID_FORMAT_ROW", event.getErrorCode());
    assertEquals("Invalid value", event.getErrorMessage());
  }

  @Test
  public void testToTelemetryPayload() {
    long timestamp = System.currentTimeMillis();
    ValidationTelemetryEvent event =
        new ValidationTelemetryEvent(
            "channel1",
            "table1",
            "structural_error",
            "col2",
            "MISSING_NOT_NULL",
            timestamp,
            "Missing required column");

    Map<String, Object> payload = event.toTelemetryPayload();

    assertEquals("validation_event", payload.get("event_type"));
    assertEquals("channel1", payload.get("channel_name"));
    assertEquals("table1", payload.get("table_name"));
    assertEquals("structural_error", payload.get("error_type"));
    assertEquals("col2", payload.get("column_name"));
    assertEquals("MISSING_NOT_NULL", payload.get("error_code"));
    assertEquals(timestamp, payload.get("timestamp"));
    assertEquals("Missing required column", payload.get("error_message"));
  }

  @Test
  public void testToStringDoesNotThrow() {
    ValidationTelemetryEvent event =
        new ValidationTelemetryEvent(
            "channel1",
            "table1",
            "schema_evolution",
            "new_col",
            "EXTRA_COLUMN",
            System.currentTimeMillis(),
            "Added new column");

    String str = event.toString();
    assertNotNull(str);
    assertTrue(str.contains("channel1"));
    assertTrue(str.contains("schema_evolution"));
  }
}
