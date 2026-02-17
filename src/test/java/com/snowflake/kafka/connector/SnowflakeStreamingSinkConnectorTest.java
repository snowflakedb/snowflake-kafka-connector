package com.snowflake.kafka.connector;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import com.snowflake.kafka.connector.internal.SnowflakeConnectionService;
import com.snowflake.kafka.connector.internal.telemetry.SnowflakeTelemetryService;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for SnowflakeStreamingSinkConnector resource cleanup.
 *
 * <p>Issue #8: Verifies that connector-level conn is closed in stop().
 */
class SnowflakeStreamingSinkConnectorTest {

  @Test
  void stop_closesConnection() {
    // Given: a connector with a mock connection
    SnowflakeConnectionService mockConn = mock(SnowflakeConnectionService.class);
    SnowflakeStreamingSinkConnector connector = new SnowflakeStreamingSinkConnector(mockConn, null);

    // When
    connector.stop();

    // Then
    verify(mockConn).close();
  }

  @Test
  void stop_handlesCloseException() {
    // Given: a connector whose connection throws on close
    SnowflakeConnectionService mockConn = mock(SnowflakeConnectionService.class);
    doThrow(new RuntimeException("close failed")).when(mockConn).close();
    SnowflakeStreamingSinkConnector connector = new SnowflakeStreamingSinkConnector(mockConn, null);

    // When/Then: stop should not throw
    connector.stop();

    // And close was still attempted
    verify(mockConn).close();
  }

  @Test
  void stop_handlesNullConnection() {
    // Given: a connector that was never started
    SnowflakeStreamingSinkConnector connector = new SnowflakeStreamingSinkConnector();

    // When/Then: stop should not throw
    connector.stop();
  }

  @Test
  void stop_reportsTelemetryAndClosesConnection() {
    // Given: a connector with both telemetry and connection
    SnowflakeConnectionService mockConn = mock(SnowflakeConnectionService.class);
    SnowflakeTelemetryService mockTelemetry = mock(SnowflakeTelemetryService.class);
    SnowflakeStreamingSinkConnector connector =
        new SnowflakeStreamingSinkConnector(mockConn, mockTelemetry);

    // When
    connector.stop();

    // Then: both telemetry and conn.close() should be called
    verify(mockTelemetry).reportKafkaConnectStop(0L);
    verify(mockConn).close();
  }
}
