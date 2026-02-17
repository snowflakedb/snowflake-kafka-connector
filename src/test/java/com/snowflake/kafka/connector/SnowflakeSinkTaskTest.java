package com.snowflake.kafka.connector;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import com.snowflake.kafka.connector.internal.SnowflakeConnectionService;
import com.snowflake.kafka.connector.internal.SnowflakeSinkService;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for SnowflakeSinkTask resource cleanup.
 *
 * <p>Issue #9: Verifies that task-level conn is closed in stop().
 */
class SnowflakeSinkTaskTest {

  @Test
  void stop_closesConnection() {
    // Given: a task with a mock sink service and mock connection
    SnowflakeSinkService mockSink = mock(SnowflakeSinkService.class);
    SnowflakeConnectionService mockConn = mock(SnowflakeConnectionService.class);

    SnowflakeSinkTask task = new SnowflakeSinkTask(mockSink, mockConn);

    // When: stop is called
    task.stop();

    // Then: both sink.stop() and conn.close() should be called
    verify(mockSink).stop();
    verify(mockConn).close();
  }

  @Test
  void stop_closesConnection_evenWhenSinkStopThrows() {
    // Given: a task whose sink.stop() throws
    SnowflakeSinkService mockSink = mock(SnowflakeSinkService.class);
    SnowflakeConnectionService mockConn = mock(SnowflakeConnectionService.class);
    doThrow(new RuntimeException("sink stop failed")).when(mockSink).stop();

    SnowflakeSinkTask task = new SnowflakeSinkTask(mockSink, mockConn);

    // When: stop is called
    task.stop();

    // Then: conn.close() should still be called despite sink.stop() failure
    verify(mockSink).stop();
    verify(mockConn).close();
  }

  @Test
  void stop_handlesCloseException() {
    // Given: a task whose conn.close() throws
    SnowflakeSinkService mockSink = mock(SnowflakeSinkService.class);
    SnowflakeConnectionService mockConn = mock(SnowflakeConnectionService.class);
    doThrow(new RuntimeException("close failed")).when(mockConn).close();

    SnowflakeSinkTask task = new SnowflakeSinkTask(mockSink, mockConn);

    // When/Then: stop should not throw
    task.stop();

    // And close was still attempted
    verify(mockConn).close();
  }

  @Test
  void stop_handlesNullSinkAndConnection() {
    // Given: a task with null sink and connection (edge case during early startup failure)
    SnowflakeSinkTask task = new SnowflakeSinkTask();

    // When/Then: stop should not throw
    task.stop();
  }
}
