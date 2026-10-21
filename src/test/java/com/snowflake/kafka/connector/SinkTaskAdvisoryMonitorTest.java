package com.snowflake.kafka.connector;

import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.SNOWFLAKE_FEATURE_ADVISORY_POLL_INTERVAL_SECONDS;
import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.SNOWFLAKE_FEATURE_FAIL_ON_CRITICAL_ADVISORY;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.snowflake.kafka.connector.internal.SnowflakeConnectionService;
import com.snowflake.kafka.connector.internal.SnowflakeConnectionServiceFactory;
import com.snowflake.kafka.connector.internal.SnowflakeConnectionServiceFactory.SnowflakeConnectionServiceBuilder;
import com.snowflake.kafka.connector.internal.SnowflakeErrors;
import com.snowflake.kafka.connector.internal.SnowflakeKafkaConnectorException;
import com.snowflake.kafka.connector.internal.SnowflakeSinkService;
import com.snowflake.kafka.connector.internal.advisory.AdvisoryMessage;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

/** Unit tests for {@link SinkTaskAdvisoryMonitor} and its use in {@link SnowflakeSinkTask}. */
class SinkTaskAdvisoryMonitorTest {

  private static final List<AdvisoryMessage> CRITICAL =
      Collections.singletonList(new AdvisoryMessage("critical", "you must upgrade"));

  private static final List<SinkRecord> BATCH =
      Collections.singletonList(new SinkRecord("topic", 0, null, null, null, "value", 1));

  private static SnowflakeConnectionService connReturning(List<AdvisoryMessage> advisories) {
    SnowflakeConnectionService conn = mock(SnowflakeConnectionService.class);
    when(conn.getKcAdvisoryMessages(anyString())).thenReturn(advisories);
    return conn;
  }

  private static Map<String, String> config(String key, String value) {
    Map<String, String> config = new HashMap<>();
    config.put(key, value);
    return config;
  }

  private static void assertIs5031(ConnectException e) {
    assertEquals(
        SnowflakeErrors.ERROR_5031.getCode(),
        ((SnowflakeKafkaConnectorException) e.getCause()).getCode());
    assertTrue(
        e.getMessage().contains(SNOWFLAKE_FEATURE_FAIL_ON_CRITICAL_ADVISORY + "=false"),
        "the failure must say how to override it: " + e.getMessage());
  }

  @Test
  void criticalMidRun_nextPutThrowsAndDoesNotIngest() {
    SnowflakeSinkService sink = mock(SnowflakeSinkService.class);
    SinkTaskAdvisoryMonitor monitor =
        new SinkTaskAdvisoryMonitor(connReturning(CRITICAL), new HashMap<>(), "0");
    SnowflakeSinkTask task = new SnowflakeSinkTask(sink, null, monitor);

    monitor.poll();

    assertIs5031(assertThrows(ConnectException.class, () -> task.put(BATCH)));
    assertIs5031(assertThrows(ConnectException.class, () -> task.put(Collections.emptyList())));
    verify(sink, never()).insert(anyCollection());
  }

  @Test
  void optOut_neverChecksAndNeverThrows() {
    SnowflakeConnectionService conn = connReturning(CRITICAL);
    SinkTaskAdvisoryMonitor monitor =
        new SinkTaskAdvisoryMonitor(
            conn, config(SNOWFLAKE_FEATURE_FAIL_ON_CRITICAL_ADVISORY, "false"), "0");

    assertDoesNotThrow(monitor::checkOnStart);
    monitor.startPolling();

    assertNull(monitor.pollerForTest(), "with the opt-out the task does not poll");
    assertDoesNotThrow(monitor::throwIfCritical);
    verify(conn, never()).getKcAdvisoryMessages(anyString());
  }

  @Test
  void nonCritical_doesNotThrow() {
    SinkTaskAdvisoryMonitor monitor =
        new SinkTaskAdvisoryMonitor(
            connReturning(
                List.of(
                    new AdvisoryMessage("info", "fyi"),
                    new AdvisoryMessage("warn", "heads up"),
                    new AdvisoryMessage("error", "known bug"))),
            new HashMap<>(),
            "0");

    assertDoesNotThrow(monitor::checkOnStart);
    monitor.poll();
    assertDoesNotThrow(monitor::throwIfCritical);
  }

  @Test
  void fetchFailure_doesNotThrow() {
    SnowflakeConnectionService conn = mock(SnowflakeConnectionService.class);
    when(conn.getKcAdvisoryMessages(anyString())).thenThrow(new RuntimeException("boom"));
    SinkTaskAdvisoryMonitor monitor = new SinkTaskAdvisoryMonitor(conn, new HashMap<>(), "0");

    assertDoesNotThrow(monitor::checkOnStart);
    monitor.poll();
    assertDoesNotThrow(monitor::throwIfCritical);
  }

  @Test
  void criticalOnStart_taskStartThrows() {
    SnowflakeConnectionService conn = connReturning(CRITICAL);
    SnowflakeConnectionServiceBuilder builder =
        mock(SnowflakeConnectionServiceBuilder.class, RETURNS_SELF);
    when(builder.build()).thenReturn(conn);
    Map<String, String> config = new HashMap<>();
    config.put(Constants.KafkaConnectorConfigParams.NAME, "test_connector");
    config.put(Utils.TASK_ID, "0");

    try (MockedStatic<SnowflakeConnectionServiceFactory> factory =
        mockStatic(SnowflakeConnectionServiceFactory.class)) {
      factory.when(SnowflakeConnectionServiceFactory::builder).thenReturn(builder);
      SnowflakeSinkTask task = new SnowflakeSinkTask();

      assertIs5031(assertThrows(ConnectException.class, () -> task.start(config)));
      task.stop(); // Kafka Connect calls stop() after a failed start(); it must not throw
    }
  }

  @Test
  void criticalOnStart_optOut_doesNotThrow() {
    SinkTaskAdvisoryMonitor monitor =
        new SinkTaskAdvisoryMonitor(
            connReturning(CRITICAL),
            config(SNOWFLAKE_FEATURE_FAIL_ON_CRITICAL_ADVISORY, "false"),
            "0");
    assertDoesNotThrow(monitor::checkOnStart);
  }

  @Test
  void polling_firstPollWithinOneInterval_stopsPollerAndTaskStopShutsItDown() throws Exception {
    SnowflakeConnectionService conn = connReturning(CRITICAL);
    SinkTaskAdvisoryMonitor monitor =
        new SinkTaskAdvisoryMonitor(
            conn, config(SNOWFLAKE_FEATURE_ADVISORY_POLL_INTERVAL_SECONDS, "1"), "0");
    monitor.startPolling();

    // The jittered first poll fires within one interval; a critical result stops the poller.
    Thread.sleep(1500);
    assertThrows(ConnectException.class, monitor::throwIfCritical);
    assertTrue(monitor.pollerForTest().isShutdown());
  }

  @Test
  void stop_shutsDownPoller() {
    SnowflakeConnectionService conn = connReturning(Collections.emptyList());
    SinkTaskAdvisoryMonitor monitor =
        new SinkTaskAdvisoryMonitor(
            conn, config(SNOWFLAKE_FEATURE_ADVISORY_POLL_INTERVAL_SECONDS, "3600"), "0");
    SnowflakeSinkTask task = new SnowflakeSinkTask(mock(SnowflakeSinkService.class), conn, monitor);
    monitor.startPolling();

    task.stop();

    assertTrue(monitor.pollerForTest().isShutdown());
  }

  @Test
  void intervalNotPositive_noPoller() {
    SinkTaskAdvisoryMonitor monitor =
        new SinkTaskAdvisoryMonitor(
            connReturning(CRITICAL),
            config(SNOWFLAKE_FEATURE_ADVISORY_POLL_INTERVAL_SECONDS, "0"),
            "0");
    monitor.startPolling();
    assertNull(monitor.pollerForTest());
  }
}
