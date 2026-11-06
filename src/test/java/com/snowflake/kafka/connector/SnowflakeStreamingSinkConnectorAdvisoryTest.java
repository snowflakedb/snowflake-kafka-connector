package com.snowflake.kafka.connector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.snowflake.kafka.connector.internal.SnowflakeConnectionService;
import com.snowflake.kafka.connector.internal.advisory.AdvisoryMessage;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.connect.connector.Connector;
import org.apache.kafka.connect.connector.ConnectorContext;
import org.apache.kafka.connect.sink.SinkConnectorContext;
import org.junit.jupiter.api.Test;
import org.objenesis.Objenesis;
import org.objenesis.ObjenesisStd;

/**
 * Unit tests for the startup advisory check ({@code logServerAdvisories}) and periodic advisory
 * polling ({@code pollAdvisories}/{@code startAdvisoryPolling}).
 */
public class SnowflakeStreamingSinkConnectorAdvisoryTest {

  /**
   * Builds a connector instance without going through the no-arg constructor's side effects,
   * reflectively wiring in the mocked connection service and config map fields that {@code
   * pollAdvisories}/{@code startAdvisoryPolling} read.
   */
  private static SnowflakeStreamingSinkConnector connectorWith(
      SnowflakeConnectionService conn, Map<String, String> config) throws Exception {
    Objenesis objenesis = new ObjenesisStd();
    SnowflakeStreamingSinkConnector connector =
        objenesis.newInstance(SnowflakeStreamingSinkConnector.class);
    setField(connector, "conn", conn);
    setField(connector, "config", config);
    // Objenesis skips field initializers, so the AtomicInteger counter must be wired manually.
    setField(connector, "advisoryPollCount", new AtomicInteger());
    return connector;
  }

  private static void setField(Object target, String fieldName, Object value) throws Exception {
    Field field = SnowflakeStreamingSinkConnector.class.getDeclaredField(fieldName);
    field.setAccessible(true);
    field.set(target, value);
  }

  /** Wires the framework-supplied context (declared on the {@link Connector} superclass). */
  private static void setContext(Object target, ConnectorContext ctx) throws Exception {
    Field field = Connector.class.getDeclaredField("context");
    field.setAccessible(true);
    field.set(target, ctx);
  }

  private static Object invokePrivate(Object target, String methodName) throws Exception {
    Method method = SnowflakeStreamingSinkConnector.class.getDeclaredMethod(methodName);
    method.setAccessible(true);
    return method.invoke(target);
  }

  @Test
  public void logServerAdvisories_critical_starts() throws Exception {
    SnowflakeConnectionService conn = mock(SnowflakeConnectionService.class);
    when(conn.getKcAdvisoryMessages(anyString()))
        .thenReturn(Collections.singletonList(new AdvisoryMessage("critical", "you must upgrade")));
    SnowflakeStreamingSinkConnector connector = connectorWith(conn, new HashMap<>());

    invokePrivate(connector, "logServerAdvisories"); // must not throw
  }

  @Test
  public void logServerAdvisories_nonCritical_starts() throws Exception {
    SnowflakeConnectionService conn = mock(SnowflakeConnectionService.class);
    when(conn.getKcAdvisoryMessages(anyString()))
        .thenReturn(
            Arrays.asList(
                new AdvisoryMessage("info", "fyi"),
                new AdvisoryMessage("warn", "heads up"),
                new AdvisoryMessage("error", "known bug")));
    SnowflakeStreamingSinkConnector connector = connectorWith(conn, new HashMap<>());

    invokePrivate(connector, "logServerAdvisories"); // must not throw
  }

  @Test
  public void logServerAdvisories_fetchThrows_starts() throws Exception {
    SnowflakeConnectionService conn = mock(SnowflakeConnectionService.class);
    when(conn.getKcAdvisoryMessages(anyString())).thenThrow(new RuntimeException("boom"));
    SnowflakeStreamingSinkConnector connector = connectorWith(conn, new HashMap<>());

    invokePrivate(connector, "logServerAdvisories"); // must not throw
  }

  @Test
  public void pollAdvisories_criticalMidRun_doesNotStopConnector() throws Exception {
    SnowflakeConnectionService conn = mock(SnowflakeConnectionService.class);
    when(conn.getKcAdvisoryMessages(anyString()))
        .thenReturn(Collections.singletonList(new AdvisoryMessage("critical", "you must upgrade")));
    SnowflakeStreamingSinkConnector connector = connectorWith(conn, new HashMap<>());
    SinkConnectorContext ctx = mock(SinkConnectorContext.class);
    setContext(connector, ctx);
    ScheduledExecutorService poller = Executors.newSingleThreadScheduledExecutor();
    setField(connector, "advisoryPoller", poller);

    invokePrivate(connector, "pollAdvisories");
    invokePrivate(connector, "pollAdvisories");

    verify(ctx, never()).raiseError(any(Exception.class));
    assertFalse(poller.isShutdown(), "a critical advisory must not stop the poller");
    assertEquals(2, connector.advisoryPollCountForTest());
    poller.shutdownNow();
  }

  @Test
  public void startAdvisoryPolling_criticalMidRun_keepsPolling() throws Exception {
    SnowflakeConnectionService conn = mock(SnowflakeConnectionService.class);
    when(conn.getKcAdvisoryMessages(anyString()))
        .thenReturn(Collections.singletonList(new AdvisoryMessage("critical", "you must upgrade")));
    Map<String, String> config = new HashMap<>();
    config.put(
        Constants.KafkaConnectorConfigParams.SNOWFLAKE_FEATURE_ADVISORY_POLL_INTERVAL_SECONDS, "1");
    SnowflakeStreamingSinkConnector connector = connectorWith(conn, config);
    SinkConnectorContext ctx = mock(SinkConnectorContext.class);
    setContext(connector, ctx);

    invokePrivate(connector, "startAdvisoryPolling");
    // Long enough for at least 2 cycles at a 1s interval.
    Thread.sleep(2300);

    verify(ctx, never()).raiseError(any(Exception.class));
    assertTrue(
        connector.advisoryPollCountForTest() >= 2,
        "expected at least 2 poll cycles, got " + connector.advisoryPollCountForTest());
    Field pollerField = SnowflakeStreamingSinkConnector.class.getDeclaredField("advisoryPoller");
    pollerField.setAccessible(true);
    ScheduledExecutorService poller = (ScheduledExecutorService) pollerField.get(connector);
    assertFalse(poller.isShutdown(), "a critical advisory must not stop the poller");

    connector.stop();
  }

  @Test
  public void pollAdvisories_nonCriticalMidRun_keepsRunning() throws Exception {
    SnowflakeConnectionService conn = mock(SnowflakeConnectionService.class);
    when(conn.getKcAdvisoryMessages(anyString()))
        .thenReturn(
            Arrays.asList(
                new AdvisoryMessage("info", "fyi"),
                new AdvisoryMessage("warn", "heads up"),
                new AdvisoryMessage("error", "known bug")));
    SnowflakeStreamingSinkConnector connector = connectorWith(conn, new HashMap<>());
    SinkConnectorContext ctx = mock(SinkConnectorContext.class);
    setContext(connector, ctx);

    invokePrivate(connector, "pollAdvisories");

    verify(ctx, never()).raiseError(any(Exception.class));
    assertEquals(1, connector.advisoryPollCountForTest());
  }

  @Test
  public void pollAdvisories_fetchThrows_countsCycleAndDoesNotThrow() throws Exception {
    SnowflakeConnectionService conn = mock(SnowflakeConnectionService.class);
    when(conn.getKcAdvisoryMessages(anyString())).thenThrow(new RuntimeException("boom"));
    SnowflakeStreamingSinkConnector connector = connectorWith(conn, new HashMap<>());
    SinkConnectorContext ctx = mock(SinkConnectorContext.class);
    setContext(connector, ctx);

    invokePrivate(connector, "pollAdvisories");

    verify(ctx, never()).raiseError(any(Exception.class));
    assertEquals(1, connector.advisoryPollCountForTest());
  }

  @Test
  public void startAdvisoryPolling_disabledWhenIntervalNotPositive() throws Exception {
    SnowflakeConnectionService conn = mock(SnowflakeConnectionService.class);
    Map<String, String> config = new HashMap<>();
    config.put(
        Constants.KafkaConnectorConfigParams.SNOWFLAKE_FEATURE_ADVISORY_POLL_INTERVAL_SECONDS, "0");
    SnowflakeStreamingSinkConnector connector = connectorWith(conn, config);

    invokePrivate(connector, "startAdvisoryPolling");

    Field pollerField = SnowflakeStreamingSinkConnector.class.getDeclaredField("advisoryPoller");
    pollerField.setAccessible(true);
    assertNull(pollerField.get(connector));
  }

  @Test
  public void startAdvisoryPolling_scheduledTaskFiresRepeatedly() throws Exception {
    SnowflakeConnectionService conn = mock(SnowflakeConnectionService.class);
    when(conn.getKcAdvisoryMessages(anyString())).thenReturn(Collections.emptyList());
    Map<String, String> config = new HashMap<>();
    config.put(
        Constants.KafkaConnectorConfigParams.SNOWFLAKE_FEATURE_ADVISORY_POLL_INTERVAL_SECONDS, "1");
    SnowflakeStreamingSinkConnector connector = connectorWith(conn, config);

    invokePrivate(connector, "startAdvisoryPolling");
    Thread.sleep(2300);

    assertTrue(
        connector.advisoryPollCountForTest() >= 2,
        "expected at least 2 poll cycles, got " + connector.advisoryPollCountForTest());

    connector.stop();

    Field pollerField = SnowflakeStreamingSinkConnector.class.getDeclaredField("advisoryPoller");
    pollerField.setAccessible(true);
    ScheduledExecutorService poller = (ScheduledExecutorService) pollerField.get(connector);
    assertTrue(poller.isShutdown());
  }
}
