package com.snowflake.kafka.connector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.snowflake.kafka.connector.internal.SnowflakeConnectionService;
import com.snowflake.kafka.connector.internal.advisory.AdvisoryMessage;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.objenesis.Objenesis;
import org.objenesis.ObjenesisStd;

/**
 * Unit tests for {@link SnowflakeStreamingSinkConnector#hasCritical(List)} and periodic advisory
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

  private static Object invokePrivate(Object target, String methodName) throws Exception {
    Method method = SnowflakeStreamingSinkConnector.class.getDeclaredMethod(methodName);
    method.setAccessible(true);
    return method.invoke(target);
  }

  @Test
  public void hasCritical_emptyList_returnsFalse() {
    assertFalse(SnowflakeStreamingSinkConnector.hasCritical(Collections.emptyList()));
  }

  @Test
  public void hasCritical_onlyInfoAndWarn_returnsFalse() {
    List<AdvisoryMessage> advisories =
        Arrays.asList(
            new AdvisoryMessage("INFO", "all good"), new AdvisoryMessage("WARN", "heads up"));
    assertFalse(SnowflakeStreamingSinkConnector.hasCritical(advisories));
  }

  @Test
  public void hasCritical_containsCritical_returnsTrue() {
    List<AdvisoryMessage> advisories =
        Arrays.asList(
            new AdvisoryMessage("WARN", "heads up"),
            new AdvisoryMessage("CRITICAL", "you must upgrade"));
    assertTrue(SnowflakeStreamingSinkConnector.hasCritical(advisories));
  }

  @Test
  public void hasCritical_criticalCaseInsensitive_returnsTrue() {
    List<AdvisoryMessage> advisories =
        Collections.singletonList(new AdvisoryMessage("critical", "lower-case critical"));
    assertTrue(SnowflakeStreamingSinkConnector.hasCritical(advisories));
  }

  @Test
  public void hasCritical_unknownLevelDefaultsToWarn_returnsFalse() {
    // Unknown levels default to WARN, so they must not trigger the critical path.
    List<AdvisoryMessage> advisories =
        Collections.singletonList(new AdvisoryMessage("UNKNOWN_LEVEL", "some message"));
    assertFalse(SnowflakeStreamingSinkConnector.hasCritical(advisories));
  }

  @Test
  public void pollAdvisories_logsCriticalButDoesNotThrow() throws Exception {
    SnowflakeConnectionService conn = mock(SnowflakeConnectionService.class);
    when(conn.getKcAdvisoryMessages(anyString()))
        .thenReturn(Collections.singletonList(new AdvisoryMessage("CRITICAL", "you must upgrade")));
    SnowflakeStreamingSinkConnector connector = connectorWith(conn, new HashMap<>());

    // Mid-run polling must never throw, even when a CRITICAL advisory is returned.
    invokePrivate(connector, "pollAdvisories");

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
