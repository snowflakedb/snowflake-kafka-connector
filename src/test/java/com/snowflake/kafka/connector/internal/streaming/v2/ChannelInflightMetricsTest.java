package com.snowflake.kafka.connector.internal.streaming.v2;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.snowflake.kafka.connector.internal.streaming.channel.InflightUsage;
import javax.management.InstanceNotFoundException;
import javax.management.JMException;
import javax.management.MBeanException;
import javax.management.ObjectName;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class ChannelInflightMetricsTest {

  private static final String ROWS_KEY = "snowflake.streaming.max.task.inflight.rows";

  private final FakeInflightMBeans beans = new FakeInflightMBeans();

  @AfterEach
  void tearDown() throws JMException {
    beans.close();
  }

  @Test
  void objectNameMatchesSdkLayoutLiteralForBothGauges() throws Exception {
    assertEquals(
        new ObjectName(
            "snowflake.ingest.sdk:clientName=\"KC_CLIENT_1\",channel=\"conn_topic_0\","
                + "name=\"inflight_rows\""),
        ChannelInflightMetrics.objectName(
            "KC_CLIENT_1", "conn_topic_0", ChannelInflightMetrics.INFLIGHT_ROWS));
    assertEquals(
        new ObjectName(
            "snowflake.ingest.sdk:clientName=\"KC_CLIENT_1\",channel=\"conn_topic_0\","
                + "name=\"inflight_bytes\""),
        ChannelInflightMetrics.objectName(
            "KC_CLIENT_1", "conn_topic_0", ChannelInflightMetrics.INFLIGHT_BYTES));
  }

  private static InflightUsage read(String clientName, String channelName)
      throws InstanceNotFoundException {
    return ChannelInflightMetrics.read(
        new ChannelInflightMetrics.InflightBeanNames(clientName, channelName));
  }

  @Test
  void readsRowsAndBytesForOneChannel() throws Exception {
    beans.channel("client_a", "conn_topic_0", 7, 700);

    assertEquals(new InflightUsage(7, 700), read("client_a", "conn_topic_0"));
  }

  @Test
  void readsOnlyTheRequestedClientAndChannel() throws Exception {
    beans
        .channel("client_a", "conn_topic_0", 1, 10)
        .channel("client_b", "conn_topic_0", 2, 20)
        .channel("client_a", "conn_topic_1", 3, 30)
        // Unquoted near-duplicate of client_a/conn_topic_0, a distinct ObjectName.
        .register(
            new ObjectName(
                "snowflake.ingest.sdk:clientName=client_a,channel=conn_topic_0,name=inflight_rows"),
            () -> 99.0)
        .register(
            new ObjectName(
                "snowflake.ingest.sdk:clientName=client_a,channel=conn_topic_0,name=inflight_bytes"),
            () -> 990.0);

    assertEquals(new InflightUsage(1, 10), read("client_a", "conn_topic_0"));
    assertEquals(new InflightUsage(2, 20), read("client_b", "conn_topic_0"));
    assertEquals(new InflightUsage(3, 30), read("client_a", "conn_topic_1"));
  }

  @Test
  void missingBeanThrowsInstanceNotFound() {
    InstanceNotFoundException thrown =
        assertThrows(
            InstanceNotFoundException.class, () -> read("missing_client", "missing_channel"));
    assertTrue(thrown.getMessage().contains("inflight_rows"), thrown.getMessage());

    beans.gauge("client_a", "conn_topic_0", FakeInflightMBeans.ROWS, 1);
    InstanceNotFoundException bytesMissing =
        assertThrows(InstanceNotFoundException.class, () -> read("client_a", "conn_topic_0"));
    assertTrue(bytesMissing.getMessage().contains("inflight_bytes"), bytesMissing.getMessage());
  }

  @Test
  void beanMBeanExceptionIsUnavailable() {
    MBeanException failure = new MBeanException(new Exception("ffi failure"), "ffi");
    beans
        .gauge("client_a", "conn_topic_0", FakeInflightMBeans.ROWS, 1)
        .register(
            FakeInflightMBeans.sdkObjectName("client_a", "conn_topic_0", FakeInflightMBeans.BYTES),
            () -> {
              throw failure;
            });

    InflightUsageUnavailableException thrown =
        assertThrows(
            InflightUsageUnavailableException.class, () -> read("client_a", "conn_topic_0"));
    assertSame(failure, thrown.getCause());
    assertTrue(thrown.getMessage().contains("inflight_bytes"), thrown.getMessage());
    assertTrue(thrown.getMessage().contains("conn_topic_0"), thrown.getMessage());
  }

  @ParameterizedTest
  @ValueSource(doubles = {Double.NaN, -1.0})
  void nonFiniteOrNegativeValueIsUnavailable(double value) {
    beans
        .gauge("client_a", "conn_topic_0", FakeInflightMBeans.ROWS, 1)
        .gauge("client_a", "conn_topic_0", FakeInflightMBeans.BYTES, value);

    InflightUsageUnavailableException thrown =
        assertThrows(
            InflightUsageUnavailableException.class, () -> read("client_a", "conn_topic_0"));
    assertTrue(thrown.getMessage().contains("inflight_bytes"), thrown.getMessage());
    assertTrue(thrown.getMessage().contains(String.valueOf(value)), thrown.getMessage());
  }

  @Test
  void sdkSupportNotRequiredWhenBothCapsDisabled() {
    assertDoesNotThrow(() -> ChannelInflightMetrics.requireSdkSupport(0, 0, "no.such.SdkClass"));
  }

  @Test
  void sdkSupportFailsWhenRowsCapEnabledAndSdkLacksJmxMetrics() {
    ConnectException thrown =
        assertThrows(
            ConnectException.class,
            () -> ChannelInflightMetrics.requireSdkSupport(1, 0, "no.such.SdkClass"));
    assertTrue(thrown.getMessage().contains(ROWS_KEY), thrown.getMessage());
    assertTrue(thrown.getMessage().contains("no.such.SdkClass"), thrown.getMessage());
    assertInstanceOf(ClassNotFoundException.class, thrown.getCause());
  }
}
