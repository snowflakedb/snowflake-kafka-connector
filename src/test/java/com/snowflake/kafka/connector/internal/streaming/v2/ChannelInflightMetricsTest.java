package com.snowflake.kafka.connector.internal.streaming.v2;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.snowflake.kafka.connector.internal.streaming.channel.InflightUsage;
import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.JMException;
import javax.management.MBeanException;
import javax.management.ObjectName;
import javax.management.RuntimeMBeanException;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class ChannelInflightMetricsTest {

  private static final String ROWS_KEY = "snowflake.streaming.max.task.inflight.rows";
  private static final String MEGABYTES_KEY = "snowflake.streaming.max.task.inflight.megabytes";

  private final FakeInflightMBeans beans = new FakeInflightMBeans();

  @AfterEach
  void tearDown() throws JMException {
    beans.close();
  }

  @Test
  void constantsMatchSdkGaugeNames() {
    assertEquals("snowflake.ingest.sdk", ChannelInflightMetrics.DOMAIN);
    assertEquals("inflight_rows", ChannelInflightMetrics.INFLIGHT_ROWS);
    assertEquals("inflight_bytes", ChannelInflightMetrics.INFLIGHT_BYTES);
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

  @Test
  void objectNameEscapesSpecialCharactersLikeSdkLiteral() throws Exception {
    // Hand-escaped: quote() wraps in double quotes and escapes '"', '*', '?', '\' and newline.
    assertEquals(
        new ObjectName(
            "snowflake.ingest.sdk:clientName=\"a,b=c:d\\*\\?\",channel=\"conn_\\\"q\\\"_\\\\0\","
                + "name=\"inflight_rows\""),
        ChannelInflightMetrics.objectName(
            "a,b=c:d*?", "conn_\"q\"_\\0", ChannelInflightMetrics.INFLIGHT_ROWS));
  }

  @Test
  void readsRowsAndBytesForOneChannel() throws Exception {
    beans.channel("client_a", "conn_topic_0", 7, 700);

    assertEquals(
        new InflightUsage(7, 700), ChannelInflightMetrics.read("client_a", "conn_topic_0"));
  }

  @ParameterizedTest
  @ValueSource(strings = {"client,with=special:chars*", "c?\\x", "line\nbreak"})
  void readsBeansRegisteredUnderSdkNameWithSpecialCharacters(String clientName) throws Exception {
    String channelName = "conn_\"quoted\"_0";
    beans.channel(clientName, channelName, 3, 30);

    assertEquals(new InflightUsage(3, 30), ChannelInflightMetrics.read(clientName, channelName));
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

    assertEquals(new InflightUsage(1, 10), ChannelInflightMetrics.read("client_a", "conn_topic_0"));
    assertEquals(new InflightUsage(2, 20), ChannelInflightMetrics.read("client_b", "conn_topic_0"));
    assertEquals(new InflightUsage(3, 30), ChannelInflightMetrics.read("client_a", "conn_topic_1"));
  }

  @Test
  void missingBeanThrowsInstanceNotFound() {
    InstanceNotFoundException thrown =
        assertThrows(
            InstanceNotFoundException.class,
            () -> ChannelInflightMetrics.read("missing_client", "missing_channel"));
    assertTrue(thrown.getMessage().contains("inflight_rows"), thrown.getMessage());
  }

  @Test
  void missingBytesBeanThrowsInstanceNotFoundEvenWhenRowsBeanExists() {
    beans.gauge("client_a", "conn_topic_0", FakeInflightMBeans.ROWS, 1);

    InstanceNotFoundException thrown =
        assertThrows(
            InstanceNotFoundException.class,
            () -> ChannelInflightMetrics.read("client_a", "conn_topic_0"));
    assertTrue(thrown.getMessage().contains("inflight_bytes"), thrown.getMessage());
  }

  @Test
  void missingBeanForOpenChannelNamesBeanChannelClientKeysAndRegistrationLogs() {
    InstanceNotFoundException cause = new InstanceNotFoundException("bean");

    ConnectException thrown =
        ChannelInflightMetrics.missingBeanForOpenChannel("client_a", "conn_topic_0", cause);

    assertSame(cause, thrown.getCause());
    String message = thrown.getMessage();
    assertTrue(message.contains("client_a"), message);
    assertTrue(message.contains("conn_topic_0"), message);
    assertTrue(message.contains(ROWS_KEY), message);
    assertTrue(message.contains(MEGABYTES_KEY), message);
    assertTrue(message.contains("registration"), message);
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
            InflightUsageUnavailableException.class,
            () -> ChannelInflightMetrics.read("client_a", "conn_topic_0"));
    assertSame(failure, thrown.getCause());
    assertTrue(thrown.getMessage().contains("inflight_bytes"), thrown.getMessage());
    assertTrue(thrown.getMessage().contains("conn_topic_0"), thrown.getMessage());
  }

  @Test
  void beanRuntimeExceptionIsUnavailable() {
    IllegalStateException failure = new IllegalStateException("ambiguous series");
    beans.register(
        FakeInflightMBeans.sdkObjectName("client_a", "conn_topic_0", FakeInflightMBeans.ROWS),
        () -> {
          throw failure;
        });

    InflightUsageUnavailableException thrown =
        assertThrows(
            InflightUsageUnavailableException.class,
            () -> ChannelInflightMetrics.read("client_a", "conn_topic_0"));
    RuntimeMBeanException wrapped =
        assertInstanceOf(RuntimeMBeanException.class, thrown.getCause());
    assertSame(failure, wrapped.getCause());
  }

  @Test
  void beanWithoutValueAttributeIsUnavailable() {
    beans.register(
        FakeInflightMBeans.sdkObjectName("client_a", "conn_topic_0", FakeInflightMBeans.ROWS),
        () -> {
          throw new AttributeNotFoundException("Value");
        });

    InflightUsageUnavailableException thrown =
        assertThrows(
            InflightUsageUnavailableException.class,
            () -> ChannelInflightMetrics.read("client_a", "conn_topic_0"));
    assertInstanceOf(AttributeNotFoundException.class, thrown.getCause());
  }

  @ParameterizedTest
  @ValueSource(
      doubles = {Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, -1.0, -0.5})
  void nonFiniteOrNegativeValueIsUnavailable(double value) {
    beans
        .gauge("client_a", "conn_topic_0", FakeInflightMBeans.ROWS, 1)
        .gauge("client_a", "conn_topic_0", FakeInflightMBeans.BYTES, value);

    InflightUsageUnavailableException thrown =
        assertThrows(
            InflightUsageUnavailableException.class,
            () -> ChannelInflightMetrics.read("client_a", "conn_topic_0"));
    assertTrue(thrown.getMessage().contains("inflight_bytes"), thrown.getMessage());
    assertTrue(thrown.getMessage().contains(String.valueOf(value)), thrown.getMessage());
  }

  @Test
  void nonNumericValueFailsLoud() {
    beans.register(
        FakeInflightMBeans.sdkObjectName("client_a", "conn_topic_0", FakeInflightMBeans.ROWS),
        () -> "12");

    ConnectException thrown =
        assertThrows(
            ConnectException.class, () -> ChannelInflightMetrics.read("client_a", "conn_topic_0"));
    assertEquals(ConnectException.class, thrown.getClass());
    assertTrue(thrown.getMessage().contains("not numeric"), thrown.getMessage());
  }

  @Test
  void sdkSupportNotRequiredWhenBothCapsDisabled() {
    assertDoesNotThrow(() -> ChannelInflightMetrics.requireSdkSupport(0, 0, "no.such.SdkClass"));
  }

  @ParameterizedTest
  @ValueSource(strings = {"rows", "bytes"})
  void sdkSupportFailsWhenEitherCapEnabledAndSdkLacksJmxMetrics(String cap) {
    long rows = cap.equals("rows") ? 1 : 0;
    long bytes = cap.equals("bytes") ? 1 : 0;

    ConnectException thrown =
        assertThrows(
            ConnectException.class,
            () -> ChannelInflightMetrics.requireSdkSupport(rows, bytes, "no.such.SdkClass"));
    assertTrue(thrown.getMessage().contains(ROWS_KEY), thrown.getMessage());
    assertTrue(thrown.getMessage().contains(MEGABYTES_KEY), thrown.getMessage());
    assertTrue(thrown.getMessage().contains("no.such.SdkClass"), thrown.getMessage());
    assertInstanceOf(ClassNotFoundException.class, thrown.getCause());
  }

  @Test
  void sdkSupportPassesWhenSdkClassIsLoadable() {
    assertDoesNotThrow(
        () ->
            ChannelInflightMetrics.requireSdkSupport(1, 1, ChannelInflightMetrics.class.getName()));
  }

  @Test
  void bundledSdkFailsCapabilityCheck() {
    // The pinned snowpipe-streaming 1.9.0 predates the per-channel inflight JMX beans. Flip this
    // test when the pom moves to a release that ships com.snowflake.ingest.streaming.JmxMetrics.
    assertEquals("com.snowflake.ingest.streaming.JmxMetrics", ChannelInflightMetrics.SDK_JMX_CLASS);
    assertThrows(ConnectException.class, () -> ChannelInflightMetrics.requireSdkSupport(1, 0));
  }
}
