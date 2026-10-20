package com.snowflake.kafka.connector.internal.streaming.v2;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.snowflake.kafka.connector.internal.streaming.channel.InflightUsage;
import javax.management.JMException;
import javax.management.ObjectName;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class ChannelInflightMetricsTest {

  private final FakeInflightMBeans beans = new FakeInflightMBeans();

  @AfterEach
  void tearDown() throws JMException {
    beans.close();
  }

  @Test
  void objectNameMatchesSdkLayout() throws Exception {
    assertEquals(
        new ObjectName(
            "snowflake.ingest.sdk:clientName=\"KC_CLIENT_1\",channel=\"conn_topic_0\","
                + "name=\"inflight_rows\""),
        ChannelInflightMetrics.objectName("KC_CLIENT_1", "conn_topic_0", "inflight_rows"));
  }

  @Test
  void readsRowsAndBytesForOneChannel() {
    beans.channel("client_a", "conn_topic_0", 7, 700);

    assertEquals(
        new InflightUsage(7, 700), ChannelInflightMetrics.read("client_a", "conn_topic_0"));
  }

  @Test
  void quotesNamesWithObjectNameSpecialCharacters() {
    String clientName = "client,with=special:chars*";
    String channelName = "conn_\"quoted\"_0";
    beans.channel(clientName, channelName, 3, 30);

    assertEquals(new InflightUsage(3, 30), ChannelInflightMetrics.read(clientName, channelName));
  }

  @Test
  void keepsChannelsOnDifferentClientsSeparate() {
    beans.channel("client_a", "conn_topic_0", 1, 10).channel("client_b", "conn_topic_0", 2, 20);

    assertEquals(new InflightUsage(1, 10), ChannelInflightMetrics.read("client_a", "conn_topic_0"));
    assertEquals(new InflightUsage(2, 20), ChannelInflightMetrics.read("client_b", "conn_topic_0"));
  }

  @Test
  void truncatesFractionalGaugeValues() {
    beans
        .gauge("client_a", "conn_topic_0", ChannelInflightMetrics.INFLIGHT_ROWS, 4.9)
        .gauge("client_a", "conn_topic_0", ChannelInflightMetrics.INFLIGHT_BYTES, 1024.0);

    assertEquals(
        new InflightUsage(4, 1024), ChannelInflightMetrics.read("client_a", "conn_topic_0"));
  }

  @Test
  void missingBeanFailsLoud() {
    ConnectException thrown =
        assertThrows(
            ConnectException.class,
            () -> ChannelInflightMetrics.read("missing_client", "missing_channel"));
    assertTrue(thrown.getMessage().contains("missing_client"), thrown.getMessage());
    assertTrue(thrown.getMessage().contains("missing_channel"), thrown.getMessage());
    assertTrue(
        thrown.getMessage().contains("snowflake.streaming.max.task.inflight.rows"),
        thrown.getMessage());
  }

  @Test
  void missingBytesBeanFailsLoudEvenWhenRowsBeanExists() {
    beans.gauge("client_a", "conn_topic_0", ChannelInflightMetrics.INFLIGHT_ROWS, 1);

    ConnectException thrown =
        assertThrows(
            ConnectException.class, () -> ChannelInflightMetrics.read("client_a", "conn_topic_0"));
    assertTrue(thrown.getMessage().contains("inflight_bytes"), thrown.getMessage());
  }
}
