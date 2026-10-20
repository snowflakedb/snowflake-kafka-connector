package com.snowflake.kafka.connector.internal.streaming.v2;

import com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams;
import com.snowflake.kafka.connector.internal.streaming.channel.InflightUsage;
import java.lang.management.ManagementFactory;
import javax.management.InstanceNotFoundException;
import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import org.apache.kafka.connect.errors.ConnectException;

/**
 * Reads the per-channel {@code inflight_rows} and {@code inflight_bytes} gauges that
 * snowpipe-streaming publishes on the platform MBeanServer. Each gauge counts what a channel has
 * accepted and Snowflake has not yet acknowledged. The SDK refreshes gauge values at most once per
 * second, so a read can be up to about one second stale.
 */
public final class ChannelInflightMetrics {

  static final String DOMAIN = "snowflake.ingest.sdk";
  static final String INFLIGHT_ROWS = "inflight_rows";
  static final String INFLIGHT_BYTES = "inflight_bytes";
  private static final String VALUE_ATTRIBUTE = "Value";

  private ChannelInflightMetrics() {}

  /**
   * Reads both gauges for the channel {@code channelName} opened on the SDK client {@code
   * clientName}.
   *
   * @throws ConnectException if either bean is missing or unreadable. The task caps would otherwise
   *     silently stop limiting.
   */
  public static InflightUsage read(String clientName, String channelName) {
    MBeanServer server = ManagementFactory.getPlatformMBeanServer();
    return new InflightUsage(
        readGauge(server, clientName, channelName, INFLIGHT_ROWS),
        readGauge(server, clientName, channelName, INFLIGHT_BYTES));
  }

  /** Builds the same ObjectName the SDK registers: every key value quoted. */
  static ObjectName objectName(String clientName, String channelName, String metricName) {
    String name =
        DOMAIN
            + ":clientName="
            + ObjectName.quote(clientName)
            + ",channel="
            + ObjectName.quote(channelName)
            + ",name="
            + ObjectName.quote(metricName);
    try {
      return new ObjectName(name);
    } catch (MalformedObjectNameException e) {
      throw new IllegalArgumentException("Invalid JMX ObjectName " + name, e);
    }
  }

  private static long readGauge(
      MBeanServer server, String clientName, String channelName, String metricName) {
    ObjectName name = objectName(clientName, channelName, metricName);
    final Object value;
    try {
      value = server.getAttribute(name, VALUE_ATTRIBUTE);
    } catch (InstanceNotFoundException e) {
      throw new ConnectException(
          String.format(
              "Snowpipe Streaming JMX bean %s is not registered for open channel %s on client %s."
                  + " %s and %s require a snowpipe-streaming version that publishes per-channel"
                  + " inflight JMX metrics. Set both to 0 or upgrade the connector.",
              name,
              channelName,
              clientName,
              KafkaConnectorConfigParams.SNOWFLAKE_STREAMING_MAX_TASK_INFLIGHT_ROWS,
              KafkaConnectorConfigParams.SNOWFLAKE_STREAMING_MAX_TASK_INFLIGHT_MEGABYTES),
          e);
    } catch (JMException e) {
      throw new ConnectException(
          "Failed to read JMX attribute " + VALUE_ATTRIBUTE + " of " + name, e);
    }
    if (!(value instanceof Number)) {
      throw new ConnectException(
          "JMX attribute " + VALUE_ATTRIBUTE + " of " + name + " is not numeric: " + value);
    }
    return ((Number) value).longValue();
  }
}
