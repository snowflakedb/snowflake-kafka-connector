package com.snowflake.kafka.connector.internal.streaming.v2;

import com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams;
import com.snowflake.kafka.connector.internal.streaming.channel.InflightUsage;
import java.lang.management.ManagementFactory;
import javax.management.InstanceNotFoundException;
import javax.management.JMException;
import javax.management.JMRuntimeException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import org.apache.kafka.connect.errors.ConnectException;

/**
 * Reads the per-channel {@code inflight_rows} and {@code inflight_bytes} gauges that
 * snowpipe-streaming publishes on the platform MBeanServer. Each gauge counts what a channel has
 * accepted and Snowflake has not yet acknowledged.
 *
 * <p>Contract of the SDK beans: every {@code Value} read returns the exact current value of that
 * channel instance at read time, or throws (channel gone, FFI failure). A limit checked against
 * these values before appending can therefore be exceeded by at most what is appended after the
 * check.
 */
public final class ChannelInflightMetrics {

  static final String DOMAIN = "snowflake.ingest.sdk";
  static final String INFLIGHT_ROWS = "inflight_rows";
  static final String INFLIGHT_BYTES = "inflight_bytes";
  private static final String VALUE_ATTRIBUTE = "Value";

  /** SDK class that registers the per-channel inflight beans. */
  static final String SDK_JMX_CLASS = "com.snowflake.ingest.streaming.JmxMetrics";

  private ChannelInflightMetrics() {}

  /**
   * Fails when either task inflight cap is enabled and the snowpipe-streaming SDK on the classpath
   * does not publish per-channel inflight beans.
   *
   * @throws ConnectException naming both cap config keys
   */
  public static void requireSdkSupport(long maxRows, long maxBytes) {
    requireSdkSupport(maxRows, maxBytes, SDK_JMX_CLASS);
  }

  static void requireSdkSupport(long maxRows, long maxBytes, String sdkClassName) {
    if (maxRows <= 0 && maxBytes <= 0) {
      return;
    }
    try {
      Class.forName(sdkClassName, false, ChannelInflightMetrics.class.getClassLoader());
    } catch (ClassNotFoundException e) {
      throw new ConnectException(
          String.format(
              "%s and %s require a snowpipe-streaming SDK that publishes per-channel inflight JMX"
                  + " beans, but %s is not on the classpath. Set both to 0 or run a connector"
                  + " version bundling such an SDK.",
              KafkaConnectorConfigParams.SNOWFLAKE_STREAMING_MAX_TASK_INFLIGHT_ROWS,
              KafkaConnectorConfigParams.SNOWFLAKE_STREAMING_MAX_TASK_INFLIGHT_MEGABYTES,
              sdkClassName),
          e);
    }
  }

  /**
   * Reads both gauges for the channel {@code channelName} opened on the SDK client {@code
   * clientName}.
   *
   * @throws InstanceNotFoundException if either bean is not registered
   * @throws InflightUsageUnavailableException if a bean fails to produce a value, or produces a
   *     negative or non-finite one
   * @throws ConnectException if a bean produces a non-numeric value
   */
  public static InflightUsage read(String clientName, String channelName)
      throws InstanceNotFoundException {
    MBeanServer server = ManagementFactory.getPlatformMBeanServer();
    return new InflightUsage(
        readGauge(server, clientName, channelName, INFLIGHT_ROWS),
        readGauge(server, clientName, channelName, INFLIGHT_BYTES));
  }

  /** The failure for a bean that is missing while its channel and client are open. */
  public static ConnectException missingBeanForOpenChannel(
      String clientName, String channelName, InstanceNotFoundException cause) {
    return new ConnectException(
        String.format(
            "Snowpipe Streaming inflight JMX bean %s is not registered for open channel %s on"
                + " client %s. %s and %s need these beans. Check the worker log for"
                + " snowpipe-streaming JMX bean registration warnings, and confirm the bundled"
                + " snowpipe-streaming version publishes per-channel inflight beans, or set both"
                + " to 0.",
            cause.getMessage(),
            channelName,
            clientName,
            KafkaConnectorConfigParams.SNOWFLAKE_STREAMING_MAX_TASK_INFLIGHT_ROWS,
            KafkaConnectorConfigParams.SNOWFLAKE_STREAMING_MAX_TASK_INFLIGHT_MEGABYTES),
        cause);
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
      MBeanServer server, String clientName, String channelName, String metricName)
      throws InstanceNotFoundException {
    ObjectName name = objectName(clientName, channelName, metricName);
    final Object value;
    try {
      value = server.getAttribute(name, VALUE_ATTRIBUTE);
    } catch (InstanceNotFoundException e) {
      throw e;
    } catch (JMException | JMRuntimeException e) {
      throw new InflightUsageUnavailableException(
          "Failed to read JMX attribute " + VALUE_ATTRIBUTE + " of " + name, e);
    }
    if (!(value instanceof Number)) {
      throw new ConnectException(
          "JMX attribute " + VALUE_ATTRIBUTE + " of " + name + " is not numeric: " + value);
    }
    double number = ((Number) value).doubleValue();
    // NaN would truncate to 0 and lift the cap, a negative value would offset other channels.
    if (!Double.isFinite(number) || number < 0) {
      throw new InflightUsageUnavailableException(
          "JMX attribute " + VALUE_ATTRIBUTE + " of " + name + " is not a valid count: " + value,
          null);
    }
    return ((Number) value).longValue();
  }
}
