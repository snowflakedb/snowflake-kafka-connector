package com.snowflake.kafka.connector.internal.metrics;

import java.util.Collection;

/** All metrics related constants. Mainly for JMX */
public class MetricsUtil {
  public static final String JMX_METRIC_PREFIX = "snowflake.kafka.connector";

  // file count related constants
  public static final String OFFSET_SUB_DOMAIN = "offsets";

  /**
   * Offset number that is most recent inside the buffer (In memory buffer)
   *
   * <p>This is updated every time an offset is sent as put API of SinkTask {@link
   * org.apache.kafka.connect.sink.SinkTask#put(Collection)}
   */
  public static final String PROCESSED_OFFSET = "processed-offset";

  public static final String OFFSET_PERSISTED_IN_SNOWFLAKE = "persisted-in-snowflake-offset";

  public static final String LATEST_CONSUMER_OFFSET = "latest-consumer-offset";

  /**
   * Returns the metric-registry key prefix for a given channel, e.g. {@code "channel:myConn_t_0"}.
   * Use this when removing all metrics for a channel via {@link
   * MetricsJmxReporter#removeMetricsFromRegistry}.
   */
  public static String channelMetricPrefix(final String channelName) {
    return "channel:" + channelName;
  }

  /**
   * Construct a channel-level metric name. The resulting MBean will use {@code channel=} as the
   * first key property.
   *
   * <p>Will be of form <b>channel:channelName/subDomain/metricName</b>. The {@code channel:} prefix
   * is parsed by {@link MetricsJmxReporter#getObjectName} to produce the MBean key.
   *
   * @param channelName channel or partition identifier
   * @param subDomain categorize this metric (e.g. "offsets")
   * @param metricName actual Metric name for which we will use Gauge, Meter, Histogram
   * @return concatenized String
   */
  public static String channelMetricName(
      final String channelName, final String subDomain, final String metricName) {
    return channelMetricPrefix(channelName) + "/" + subDomain + "/" + metricName;
  }
}
