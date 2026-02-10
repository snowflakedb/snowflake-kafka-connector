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
   * Construct the actual metrics name that will be passed in by dropwizard framework to {@link
   * MetricsJmxReporter#getObjectName(String, String, String)} We will prefix actual metric name
   * with partitionName and subcategory of the metric.
   *
   * <p>Will be of form <b>partitionName/subDomain/metricName</b>
   *
   * @param partitionName partitionNAme based on partition number (pipeName for Snowpipe or
   *     partitionChannelKey for Streaming)
   * @param subDomain categorize this metric (Actual ObjectName creation Logic will be handled in
   *     getObjectName)
   * @param metricName actual Metric name for which we will use Gauge, Meter, Histogram
   * @return concatenized String
   */
  public static String constructMetricName(
      final String partitionName, final String subDomain, final String metricName) {
    return String.format("%s/%s/%s", partitionName, subDomain, metricName);
  }
}
