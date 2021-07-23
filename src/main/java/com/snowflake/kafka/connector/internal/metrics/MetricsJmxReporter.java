package com.snowflake.kafka.connector.internal.metrics;

import static com.snowflake.kafka.connector.internal.metrics.MetricsUtil.JMX_METRIC_PREFIX;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.jmx.JmxReporter;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import com.snowflake.kafka.connector.internal.Logging;
import com.snowflake.kafka.connector.internal.SnowflakeErrors;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class for creation of JMX Metrics from metrics registry, also includes a definition to
 * create an ObjectName used to register a {@link com.codahale.metrics.Metric}
 */
public class MetricsJmxReporter {
  static final Logger LOGGER = LoggerFactory.getLogger(MetricsJmxReporter.class);

  /**
   * Create JMXReporter Instance, which internally handles the mbean server fetching and
   * registration of Mbeans. We use codehale metrics library to achieve this. More details
   * here: @see <a href="https://metrics.dropwizard.io/4.2.0/getting-started.html">DropWizard</a>
   *
   * <p>We will convert all duration to SECONDS and prefix our metrics with {@link
   * MetricsUtil#JMX_METRIC_PREFIX}
   *
   * @param metricRegistry Pool of metrics we have registered. May be a Gauge, Meter, Histogram.
   * @param connectorName connectorName passed inside configuration
   * @return JMXReporter instance.
   */
  public static JmxReporter createJMXReporter(
      final MetricRegistry metricRegistry, final String connectorName) {

    return JmxReporter.forRegistry(metricRegistry)
        .inDomain(JMX_METRIC_PREFIX)
        .convertDurationsTo(TimeUnit.SECONDS)
        .createsObjectNamesWith(
            (ignoreMeterType, jmxDomain, metricName) ->
                getObjectName(connectorName, jmxDomain, metricName))
        .build();
  }

  @VisibleForTesting
  public static ObjectName getObjectName(
      String connectorName, String jmxDomain, String metricName) {
    LOGGER.debug(
        "registering JMX objectName - instanceName: {}, jmxDomain: {}, metricName: {}",
        connectorName,
        jmxDomain,
        metricName);
    try {
      StringBuilder sb =
          new StringBuilder(jmxDomain).append(":connector=").append(connectorName).append(',');

      // each metric name will be in a form pipeName/subDomain/metricName
      Iterator<String> tokens = Splitter.on("/").split(metricName).iterator();
      // Append PipeName
      sb.append("pipe=").append(tokens.next());

      // Append subDomain
      sb.append(",category=").append(tokens.next());

      // append metric name
      sb.append(",name=").append(tokens.next());

      return new ObjectName(sb.toString());
    } catch (MalformedObjectNameException e) {
      LOGGER.warn(Logging.logMessage("Could not create Object name for MetricName:{}", metricName));
      throw SnowflakeErrors.ERROR_5020.getException();
    }
  }
}
