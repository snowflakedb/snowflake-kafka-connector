package com.snowflake.kafka.connector.internal.metrics;

import static com.snowflake.kafka.connector.internal.metrics.MetricsUtil.JMX_METRIC_PREFIX;

import com.codahale.metrics.MetricFilter;
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

  /**
   * This method is called to fetch an object name for all registered metrics. It can be called
   * during registration or unregistration.
   *
   * @param connectorName name of the connector. (From Config)
   * @param jmxDomain JMX Domain
   * @param metricName metric name used while registering the metric. (Check {@link
   *     MetricsUtil#constructMetricName(String, String, String)}
   * @return Object Name constructed from above three args
   */
  @VisibleForTesting
  public static ObjectName getObjectName(
      String connectorName, String jmxDomain, String metricName) {
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

  /**
   * Unregister all snowflake KC related metrics from registry
   *
   * @param metricRegistry to remove all metrics from
   * @param prefixFilter prefix for removing the filter.
   */
  public static void removeMetricsFromRegistry(
      MetricRegistry metricRegistry, final String prefixFilter) {
    if (metricRegistry.getMetrics().size() != 0) {
      LOGGER.debug(Logging.logMessage("Unregistering all metrics for pipe:{}", prefixFilter));
      metricRegistry.removeMatching(MetricFilter.startsWith(prefixFilter));
      LOGGER.debug(
          Logging.logMessage(
              "Metric registry size for pipe:{} is:{}",
              prefixFilter,
              metricRegistry.getMetrics().size()));
    }
  }
}
