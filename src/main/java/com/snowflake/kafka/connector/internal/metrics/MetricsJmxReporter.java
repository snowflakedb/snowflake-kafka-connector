package com.snowflake.kafka.connector.internal.metrics;

import static com.snowflake.kafka.connector.internal.metrics.MetricsUtil.JMX_METRIC_PREFIX;

import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.jmx.JmxReporter;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import com.snowflake.kafka.connector.internal.KCLogger;
import com.snowflake.kafka.connector.internal.SnowflakeErrors;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

/**
 * Helper class for creation of JMX Metrics from metrics registry, also includes a definition to
 * create an ObjectName used to register a {@link com.codahale.metrics.Metric}
 *
 * <p>This instance is separate for all pipes and hence registration and unregistration of metrics
 * is handled per pipe level.
 */
public class MetricsJmxReporter {
  static final KCLogger LOGGER = new KCLogger(MetricsJmxReporter.class.getName());

  // The registry which will hold pool of all metrics for this instance
  private final MetricRegistry metricRegistry;

  /**
   * Wrapper on top of listeners and metricRegistry for codehale. This will be useful to start the
   * jmx metrics when time is appropriate. (Check {@link MetricsJmxReporter#start()}
   */
  private final JmxReporter jmxReporter;

  public MetricsJmxReporter(MetricRegistry metricRegistry, final String connectorName) {
    this.metricRegistry = metricRegistry;
    this.jmxReporter = createJMXReporter(connectorName);
  }

  public MetricRegistry getMetricRegistry() {
    return metricRegistry;
  }

  /**
   * This function will internally register all metrics present inside metric registry and will
   * register mbeans to the mbeanserver
   */
  public void start() {
    jmxReporter.start();
  }

  /**
   * This method is called to fetch an object name for all registered metrics. It can be called
   * during registration or unregistration. (Internal implementation of codehale)
   *
   * @param connectorName name of the connector. (From Config)
   * @param jmxDomain JMX Domain
   * @param metricName metric name used while registering the metric. (Check {@link
   *     MetricsUtil#channelMetricName(String, String, String)}
   * @return Object Name constructed from above three args
   */
  @VisibleForTesting
  static ObjectName getObjectName(String connectorName, String jmxDomain, String metricName) {
    try {
      // each metric name is scope:scopeValue/subDomain/metricName
      // e.g. "channel:conn_topic_0/offsets/processed-offset"
      Iterator<String> tokens = Splitter.on("/").split(metricName).iterator();

      // First token is always scope:value -- split on colon to get the MBean key and value
      String firstToken = tokens.next();
      int colonIndex = firstToken.indexOf(':');

      Hashtable<String, String> keys = new Hashtable<>();
      keys.put("connector", connectorName);
      keys.put(firstToken.substring(0, colonIndex), firstToken.substring(colonIndex + 1));
      keys.put("category", tokens.next());
      keys.put("name", tokens.next());

      return new ObjectName(jmxDomain, keys);
    } catch (MalformedObjectNameException e) {
      LOGGER.warn("Could not create Object name for MetricName:{}", metricName);
      throw SnowflakeErrors.ERROR_5020.getException();
    }
  }

  /**
   * Unregister all snowflake KC related metrics from registry
   *
   * @param prefixFilter prefix for removing the filter.
   */
  public void removeMetricsFromRegistry(final String prefixFilter) {
    if (metricRegistry.getMetrics().size() != 0) {
      LOGGER.debug("Unregistering all metrics matching prefix '{}'", prefixFilter);
      metricRegistry.removeMatching(MetricFilter.startsWith(prefixFilter));
      LOGGER.debug(
          "Metric registry size after removing '{}' is:{}, names:{}",
          prefixFilter,
          metricRegistry.getMetrics().size(),
          metricRegistry.getMetrics().keySet().toString());
    }
  }

  /**
   * Create JMXReporter Instance, which internally handles the mbean server fetching and
   * registration of Mbeans. We use codehale metrics library to achieve this. More details
   * here: @see <a href="https://metrics.dropwizard.io/4.2.0/getting-started.html">DropWizard</a>
   *
   * <p>We will convert all duration to SECONDS and prefix our metrics with {@link
   * MetricsUtil#JMX_METRIC_PREFIX}
   *
   * @param connectorName connectorName passed inside configuration
   * @return JMXReporter instance.
   */
  private JmxReporter createJMXReporter(final String connectorName) {
    return JmxReporter.forRegistry(this.metricRegistry)
        .inDomain(JMX_METRIC_PREFIX)
        .convertDurationsTo(TimeUnit.SECONDS)
        .createsObjectNamesWith(
            (ignoreMeterType, jmxDomain, metricName) ->
                getObjectName(connectorName, jmxDomain, metricName))
        .build();
  }
}
