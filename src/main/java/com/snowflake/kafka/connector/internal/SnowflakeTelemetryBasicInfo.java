package com.snowflake.kafka.connector.internal;

import java.lang.management.ManagementFactory;
import java.util.Optional;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class SnowflakeTelemetryBasicInfo {
  final String tableName;
  final String stageName;
  final String pipeName;
  static final String TABLE_NAME = "table_name";
  static final String STAGE_NAME = "stage_name";
  static final String PIPE_NAME = "pipe_name";

  // Name of mBean if jmx monitoring is enabled
  private ObjectName mBeanName;

  // A boolean to turn on or off a JMX metric as required.
  private volatile boolean enableJMXMonitoring;

  private static final Logger LOGGER = LoggerFactory.getLogger(SnowflakeTelemetryBasicInfo.class);

  private static final String JMX_METRIC_PREFIX = "snowflake.kafka.connector";

  SnowflakeTelemetryBasicInfo(
      final String tableName,
      final String stageName,
      final String pipeName,
      final String connectorName,
      final boolean enableJMXMonitoring) {
    this.tableName = tableName;
    this.stageName = stageName;
    this.pipeName = pipeName;
    this.enableJMXMonitoring = enableJMXMonitoring;
    if (enableJMXMonitoring) {
      mBeanName = metricName(pipeName, connectorName);
    }
  }

  abstract void dumpTo(ObjectNode msg);

  /**
   * Registers a metrics MBean into the platform MBean server. The method is intentionally
   * synchronized to prevent preemption between registration and unregistration.
   */
  public synchronized void registerMBean() {
    try {
      Optional<MBeanServer> optMBeanServer = getValidMbeanServer();
      if (optMBeanServer.isPresent()) {
        optMBeanServer.get().registerMBean(this, mBeanName);
        LOGGER.info(Logging.logMessage("Registered Mbean:{}", mBeanName));
      }
    } catch (Exception e) {
      LOGGER.warn(
          Logging.logMessage(
              "Unable to register the MBean: '{}' with exception: {}", mBeanName, e.getMessage()));
    }
  }

  /**
   * Unregisters a metrics MBean from the platform MBean server. The method is intentionally
   * synchronized to prevent preemption between registration and un-registration.
   */
  public synchronized void unregisterMBean() {
    try {
      Optional<MBeanServer> optMBeanServer = getValidMbeanServer();
      if (optMBeanServer.isPresent()) {
        optMBeanServer.get().unregisterMBean(mBeanName);
        LOGGER.info(Logging.logMessage("Unregistered Mbean:{}", mBeanName));
      }
    } catch (Exception e) {
      LOGGER.warn(
          Logging.logMessage("Unable to unregister the MBean '{}': {}", mBeanName, e.getMessage()));
    }
  }

  /**
   * @return Gets an {@link MBeanServer} instance wrapped around Optional if we are able to fetch
   *     from ManagementFactory.
   */
  private Optional<MBeanServer> getValidMbeanServer() {
    if (this.mBeanName != null && enableJMXMonitoring) {
      final MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
      if (mBeanServer == null) {
        LOGGER.info(Logging.logMessage("JMX not supported, bean '{}' not registered/unregistered"));
        throw new UnsupportedOperationException("JMX not supported");
      }
      return Optional.of(mBeanServer);
    }
    LOGGER.info(
        Logging.logMessage(
            "Mbean Name is invalid or JMX is not enabled for class:{}", this.getClass().getName()));
    return Optional.empty();
  }

  /**
   * Create a JMX metric name for the given metric.
   *
   * @param pipeName the name of the context
   * @param connectorName Connector Name given inside configuration. (A required config)
   * @return the JMX metric name
   * @throws com.snowflake.kafka.connector.internal.SnowflakeKafkaConnectorException if the name is
   *     invalid
   */
  public static ObjectName metricName(String pipeName, String connectorName) {
    final String metricName = JMX_METRIC_PREFIX + ":type=" + connectorName + ",name=" + pipeName;
    try {
      return new ObjectName(metricName);
    } catch (MalformedObjectNameException e) {
      LOGGER.warn(Logging.logMessage("Could not create Object name for MetricName:{}", metricName));
      throw SnowflakeErrors.ERROR_5020.getException();
    }
  }
}
