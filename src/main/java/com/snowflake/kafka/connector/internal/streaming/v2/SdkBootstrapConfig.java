package com.snowflake.kafka.connector.internal.streaming.v2;

import com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams;
import com.snowflake.kafka.connector.internal.KCLogger;
import java.util.Map;
import java.util.Optional;

/**
 * Applies Snowpipe Streaming SDK bootstrap knobs as JVM system properties. The SDK's FFIBootstrap
 * reads SS_* from sysprop/env on class load, so this MUST run in the task JVM before the first SDK
 * client is created (i.e. early in SnowflakeSinkTask.start()). In Kafka Connect distributed mode
 * the connector and task run in different JVMs, so applying this in the connector would not affect
 * the task that actually creates the SDK client.
 */
public final class SdkBootstrapConfig {

  private static final KCLogger LOGGER = new KCLogger(SdkBootstrapConfig.class.getName());

  static final String SS_LOG_LEVEL = "SS_LOG_LEVEL";
  static final String SS_ENABLE_METRICS = "SS_ENABLE_METRICS";
  static final String SS_METRICS_PORT = "SS_METRICS_PORT";
  static final String SS_METRICS_IP = "SS_METRICS_IP";
  static final String DEFAULT_LOG_LEVEL = "warn";

  private SdkBootstrapConfig() {}

  /**
   * Applies bootstrap knobs for the current task's config.
   *
   * <p><b>JVM-global, first-write-wins:</b> the SDK's FFIBootstrap reads these system properties
   * once per worker JVM on first client creation. When multiple tasks or connectors share a worker
   * JVM, whichever task calls {@code apply()} first wins; a later task enabling Prometheus (or
   * setting a different log level) after the SDK has already bootstrapped will have no effect.
   */
  public static void apply(Map<String, String> config) {
    logLevelToSet(System.getProperty(SS_LOG_LEVEL), System.getenv(SS_LOG_LEVEL))
        .ifPresent(
            level -> {
              System.setProperty(SS_LOG_LEVEL, level);
              LOGGER.info(
                  "Set SDK {} to default '{}' (no operator override found)", SS_LOG_LEVEL, level);
            });

    prometheusToSet(config)
        .ifPresent(
            port -> {
              String host = prometheusHost(config);
              System.setProperty(SS_ENABLE_METRICS, "true");
              System.setProperty(SS_METRICS_PORT, String.valueOf(port));
              System.setProperty(SS_METRICS_IP, host);
              LOGGER.info("Enabled SDK Prometheus metrics endpoint on {}:{}", host, port);
            });
  }

  static Optional<String> logLevelToSet(String currentSysprop, String currentEnv) {
    if (currentSysprop == null && currentEnv == null) {
      return Optional.of(DEFAULT_LOG_LEVEL);
    }
    return Optional.empty();
  }

  static Optional<Integer> prometheusToSet(Map<String, String> config) {
    boolean enable =
        Boolean.parseBoolean(
            config.getOrDefault(
                KafkaConnectorConfigParams.PROMETHEUS_ENABLE,
                String.valueOf(KafkaConnectorConfigParams.PROMETHEUS_ENABLE_DEFAULT)));
    if (!enable) {
      return Optional.empty();
    }
    String raw =
        config.getOrDefault(
            KafkaConnectorConfigParams.PROMETHEUS_PORT,
            String.valueOf(KafkaConnectorConfigParams.PROMETHEUS_PORT_DEFAULT));
    final int port;
    try {
      port = Integer.parseInt(raw.trim());
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
          "Invalid " + KafkaConnectorConfigParams.PROMETHEUS_PORT + " value: '" + raw + "'", e);
    }
    if (port < 1 || port > 65535) {
      throw new IllegalArgumentException(
          KafkaConnectorConfigParams.PROMETHEUS_PORT + " out of range (1-65535): " + port);
    }
    return Optional.of(port);
  }

  static String prometheusHost(java.util.Map<String, String> config) {
    return config.getOrDefault(
        KafkaConnectorConfigParams.PROMETHEUS_HOST,
        KafkaConnectorConfigParams.PROMETHEUS_HOST_DEFAULT);
  }
}
