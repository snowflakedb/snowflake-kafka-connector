package com.snowflake.kafka.connector.internal.streaming.telemetry;

import com.google.common.annotations.VisibleForTesting;
import com.snowflake.kafka.connector.internal.KCLogger;
import com.snowflake.kafka.connector.internal.streaming.channel.TopicPartitionChannel;
import com.snowflake.kafka.connector.internal.telemetry.SnowflakeTelemetryService;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Handles periodic reporting of channel status telemetry to Snowflake. This class manages a
 * background daemon thread that reports telemetry at regular intervals.
 */
public final class PeriodicTelemetryReporter {

  private static final KCLogger LOGGER = new KCLogger(PeriodicTelemetryReporter.class.getName());

  public static final long DEFAULT_REPORT_INTERVAL_MS = 120 * 1000L;

  public static final long MAX_INITIAL_JITTER_MS = 10 * 1000L;

  private final SnowflakeTelemetryService telemetryService;
  private final Supplier<Map<String, TopicPartitionChannel>> channelsSupplier;
  private final String connectorName;
  private final String taskId;
  private final long reportIntervalMs;
  private final ScheduledExecutorService executor;

  public PeriodicTelemetryReporter(
      SnowflakeTelemetryService telemetryService,
      Supplier<Map<String, TopicPartitionChannel>> channelsSupplier,
      String connectorName,
      String taskId) {
    this(telemetryService, channelsSupplier, connectorName, taskId, DEFAULT_REPORT_INTERVAL_MS);
  }

  @VisibleForTesting
  PeriodicTelemetryReporter(
      SnowflakeTelemetryService telemetryService,
      Supplier<Map<String, TopicPartitionChannel>> channelsSupplier,
      String connectorName,
      String taskId,
      long reportIntervalMs) {
    this.telemetryService = telemetryService;
    this.channelsSupplier = channelsSupplier;
    this.connectorName = connectorName;
    this.taskId = taskId;
    this.reportIntervalMs = reportIntervalMs;
    this.executor = createExecutor();
  }

  private ScheduledExecutorService createExecutor() {
    return Executors.newSingleThreadScheduledExecutor(
        r -> {
          Thread t = new Thread(r);
          t.setName("snowflake-telemetry-reporter-" + connectorName + "-" + taskId);
          t.setDaemon(true);
          return t;
        });
  }

  /** Starts the periodic telemetry reporting with jitter to prevent thundering herd. */
  public void start() {
    long jitter = ThreadLocalRandom.current().nextLong(0, MAX_INITIAL_JITTER_MS);
    long initialDelay = reportIntervalMs + jitter;

    executor.scheduleAtFixedRate(
        this::reportChannelStatusTelemetry, initialDelay, reportIntervalMs, TimeUnit.MILLISECONDS);
    LOGGER.info(
        "Started periodic telemetry reporter with interval {} ms (initial delay {} ms including {}"
            + " ms jitter) for connector: {}, task: {}",
        reportIntervalMs,
        initialDelay,
        jitter,
        connectorName,
        taskId);
  }

  public void stop() {
    if (!executor.isShutdown()) {
      LOGGER.info("Stopping telemetry reporter for connector: {}, task: {}", connectorName, taskId);
      executor.shutdown();
      try {
        if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
          LOGGER.warn("Telemetry reporter did not terminate gracefully, forcing shutdown");
          executor.shutdownNow();
        }
      } catch (InterruptedException e) {
        LOGGER.warn("Interrupted while waiting for telemetry reporter to terminate");
        executor.shutdownNow();
        Thread.currentThread().interrupt();
      }
    }
  }

  /**
   * Reports telemetry for all active channels. This method is called periodically by the scheduled
   * executor.
   */
  private void reportChannelStatusTelemetry() {
    try {
      Map<String, TopicPartitionChannel> channels = channelsSupplier.get();
      if (channels == null || channels.isEmpty()) {
        LOGGER.info("No active channels to report telemetry for");
        return;
      }

      LOGGER.debug(
          "Reporting telemetry for {} active channels for connector: {}, task: {}",
          channels.size(),
          connectorName,
          taskId);

      for (Map.Entry<String, TopicPartitionChannel> entry : channels.entrySet()) {
        reportChannelTelemetry(entry.getKey(), entry.getValue());
      }
    } catch (Exception e) {
      LOGGER.error("Error during periodic telemetry reporting: {}", e.getMessage());
    }
  }

  private void reportChannelTelemetry(String channelKey, TopicPartitionChannel channel) {
    try {
      final SnowflakeTelemetryChannelStatus channelStatus =
          channel.getSnowflakeTelemetryChannelStatus();

      if (channelStatus != null && !channelStatus.isEmpty()) {
        telemetryService.reportKafkaPartitionUsage(channelStatus, false);
        LOGGER.trace("Reported telemetry for channel: {}", channelKey);
      }
    } catch (Exception e) {
      LOGGER.warn(
          "Failed to report telemetry for channel: {}, error: {}", channelKey, e.getMessage());
    }
  }
}
