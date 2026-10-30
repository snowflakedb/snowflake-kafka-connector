package com.snowflake.kafka.connector;

import com.snowflake.kafka.connector.internal.KCLogger;
import com.snowflake.kafka.connector.internal.SnowflakeConnectionService;
import com.snowflake.kafka.connector.internal.SnowflakeErrors;
import com.snowflake.kafka.connector.internal.SnowflakeKafkaConnectorException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.connect.errors.ConnectException;

/**
 * Stops a sink task when a CRITICAL server advisory applies to this connector version
 * (SNOW-3648295).
 *
 * <p>The connector's own check ({@link SnowflakeStreamingSinkConnector}) only fails the connector:
 * Kafka Connect keeps its tasks running, and they may run on other workers, so each task checks for
 * itself. The task checks once on start and then every {@code
 * snowflake.feature.advisory_poll_interval_seconds} ({@code <= 0} disables the periodic check),
 * with a random first delay so that many tasks do not query Snowflake at the same time. A
 * background poll cannot fail the task, so it only records the error (and sends {@code
 * kafka_fatal_error} telemetry); {@link #throwIfCritical()} throws from the next {@code put()},
 * which Kafka Connect calls even when there are no records. The exception message carries the
 * server's advisory text, so it shows in the task trace of {@code GET /connectors/<name>/status}.
 *
 * <p>With {@code snowflake.feature.fail_on_critical_advisory=false} the task does not check at all;
 * the connector still logs the advisories. Fetch errors never stop the task.
 */
class SinkTaskAdvisoryMonitor {
  private static final KCLogger LOGGER = new KCLogger(SinkTaskAdvisoryMonitor.class.getName());

  private final SnowflakeConnectionService conn;
  private final String taskId;
  private final boolean enabled;
  private final long intervalSeconds;
  private ScheduledExecutorService poller;
  // Set by the poll that sees a critical advisory; null until then.
  private volatile String stopMessage;
  private volatile SnowflakeKafkaConnectorException stopCause;

  SinkTaskAdvisoryMonitor(
      SnowflakeConnectionService conn, Map<String, String> config, String taskId) {
    this.conn = conn;
    this.taskId = taskId;
    this.enabled = SnowflakeStreamingSinkConnector.failOnCriticalAdvisory(config);
    this.intervalSeconds = SnowflakeStreamingSinkConnector.advisoryPollIntervalSeconds(config);
  }

  /** Refuses to start the task while a critical advisory applies. */
  void checkOnStart() {
    if (!enabled) {
      return;
    }
    List<String> criticalTexts = fetchCriticalTexts();
    if (!criticalTexts.isEmpty()) {
      String message =
          SnowflakeStreamingSinkConnector.criticalStopMessage(
              "Refusing to start task " + taskId, criticalTexts);
      LOGGER.error("{}", message);
      throw new ConnectException(message, reportCritical(message));
    }
  }

  /** Starts the periodic check. No-op when disabled or when the interval is {@code <= 0}. */
  void startPolling() {
    if (!enabled || intervalSeconds <= 0) {
      return;
    }
    poller =
        Executors.newSingleThreadScheduledExecutor(
            r -> {
              Thread t = new Thread(r, "kc-advisory-poller-task-" + taskId);
              t.setDaemon(true);
              return t;
            });
    long intervalMs = TimeUnit.SECONDS.toMillis(intervalSeconds);
    poller.scheduleAtFixedRate(
        this::poll,
        ThreadLocalRandom.current().nextLong(intervalMs),
        intervalMs,
        TimeUnit.MILLISECONDS);
  }

  /** One periodic check; package-private for testing. */
  void poll() {
    List<String> criticalTexts = fetchCriticalTexts();
    if (!criticalTexts.isEmpty() && stopCause == null) {
      String message =
          SnowflakeStreamingSinkConnector.criticalStopMessage(
              "Stopping task " + taskId, criticalTexts);
      LOGGER.error("{}", message);
      stopMessage = message;
      stopCause = reportCritical(message);
      if (poller != null) {
        poller.shutdown(); // not shutdownNow(): this runs on the poller thread
      }
    }
  }

  /** Throws if a periodic check has seen a critical advisory. Called at the start of put(). */
  void throwIfCritical() {
    SnowflakeKafkaConnectorException cause = stopCause;
    if (cause != null) {
      throw new ConnectException(stopMessage, cause);
    }
  }

  void stop() {
    if (poller != null) {
      poller.shutdownNow();
    }
  }

  /** Package-private for testing. */
  ScheduledExecutorService pollerForTest() {
    return poller;
  }

  /**
   * Returns the texts of the critical advisories; empty if there are none or the fetch failed. The
   * stop message carries the texts, so they are not logged separately; the connector logs all
   * advisories.
   */
  private List<String> fetchCriticalTexts() {
    try {
      return SnowflakeStreamingSinkConnector.criticalTexts(
          conn.getKcAdvisoryMessages(SnowflakeStreamingSinkConnector.advisoryRequestJson()));
    } catch (Exception e) {
      LOGGER.debug("Task {}: advisory check failed: {}", taskId, e.getMessage());
      return Collections.emptyList();
    }
  }

  /** Builds the ERROR_5031 cause and sends kafka_fatal_error telemetry through the task's conn. */
  private SnowflakeKafkaConnectorException reportCritical(String message) {
    return SnowflakeErrors.ERROR_5031.getException(message, conn.getTelemetryClient());
  }
}
