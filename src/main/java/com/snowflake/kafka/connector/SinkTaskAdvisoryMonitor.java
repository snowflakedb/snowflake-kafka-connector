package com.snowflake.kafka.connector;

import com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams;
import com.snowflake.kafka.connector.internal.KCLogger;
import com.snowflake.kafka.connector.internal.SnowflakeConnectionService;
import com.snowflake.kafka.connector.internal.SnowflakeErrors;
import com.snowflake.kafka.connector.internal.advisory.AdvisoryLevel;
import com.snowflake.kafka.connector.internal.advisory.AdvisoryMessage;
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
 * background poll cannot fail the task, so it only sets a flag; {@link #throwIfCritical()} throws
 * from the next {@code put()}, which Kafka Connect calls even when there are no records.
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
  private volatile boolean criticalSeen;

  SinkTaskAdvisoryMonitor(
      SnowflakeConnectionService conn, Map<String, String> config, String taskId) {
    this.conn = conn;
    this.taskId = taskId;
    this.enabled = SnowflakeStreamingSinkConnector.failOnCriticalAdvisory(config);
    this.intervalSeconds = SnowflakeStreamingSinkConnector.advisoryPollIntervalSeconds(config);
  }

  /** Refuses to start the task while a critical advisory applies. */
  void checkOnStart() {
    if (enabled && fetchHasCritical()) {
      throw stopException("Refusing to start task");
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
    if (fetchHasCritical()) {
      criticalSeen = true;
      if (poller != null) {
        poller.shutdown(); // not shutdownNow(): this runs on the poller thread
      }
    }
  }

  /** Throws if a periodic check has seen a critical advisory. Called at the start of put(). */
  void throwIfCritical() {
    if (criticalSeen) {
      throw stopException("Stopping task");
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

  private boolean fetchHasCritical() {
    try {
      List<AdvisoryMessage> advisories =
          conn.getKcAdvisoryMessages(SnowflakeStreamingSinkConnector.advisoryRequestJson());
      // Only critical advisories are logged here; the connector logs all of them.
      advisories.stream()
          .filter(m -> AdvisoryLevel.fromString(m.getLevel()) == AdvisoryLevel.CRITICAL)
          .forEach(m -> LOGGER.error("Task {}: {}", taskId, m.getText()));
      return SnowflakeStreamingSinkConnector.hasCritical(advisories);
    } catch (Exception e) {
      LOGGER.debug("Task {}: advisory check failed: {}", taskId, e.getMessage());
      return false;
    }
  }

  private ConnectException stopException(String action) {
    String message =
        action
            + ": a critical server advisory now applies to Kafka Connector "
            + Utils.VERSION
            + ". To keep running set "
            + KafkaConnectorConfigParams.SNOWFLAKE_FEATURE_FAIL_ON_CRITICAL_ADVISORY
            + "=false (not recommended).";
    LOGGER.error("Task {}: {}", taskId, message);
    return new ConnectException(message, SnowflakeErrors.ERROR_5031.getException(message));
  }
}
