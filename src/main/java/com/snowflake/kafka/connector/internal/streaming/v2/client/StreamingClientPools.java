package com.snowflake.kafka.connector.internal.streaming.v2.client;

import static com.google.common.base.Strings.isNullOrEmpty;

import com.google.common.annotations.VisibleForTesting;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import com.snowflake.kafka.connector.config.SinkTaskConfig;
import com.snowflake.kafka.connector.internal.KCLogger;
import com.snowflake.kafka.connector.internal.metrics.TaskMetrics;
import com.snowflake.kafka.connector.internal.streaming.StreamingClientProperties;
import com.snowflake.kafka.connector.internal.streaming.v2.ClientRecreationException;
import dev.failsafe.Failsafe;
import dev.failsafe.FailsafeException;
import dev.failsafe.FailsafeExecutor;
import dev.failsafe.RetryPolicy;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.kafka.connect.errors.ConnectException;

/**
 * JVM-global registry of {@link StreamingClientPool} objects, keyed by connector name.
 *
 * <p>Multiple Kafka Connect connector instances (i.e. different connector configs) can run in the
 * same JVM process. Each gets its own {@link StreamingClientPool}, but they all share this static
 * registry because Kafka Connect only passes String config values to tasks — there is no way to
 * inject a shared object directly. Tasks look up their pool by connector name at startup.
 */
public class StreamingClientPools {
  private static final KCLogger LOGGER = new KCLogger(StreamingClientPools.class.getName());

  // Map: connectorName → StreamingClientPool
  private static final Map<String, StreamingClientPool> connectors = new ConcurrentHashMap<>();

  private StreamingClientPools() {}

  /**
   * Gets or creates a client for the given connector, task, and pipe. Multiple tasks can share the
   * same client. Kafka Connect guarantees that no two tasks in the same connector can work on the
   * same partition. It means that two tasks will never work with given channel at the same time,
   * because channel names are scoped to connector_name + topic_name + partition_id
   *
   * @param connectorName the name of the connector
   * @param taskId the ID of the task requesting the client
   * @param pipeName the pipe name
   * @param config parsed task config
   * @param streamingClientProperties streaming client properties
   * @param taskMetrics metrics to record client creation time (noop-safe)
   * @return the client for this pipe
   * @throws IllegalArgumentException if connectorName, taskId, or pipeName is null or empty
   */
  public static SnowflakeStreamingIngestClient getClient(
      final String connectorName,
      final String taskId,
      final String pipeName,
      final SinkTaskConfig config,
      final StreamingClientProperties streamingClientProperties,
      final TaskMetrics taskMetrics) {
    try {
      return getClientAsync(
              connectorName, taskId, pipeName, config, streamingClientProperties, taskMetrics)
          .join();
    } catch (CompletionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof RuntimeException) {
        throw (RuntimeException) cause;
      }
      throw new ConnectException(
          "Unexpected error creating streaming client for pipe: " + pipeName, cause);
    }
  }

  /**
   * Asynchronously gets or creates a client for the given connector, task, and pipe. The returned
   * future completes when the client is ready. Client-invalid errors (including body-less 404) are
   * retried for {@link #clientCreateMaxDuration} without blocking the caller. If that burst is
   * exhausted, the same create is retried every {@link #hourlyRetryDelay}.
   */
  public static CompletableFuture<SnowflakeStreamingIngestClient> getClientAsync(
      final String connectorName,
      final String taskId,
      final String pipeName,
      final SinkTaskConfig config,
      final StreamingClientProperties streamingClientProperties,
      final TaskMetrics taskMetrics) {

    if (isNullOrEmpty(connectorName)) {
      throw new IllegalArgumentException("connectorName cannot be null or empty");
    }
    if (isNullOrEmpty(taskId)) {
      throw new IllegalArgumentException("taskId cannot be null or empty");
    }
    if (isNullOrEmpty(pipeName)) {
      throw new IllegalArgumentException("pipeName cannot be null or empty");
    }

    return clientRetryExecutor(pipeName, clientCreateMaxDuration)
        .getStageAsync(
            () ->
                getPool(connectorName)
                    .getClientAsync(
                        taskId, pipeName, config, streamingClientProperties, taskMetrics));
  }

  private static StreamingClientPool getPool(final String connectorName) {
    return connectors.computeIfAbsent(connectorName, k -> new StreamingClientPool(connectorName));
  }

  public static long getClientCountForTask(final String connectorName, final String taskId) {
    StreamingClientPool pool = connectors.get(connectorName);
    if (pool == null) {
      return 0;
    }

    return pool.getClientCountForTask(taskId);
  }

  /**
   * Atomically replaces the client for a pipe if the current client matches the given invalid
   * client. Uses compare-and-swap semantics: if another caller already replaced the entry, the
   * existing new client is returned without creating a second one.
   *
   * @param connectorName the connector name
   * @param taskId the ID of the task requesting recreation; registered on the replacement entry so
   *     the pool does not prematurely evict it on task-local cleanup
   * @param pipeName the pipe whose client should be replaced
   * @param invalidClient the client instance the caller believes is invalid (identity check)
   * @param config task config for creating the replacement client
   * @param streamingClientProperties streaming client properties
   * @param taskMetrics metrics for timing the new client creation
   * @return the new (or already-replaced) client
   */
  public static SnowflakeStreamingIngestClient recreateClient(
      final String connectorName,
      final String taskId,
      final String pipeName,
      final SnowflakeStreamingIngestClient invalidClient,
      final SinkTaskConfig config,
      final StreamingClientProperties streamingClientProperties,
      final TaskMetrics taskMetrics) {
    try {
      return clientRetryExecutor(pipeName, clientRecreateMaxDuration)
          .get(
              () ->
                  getPool(connectorName)
                      .recreateClient(
                          taskId,
                          pipeName,
                          invalidClient,
                          config,
                          streamingClientProperties,
                          taskMetrics));
    } catch (FailsafeException e) {
      Throwable cause = e.getCause() != null ? e.getCause() : e;
      throw ClientRecreationException.wrap(cause);
    }
  }

  /**
   * Initial delay before the first recreate retry. Backoff doubles each attempt up to {@link
   * #CLIENT_CREATION_MAX_DELAY}, with ±{@link #CLIENT_CREATION_JITTER_FACTOR} jitter to prevent
   * concurrent partition channels from retrying in lockstep.
   */
  private static final Duration CLIENT_CREATION_BASE_DELAY = Duration.ofSeconds(1);

  /** Cap on the exponential backoff delay between recreate retries. */
  private static final Duration CLIENT_CREATION_MAX_DELAY = Duration.ofSeconds(30);

  /** Jitter factor (±, 0.0–1.0) applied to each recreate retry delay. */
  private static final double CLIENT_CREATION_JITTER_FACTOR = 0.2;

  /** Failsafe: {@code -1} means no attempt cap. */
  private static final int UNLIMITED_ATTEMPTS = -1;

  private static final Duration DEFAULT_CLIENT_CREATE_MAX_DURATION = Duration.ofMinutes(6);
  private static final Duration DEFAULT_CLIENT_RECREATE_MAX_DURATION = Duration.ofMinutes(30);
  private static final Duration DEFAULT_HOURLY_RETRY_DELAY = Duration.ofHours(1);

  /**
   * Wall-clock budget for the first-time create burst ({@link #getClient} / {@link
   * #getClientAsync}). Covers a short Envoy NR window.
   */
  static Duration clientCreateMaxDuration = DEFAULT_CLIENT_CREATE_MAX_DURATION;

  /**
   * Wall-clock budget for the replacement-client burst. Sized for observed NR / pipe-failover
   * windows plus headroom.
   */
  static Duration clientRecreateMaxDuration = DEFAULT_CLIENT_RECREATE_MAX_DURATION;

  /**
   * Delay between create/recreate bursts after the inner Failsafe budget is exhausted. Keeps the
   * task out of {@code FAILED} for a retryable client-invalid error, including a body-less 404.
   */
  static Duration hourlyRetryDelay = DEFAULT_HOURLY_RETRY_DELAY;

  /** When unlimited, the burst is bounded by {@code maxDuration} instead of attempt count. */
  static int clientBurstMaxAttempts = UNLIMITED_ATTEMPTS;

  /**
   * Outer hourly policy wrapping an inner burst. Used for both first-time create and recreate.
   */
  private static FailsafeExecutor<SnowflakeStreamingIngestClient> clientRetryExecutor(
      String pipeName, Duration burstMaxDuration) {
    return Failsafe.with(hourlyRetryPolicy(pipeName), clientRetryPolicy(pipeName, burstMaxDuration));
  }

  /**
   * Retries client creation when the SDK reports a client-invalid error, including body-less HTTP
   * 404 (Envoy NR). A 404 with a Snowflake error message is not retried.
   */
  private static RetryPolicy<SnowflakeStreamingIngestClient> clientRetryPolicy(
      String pipeName, Duration maxDuration) {
    return RetryPolicy.<SnowflakeStreamingIngestClient>builder()
        .handleIf(ClientRecreationException::isClientInvalidError)
        .withBackoff(CLIENT_CREATION_BASE_DELAY, CLIENT_CREATION_MAX_DELAY, 2.0)
        .withJitter(CLIENT_CREATION_JITTER_FACTOR)
        .withMaxAttempts(clientBurstMaxAttempts)
        .withMaxDuration(maxDuration)
        .onRetry(
            event ->
                LOGGER.warn(
                    "Streaming client for pipe {} failed with a retryable error"
                        + " (attempt {}, elapsed {}s / {}s budget): {}",
                    pipeName,
                    event.getAttemptCount(),
                    event.getElapsedTime().toSeconds(),
                    maxDuration.toSeconds(),
                    event.getLastException().getMessage()))
        .onRetriesExceeded(
            event ->
                LOGGER.error(
                    "Streaming client for pipe {} burst budget exhausted after {} attempts"
                        + " ({}s elapsed); next attempt in {}: {}",
                    pipeName,
                    event.getAttemptCount(),
                    event.getElapsedTime().toSeconds(),
                    hourlyRetryDelay,
                    event.getException().getMessage()))
        .build();
  }

  /**
   * Outer policy: after a burst budget expires, wait {@link #hourlyRetryDelay} and run another
   * burst. No attempt cap — a wrong account will keep retrying; a routing blip will eventually
   * succeed.
   */
  private static RetryPolicy<SnowflakeStreamingIngestClient> hourlyRetryPolicy(String pipeName) {
    return RetryPolicy.<SnowflakeStreamingIngestClient>builder()
        .handleIf(StreamingClientPools::isRetryableAfterBurst)
        .withDelay(hourlyRetryDelay)
        .withMaxAttempts(UNLIMITED_ATTEMPTS)
        .onRetry(
            event ->
                LOGGER.warn(
                    "Retrying streaming client for pipe {} after {} (hourly attempt {})",
                    pipeName,
                    hourlyRetryDelay,
                    event.getAttemptCount()))
        .build();
  }

  /**
   * Failsafe wraps the last burst failure (often as {@link FailsafeException}). Walk the cause
   * chain so the hourly policy sees the original SDK error.
   */
  private static boolean isRetryableAfterBurst(Throwable e) {
    for (Throwable current = e; current != null; current = current.getCause()) {
      if (ClientRecreationException.isClientInvalidError(current)) {
        return true;
      }
    }
    return false;
  }

  @VisibleForTesting
  static void setRetryDurationsForTest(
      Duration createMax, Duration recreateMax, Duration hourlyDelay) {
    clientCreateMaxDuration = createMax;
    clientRecreateMaxDuration = recreateMax;
    hourlyRetryDelay = hourlyDelay;
  }

  @VisibleForTesting
  static void setBurstMaxAttemptsForTest(int maxAttempts) {
    clientBurstMaxAttempts = maxAttempts;
  }

  @VisibleForTesting
  static void resetRetryDurations() {
    setRetryDurationsForTest(
        DEFAULT_CLIENT_CREATE_MAX_DURATION,
        DEFAULT_CLIENT_RECREATE_MAX_DURATION,
        DEFAULT_HOURLY_RETRY_DELAY);
    clientBurstMaxAttempts = UNLIMITED_ATTEMPTS;
  }

  /**
   * Releases all clients used by a specific task. Clients that are still used by other tasks remain
   * open. Only closes clients when the last task using them stops. When the pool becomes empty (no
   * remaining clients or tasks), the pool is removed from the registry.
   *
   * @param connectorName the name of the connector
   * @param taskId the ID of the task
   */
  public static void closeTaskClients(final String connectorName, final String taskId) {
    connectors.compute(
        connectorName,
        (key, pool) -> {
          if (pool == null) {
            LOGGER.warn(
                "Attempted to release task {} for unknown connector: {}", taskId, connectorName);
            return null;
          }
          pool.closeTaskClients(taskId);
          if (pool.isEmpty()) {
            LOGGER.info("All tasks released for connector: {}", connectorName);
            return null;
          }
          return pool;
        });
  }
}
