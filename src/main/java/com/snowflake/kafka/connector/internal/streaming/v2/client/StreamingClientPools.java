package com.snowflake.kafka.connector.internal.streaming.v2.client;

import static com.google.common.base.Strings.isNullOrEmpty;

import com.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import com.snowflake.kafka.connector.config.SinkTaskConfig;
import com.snowflake.kafka.connector.internal.KCLogger;
import com.snowflake.kafka.connector.internal.metrics.TaskMetrics;
import com.snowflake.kafka.connector.internal.streaming.StreamingClientProperties;
import com.snowflake.kafka.connector.internal.streaming.v2.ClientRecreationException;
import dev.failsafe.Failsafe;
import dev.failsafe.FailsafeException;
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
   * future completes when the client is ready.
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

    return getPool(connectorName)
        .getClientAsync(taskId, pipeName, config, streamingClientProperties, taskMetrics);
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
      return Failsafe.with(recreateClientRetryPolicy(pipeName))
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
      // Retries exhausted — wrap as ClientRecreationException so the batch
      // loop can rewind offsets instead of crashing the task.
      Throwable cause = e.getCause() != null ? e.getCause() : e;
      throw ClientRecreationException.wrap(cause);
    }
  }

  /**
   * Delay between client-creation retries. Pipe failover typically takes a few seconds to stabilize
   * on the server side, and back-to-back retries with no delay would all hit the same in-flight
   * failover window and fail before the server finishes.
   *
   * <p>Note: this delay is per-invocation. {@link #recreateClient} can be called concurrently by
   * multiple {@link
   * com.snowflake.kafka.connector.internal.streaming.v2.SnowpipeStreamingPartitionChannel}s on the
   * same pipe. The pool's CAS dedupes to a single fresh client, but each caller runs its own
   * Failsafe retry schedule — so from the pool's perspective, client creation can happen more than
   * once per {@code CLIENT_CREATION_RETRY_DELAY} window across concurrent callers. This is
   * acceptable: each individual channel still retries at ~5s cadence, so its total recovery window
   * spans {@code MAX_CLIENT_CREATION_RETRIES * CLIENT_CREATION_RETRY_DELAY = ~15s}. When reading
   * logs, expect to see overlapping retry schedules across channels on the same pipe during a
   * failover event.
   */
  private static final Duration CLIENT_CREATION_RETRY_DELAY = Duration.ofSeconds(5);

  /**
   * Maximum retry attempts when a replacement client also fails with a client-invalid error during
   * {@link #recreateClient}. Three attempts provide enough headroom for transient failover windows
   * while keeping total blocking time bounded (each attempt creates a fresh SDK client).
   */
  private static final int MAX_CLIENT_CREATION_RETRIES = 3;

  /**
   * Retries replacement-client creation when the SDK reports a client-invalid error (e.g., pipe
   * failover still in flight). The pool evicts the failed entry on each attempt, so the retry
   * creates a fresh client. Non-client-invalid errors fall through immediately.
   */
  private static RetryPolicy<SnowflakeStreamingIngestClient> recreateClientRetryPolicy(
      String pipeName) {
    return RetryPolicy.<SnowflakeStreamingIngestClient>builder()
        .handleIf(
            e -> e instanceof RuntimeException && ClientRecreationException.isClientInvalidError(e))
        .withMaxAttempts(MAX_CLIENT_CREATION_RETRIES)
        .withDelay(CLIENT_CREATION_RETRY_DELAY)
        .onRetry(
            event ->
                LOGGER.warn(
                    "Replacement client for pipe {} failed with client-invalid error"
                        + " (attempt {}/{}): {}. Retrying after {}.",
                    pipeName,
                    event.getAttemptCount(),
                    MAX_CLIENT_CREATION_RETRIES,
                    event.getLastException().getMessage(),
                    CLIENT_CREATION_RETRY_DELAY))
        .onRetriesExceeded(
            event ->
                LOGGER.error(
                    "Replacement client for pipe {} failed after {} attempts: {}",
                    pipeName,
                    event.getAttemptCount(),
                    event.getException().getMessage()))
        .build();
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
