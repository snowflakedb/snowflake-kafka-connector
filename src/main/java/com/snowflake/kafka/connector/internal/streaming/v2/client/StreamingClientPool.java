package com.snowflake.kafka.connector.internal.streaming.v2.client;

import com.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import com.snowflake.kafka.connector.config.SinkTaskConfig;
import com.snowflake.kafka.connector.internal.KCLogger;
import com.snowflake.kafka.connector.internal.metrics.TaskMetrics;
import com.snowflake.kafka.connector.internal.streaming.StreamingClientProperties;
import com.snowflake.kafka.connector.internal.streaming.v2.service.ThreadPools;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import org.apache.kafka.connect.errors.ConnectException;

/**
 * Manages clients for a single connector. Tracks which tasks use which pipes and only closes
 * clients when no tasks are using them.
 *
 * <p>Client creation is dispatched to the connector's I/O thread pool so that multiple pipes can
 * initialize in parallel.
 *
 * <p>Thread safety is achieved via a single {@link ConcurrentHashMap} with per-key atomic {@code
 * compute()} calls — no explicit locking is needed. The actual blocking wait for client readiness
 * ({@code future.join()}) happens outside the atomic section so that other pipes can proceed in
 * parallel.
 */
public class StreamingClientPool {
  private static final KCLogger LOGGER = new KCLogger(StreamingClientPool.class.getName());

  private final String connectorName;

  private final ConcurrentHashMap<String, RefCountedClient> pipes = new ConcurrentHashMap<>();

  private final ExecutorService ioExecutor;

  /**
   * A client shared by one or more tasks. Holds a {@link CompletableFuture} so that client creation
   * can be kicked off asynchronously, allowing multiple pipes to initialize in parallel.
   */
  static class RefCountedClient {
    private final CompletableFuture<SnowflakeStreamingIngestClient> clientFuture;
    private final Set<String> taskIds = ConcurrentHashMap.newKeySet();

    RefCountedClient(
        String pipeName,
        String connectorName,
        SinkTaskConfig config,
        StreamingClientProperties streamingClientProperties,
        TaskMetrics taskMetrics,
        ExecutorService executor) {
      LOGGER.info(
          "Creating new streaming client for pipe: {}, connector: {}", pipeName, connectorName);
      this.clientFuture =
          CompletableFuture.supplyAsync(
              () -> {
                try (TaskMetrics.TimingContext ignored = taskMetrics.timeSdkClientCreate()) {
                  return StreamingClientFactory.createClient(
                      pipeName, config, streamingClientProperties);
                }
              },
              executor);
    }

    void addTask(String taskId) {
      taskIds.add(taskId);
    }

    boolean hasTask(String taskId) {
      return taskIds.contains(taskId);
    }

    /** Removes the task and returns {@code true} if no tasks remain (client is unreferenced). */
    boolean removeTask(String taskId) {
      return taskIds.remove(taskId) && taskIds.isEmpty();
    }

    int taskCount() {
      return taskIds.size();
    }

    /**
     * Blocks until the client is ready, unwrapping {@link CompletionException} so callers see the
     * original exception type.
     */
    SnowflakeStreamingIngestClient awaitClient(String pipeName) {
      try {
        return clientFuture.join();
      } catch (CompletionException e) {
        if (e.getCause() instanceof RuntimeException) {
          throw (RuntimeException) e.getCause();
        }
        throw new ConnectException(
            "Unexpected error creating streaming client for pipe: " + pipeName, e.getCause());
      }
    }

    void close(String pipeName, String connectorName) {
      LOGGER.info(
          "Closing client for pipe {} in connector {} (last task stopped)",
          pipeName,
          connectorName);
      clientFuture.join().close();
    }
  }

  StreamingClientPool(final String connectorName) {
    this.connectorName = connectorName;
    this.ioExecutor = ThreadPools.getIoExecutor(connectorName);

    LOGGER.info("Created client manager for connector: {}", connectorName);
  }

  SnowflakeStreamingIngestClient getClient(
      final String taskId,
      final String pipeName,
      final SinkTaskConfig config,
      final StreamingClientProperties streamingClientProperties,
      final TaskMetrics taskMetrics) {

    RefCountedClient entry =
        pipes.compute(
            pipeName,
            (key, current) -> {
              if (current == null) {
                current =
                    new RefCountedClient(
                        pipeName,
                        connectorName,
                        config,
                        streamingClientProperties,
                        taskMetrics,
                        ioExecutor);
              }
              current.addTask(taskId);
              return current;
            });

    // Block until this specific client is ready — lock is NOT held, so other pipes can proceed.
    SnowflakeStreamingIngestClient client;
    try {
      client = entry.awaitClient(pipeName);
    } catch (RuntimeException e) {
      // Only remove if the entry still holds the same (failed) future.
      pipes.compute(pipeName, (key, current) -> current == entry ? null : current);
      throw e;
    }

    LOGGER.info(
        "Task {} now using pipe {} for connector {}, total tasks on this pipe: {}",
        taskId,
        pipeName,
        connectorName,
        entry.taskCount());

    return client;
  }

  long getClientCountForTask(final String taskId) {
    return pipes.values().stream().filter(entry -> entry.hasTask(taskId)).count();
  }

  void closeTaskClients(final String taskId) {
    LOGGER.info("Releasing clients for task {} in connector {}", taskId, connectorName);

    for (String pipeName : pipes.keySet()) {
      pipes.compute(
          pipeName,
          (key, entry) -> {
            if (entry == null) {
              return null;
            }
            if (entry.removeTask(taskId)) {
              entry.close(pipeName, connectorName);
              return null;
            }
            return entry;
          });
    }
  }

  /** Returns true if there are no remaining clients or task registrations. */
  boolean isEmpty() {
    return pipes.isEmpty();
  }
}
