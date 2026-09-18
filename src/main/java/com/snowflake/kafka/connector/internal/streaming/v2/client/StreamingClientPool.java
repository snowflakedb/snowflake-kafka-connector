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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

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
    final CompletableFuture<SnowflakeStreamingIngestClient> clientFuture;
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

    /** Copies all task registrations from another entry into this one. */
    void copyTasksFrom(RefCountedClient other) {
      taskIds.addAll(other.taskIds);
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

  /**
   * Asynchronously gets or creates a client for the given task and pipe. The returned future
   * completes when the client is ready.
   */
  CompletableFuture<SnowflakeStreamingIngestClient> getClientAsync(
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

    return entry.clientFuture.whenComplete(
        (client, error) -> {
          if (error != null) {
            // Only remove if the entry still holds the same (failed) future.
            pipes.compute(pipeName, (key, current) -> current == entry ? null : current);
          } else {
            LOGGER.info(
                "Task {} now using pipe {} for connector {}, total tasks on this pipe: {}",
                taskId,
                pipeName,
                connectorName,
                entry.taskCount());
          }
        });
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

  /**
   * Atomically replaces the client for a pipe if the current client matches the given invalid
   * client. Uses compare-and-swap semantics: if another caller already replaced the entry, the
   * existing new client is returned without creating a second one.
   *
   * @param taskId the ID of the task requesting recreation; registered on the replacement entry so
   *     the pool does not prematurely evict it on task-local cleanup
   * @param pipeName the pipe whose client should be replaced
   * @param invalidClient the client instance that the caller believes is invalid (identity check)
   * @param config task config for creating the replacement client
   * @param streamingClientProperties streaming client properties
   * @param taskMetrics metrics for timing the new client creation
   * @return the new (or already-replaced) client
   */
  SnowflakeStreamingIngestClient recreateClient(
      final String taskId,
      final String pipeName,
      final SnowflakeStreamingIngestClient invalidClient,
      final SinkTaskConfig config,
      final StreamingClientProperties streamingClientProperties,
      final TaskMetrics taskMetrics) {

    // Captured inside compute() so the old client can be closed outside the lock.
    AtomicReference<SnowflakeStreamingIngestClient> clientToClose = new AtomicReference<>();

    RefCountedClient chosenEntry =
        pipes.compute(
            pipeName,
            (key, current) -> {
              if (current == null) {
                LOGGER.warn(
                    "recreateClient called for pipe {} but no entry exists in connector {}."
                        + " Creating a fresh entry.",
                    pipeName,
                    connectorName);
                return createReplacement(
                    taskId, pipeName, null, config, streamingClientProperties, taskMetrics);
              }

              // Check if the current entry still holds the invalid client (CAS guard).
              // Use timeout=0 to avoid blocking the compute() supplier on I/O: a client
              // whose future hasn't completed yet cannot possibly be the invalid client the
              // caller just observed, so we can assume it's a valid replacement already in flight.
              SnowflakeStreamingIngestClient currentClient;
              try {
                currentClient = current.clientFuture.get(0, TimeUnit.MILLISECONDS);
              } catch (TimeoutException timeout) {
                LOGGER.info(
                    "recreateClient for pipe {} in connector {}: current entry's future not"
                        + " yet complete, assuming replacement already in flight",
                    pipeName,
                    connectorName);
                current.addTask(taskId);
                return current;
              } catch (CompletionException | ExecutionException e) {
                // Current entry failed to create — replace it unconditionally.
                LOGGER.warn(
                    "recreateClient for pipe {}: current entry has a failed client future,"
                        + " replacing unconditionally",
                    pipeName);
                return createReplacement(
                    taskId, pipeName, current, config, streamingClientProperties, taskMetrics);
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
              }

              if (currentClient != invalidClient) {
                LOGGER.info(
                    "recreateClient for pipe {} in connector {}: client already replaced"
                        + " by another caller, reusing existing entry",
                    pipeName,
                    connectorName);
                current.addTask(taskId);
                return current;
              }

              // CAS matches — replace with a new entry, preserving task registrations.
              LOGGER.info(
                  "Recreating streaming client for pipe {} in connector {}."
                      + " Old client will be closed best-effort.",
                  pipeName,
                  connectorName);
              // Capture old client for best-effort close outside the compute() lock.
              clientToClose.set(currentClient);
              return createReplacement(
                  taskId, pipeName, current, config, streamingClientProperties, taskMetrics);
            });

    // Best-effort close of the old (invalid) client outside the compute() lock
    // to avoid blocking the ConcurrentHashMap bucket during I/O.
    SnowflakeStreamingIngestClient oldClient = clientToClose.get();
    if (oldClient != null) {
      try {
        oldClient.close();
      } catch (Exception e) {
        LOGGER.warn(
            "Best-effort close of invalid client for pipe {} failed: {}", pipeName, e.getMessage());
      }
    }

    return joinAndEvictOnFailure(pipeName, chosenEntry);
  }

  /**
   * Creates a new {@link RefCountedClient} for the given pipe, inheriting task registrations from
   * {@code previous} if non-null, and always registering {@code taskId}. Centralizing this logic
   * ensures the calling task is always registered so the pool does not prematurely evict a
   * freshly-created entry during subsequent task-local cleanup.
   */
  private RefCountedClient createReplacement(
      final String taskId,
      final String pipeName,
      final RefCountedClient previous,
      final SinkTaskConfig config,
      final StreamingClientProperties streamingClientProperties,
      final TaskMetrics taskMetrics) {
    RefCountedClient fresh =
        new RefCountedClient(
            pipeName, connectorName, config, streamingClientProperties, taskMetrics, ioExecutor);
    if (previous != null) {
      fresh.copyTasksFrom(previous);
    }
    fresh.addTask(taskId);
    return fresh;
  }

  /**
   * Joins the entry's client future and evicts the entry from the pool if the future has failed, so
   * the next caller gets a fresh entry instead of retrying a broken one.
   */
  private SnowflakeStreamingIngestClient joinAndEvictOnFailure(
      final String pipeName, final RefCountedClient entry) {
    try {
      return entry.clientFuture.join();
    } catch (CompletionException e) {
      pipes.compute(pipeName, (key, current) -> current == entry ? null : current);
      if (e.getCause() instanceof RuntimeException) {
        throw (RuntimeException) e.getCause();
      }
      throw e;
    }
  }

  /** Returns true if there are no remaining clients or task registrations. */
  boolean isEmpty() {
    return pipes.isEmpty();
  }
}
