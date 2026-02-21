package com.snowflake.kafka.connector.internal.streaming.v2.client;

import com.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams;
import com.snowflake.kafka.connector.internal.KCLogger;
import com.snowflake.kafka.connector.internal.metrics.TaskMetrics;
import com.snowflake.kafka.connector.internal.streaming.StreamingClientProperties;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.connect.errors.ConnectException;

/**
 * Manages clients for a single connector. Tracks which tasks use which pipes and only closes
 * clients when no tasks are using them.
 *
 * <p>Client creation is performed asynchronously on a dedicated thread pool so that multiple pipes
 * can be initialized in parallel. A second, larger, configurable pool is exposed for channel I/O
 * work (opening channels, status checks, flushes). Keeping the pools separate prevents channel work
 * from starving client creation.
 *
 * <p>Thread safety is achieved via a single {@link ConcurrentHashMap} with per-key atomic {@code
 * compute()} calls — no explicit locking is needed. The actual blocking wait for client readiness
 * ({@code future.join()}) happens outside the atomic section so that other pipes can proceed in
 * parallel.
 */
public class StreamingClientPool {
  private static final KCLogger LOGGER = new KCLogger(StreamingClientPool.class.getName());
  private static final int CLIENT_CREATION_THREADS = 4;

  private final String connectorName;
  private final ConcurrentHashMap<String, RefCountedClient> pipes = new ConcurrentHashMap<>();
  private final ExecutorService clientCreationExecutor;
  private final ExecutorService channelIoExecutor;

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
        Map<String, String> connectorConfig,
        StreamingClientProperties streamingClientProperties,
        TaskMetrics taskMetrics,
        ExecutorService executor) {
      LOGGER.info(
          "Creating new streaming client for pipe: {}, connector: {}", pipeName, connectorName);
      // Capture the task thread's context classloader so the ForkJoinPool thread can use it.
      // Kafka Connect uses a PluginClassLoader that must be on the thread context for the SDK's
      // native library loading (FFIBootstrap) to find resources inside plugin JARs.
      final ClassLoader callerClassLoader = Thread.currentThread().getContextClassLoader();
      this.clientFuture =
          CompletableFuture.supplyAsync(
              () -> {
                final ClassLoader originalCL = Thread.currentThread().getContextClassLoader();
                Thread.currentThread().setContextClassLoader(callerClassLoader);
                try (TaskMetrics.TimingContext ignored = taskMetrics.timeSdkClientCreate()) {
                  return StreamingClientFactory.createClient(
                      pipeName, connectorConfig, streamingClientProperties);
                } finally {
                  Thread.currentThread().setContextClassLoader(originalCL);
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

    void closeAsync(String pipeName, String connectorName) {
      clientFuture.thenAccept(
          client -> {
            LOGGER.info(
                "Closing client for pipe {} in connector {} (last task stopped)",
                pipeName,
                connectorName);
            client.close();
          });
    }
  }

  StreamingClientPool(final String connectorName, final Map<String, String> connectorConfig) {
    this.connectorName = connectorName;

    int maxIoThreads = KafkaConnectorConfigParams.SNOWFLAKE_STREAMING_IO_MAX_THREADS_DEFAULT;
    String configured =
        connectorConfig.get(KafkaConnectorConfigParams.SNOWFLAKE_STREAMING_IO_MAX_THREADS);
    if (configured != null) {
      try {
        maxIoThreads = Math.max(1, Integer.parseInt(configured.trim()));
      } catch (NumberFormatException e) {
        LOGGER.warn(
            "Invalid value for {}: '{}', using default {}",
            KafkaConnectorConfigParams.SNOWFLAKE_STREAMING_IO_MAX_THREADS,
            configured,
            maxIoThreads);
      }
    }

    this.clientCreationExecutor =
        Executors.newFixedThreadPool(
            CLIENT_CREATION_THREADS, new DaemonThreadFactory(connectorName + "-client-create"));
    this.channelIoExecutor =
        Executors.newFixedThreadPool(maxIoThreads, new DaemonThreadFactory(connectorName + "-io"));

    LOGGER.info(
        "Created client manager for connector: {}, clientCreationThreads: {}, ioThreads: {}",
        connectorName,
        CLIENT_CREATION_THREADS,
        maxIoThreads);
  }

  ExecutorService getChannelIoExecutor() {
    return channelIoExecutor;
  }

  SnowflakeStreamingIngestClient getClient(
      final String taskId,
      final String pipeName,
      final Map<String, String> connectorConfig,
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
                        connectorConfig,
                        streamingClientProperties,
                        taskMetrics,
                        clientCreationExecutor);
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
              entry.closeAsync(pipeName, connectorName);
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

  /** Shuts down both thread pools. Called when the pool is evicted from the connector map. */
  void shutdown() {
    LOGGER.info("Shutting down thread pools for connector: {}", connectorName);
    clientCreationExecutor.shutdownNow();
    channelIoExecutor.shutdownNow();
  }

  private static final class DaemonThreadFactory implements java.util.concurrent.ThreadFactory {
    private final AtomicInteger counter = new AtomicInteger(0);
    private final String prefix;

    DaemonThreadFactory(String prefix) {
      this.prefix = prefix;
    }

    @Override
    public Thread newThread(Runnable r) {
      Thread t = new Thread(r, prefix + "-" + counter.getAndIncrement());
      t.setDaemon(true);
      return t;
    }
  }
}
