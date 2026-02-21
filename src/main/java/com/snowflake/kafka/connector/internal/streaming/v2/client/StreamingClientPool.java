package com.snowflake.kafka.connector.internal.streaming.v2.client;

import com.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams;
import com.snowflake.kafka.connector.internal.KCLogger;
import com.snowflake.kafka.connector.internal.streaming.StreamingClientProperties;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Manages clients for a single connector. Tracks which tasks use which pipes and only closes
 * clients when no tasks are using them.
 *
 * <p>Client creation is performed asynchronously on a dedicated thread pool so that multiple pipes
 * can be initialized in parallel. A second, larger, configurable pool is exposed for channel I/O
 * work (opening channels, status checks, flushes). Keeping the pools separate prevents channel work
 * from starving client creation.
 */
public class StreamingClientPool {
  private static final KCLogger LOGGER = new KCLogger(StreamingClientPool.class.getName());
  private static final int CLIENT_CREATION_THREADS = 4;

  private final String connectorName;

  // Map: pipeName → Future<Client>
  private final Map<String, CompletableFuture<SnowflakeStreamingIngestClient>> clients =
      new HashMap<>();

  // Map: pipeName → Set of taskIds using this pipe
  private final Map<String, Set<String>> pipeToTasks = new HashMap<>();

  private final ExecutorService clientCreationExecutor;
  private final ExecutorService channelIoExecutor;

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
            CLIENT_CREATION_THREADS,
            new DaemonThreadFactory(connectorName + "-client-create"));
    this.channelIoExecutor =
        Executors.newFixedThreadPool(
            maxIoThreads,
            new DaemonThreadFactory(connectorName + "-io"));

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
      final StreamingClientProperties streamingClientProperties) {

    CompletableFuture<SnowflakeStreamingIngestClient> future;

    synchronized (this) {
      future =
          clients.computeIfAbsent(
              pipeName,
              k -> {
                LOGGER.info(
                    "Creating new streaming client for pipe: {}, connector: {}",
                    pipeName,
                    connectorName);
                return CompletableFuture.supplyAsync(
                    () -> {
                      long startMs = System.currentTimeMillis();
                      SnowflakeStreamingIngestClient created =
                          StreamingClientFactory.createClient(
                              pipeName, connectorConfig, streamingClientProperties);
                      long elapsedMs = System.currentTimeMillis() - startMs;
                      LOGGER.info(
                          "Streaming client created for pipe: {}, connector: {} in {} ms",
                          pipeName,
                          connectorName,
                          elapsedMs);
                      return created;
                    },
                    clientCreationExecutor);
              });

      pipeToTasks.computeIfAbsent(pipeName, k -> ConcurrentHashMap.newKeySet()).add(taskId);
    }

    // Block until this specific client is ready — lock is NOT held, so other pipes can proceed
    SnowflakeStreamingIngestClient client;
    try {
      client = future.join();
    } catch (Exception e) {
      synchronized (this) {
        clients.remove(pipeName, future);
      }
      throw e;
    }

    LOGGER.info(
        "Task {} now using pipe {} for connector {}, total tasks on this pipe: {}",
        taskId,
        pipeName,
        connectorName,
        pipeToTasks.get(pipeName).size());

    return client;
  }

  synchronized long getClientCountForTask(final String taskId) {
    return pipeToTasks.values().stream().filter(tasks -> tasks.contains(taskId)).count();
  }

  synchronized void closeTaskClients(final String taskId) {
    LOGGER.info("Releasing clients for task {} in connector {}", taskId, connectorName);

    pipeToTasks
        .entrySet()
        .removeIf(
            entry -> {
              String pipeName = entry.getKey();
              Set<String> tasks = entry.getValue();

              if (tasks.remove(taskId) && tasks.isEmpty()) {
                CompletableFuture<SnowflakeStreamingIngestClient> future =
                    clients.remove(pipeName);
                if (future != null) {
                  future.thenAccept(
                      client -> {
                        LOGGER.info(
                            "Closing client for pipe {} in connector {} (last task stopped)",
                            pipeName,
                            connectorName);
                        client.close();
                      });
                }
                return true;
              }
              return false;
            });
  }

  /** Returns true if there are no remaining clients or task registrations. */
  synchronized boolean isEmpty() {
    return clients.isEmpty() && pipeToTasks.isEmpty();
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
