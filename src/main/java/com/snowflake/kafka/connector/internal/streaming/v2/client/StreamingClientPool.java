package com.snowflake.kafka.connector.internal.streaming.v2.client;

import com.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import com.snowflake.kafka.connector.internal.KCLogger;
import com.snowflake.kafka.connector.internal.streaming.StreamingClientProperties;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages clients for a single connector. Tracks which tasks use which pipes and only closes
 * clients when no tasks are using them.
 *
 * <p>Client creation is performed asynchronously so that multiple pipes can be initialized in
 * parallel. The synchronized sections only cover map bookkeeping; the actual blocking wait for
 * client readiness ({@code future.join()}) happens outside the lock.
 */
public class StreamingClientPool {
  private static final KCLogger LOGGER = new KCLogger(StreamingClientPool.class.getName());
  private final String connectorName;

  // Map: pipeName → Future<Client>
  private final Map<String, CompletableFuture<SnowflakeStreamingIngestClient>> clients =
      new HashMap<>();

  // Map: pipeName → Set of taskIds using this pipe
  private final Map<String, Set<String>> pipeToTasks = new HashMap<>();

  StreamingClientPool(final String connectorName) {
    this.connectorName = connectorName;
    LOGGER.info("Created client manager for connector: {}", connectorName);
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
                    });
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
}
