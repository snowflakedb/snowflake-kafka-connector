package com.snowflake.kafka.connector.internal.streaming.v2.client;

import com.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import com.snowflake.kafka.connector.internal.KCLogger;
import com.snowflake.kafka.connector.internal.streaming.StreamingClientProperties;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages clients for a single connector. Tracks which tasks use which pipes and only closes
 * clients when no tasks are using them.
 */
public class StreamingClientPool {
  private static final KCLogger LOGGER = new KCLogger(StreamingClientPool.class.getName());
  private final String connectorName;

  // Map: pipeName → Client
  private final Map<String, SnowflakeStreamingIngestClient> clients = new HashMap<>();

  // Map: pipeName → Set of taskIds using this pipe
  private final Map<String, Set<String>> pipeToTasks = new HashMap<>();

  StreamingClientPool(final String connectorName) {
    this.connectorName = connectorName;
    LOGGER.info("Created client manager for connector: {}", connectorName);
  }

  synchronized SnowflakeStreamingIngestClient getClient(
      final String taskId,
      final String pipeName,
      final Map<String, String> connectorConfig,
      final StreamingClientProperties streamingClientProperties) {

    SnowflakeStreamingIngestClient client =
        clients.computeIfAbsent(
            pipeName,
            k -> {
              LOGGER.info(
                  "Creating new streaming client for pipe: {}, connector: {}",
                  pipeName,
                  connectorName);
              return StreamingClientFactory.createClient(
                  pipeName, connectorConfig, streamingClientProperties);
            });

    // Track that this task is using this pipe
    pipeToTasks.computeIfAbsent(pipeName, k -> ConcurrentHashMap.newKeySet()).add(taskId);

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
                // This was the last task using this pipe - close the client
                SnowflakeStreamingIngestClient client = clients.remove(pipeName);
                if (client != null) {
                  LOGGER.info(
                      "Closing client for pipe {} in connector {} (last task stopped)",
                      pipeName,
                      connectorName);
                  client.close();
                }
                return true; // Remove this entry from pipeToTasks
              }
              return false; // Keep this entry
            });
  }
}
