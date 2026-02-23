package com.snowflake.kafka.connector.internal.streaming.v2.client;

import static com.google.common.base.Strings.isNullOrEmpty;

import com.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import com.snowflake.kafka.connector.internal.KCLogger;
import com.snowflake.kafka.connector.internal.streaming.StreamingClientProperties;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

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
   * @param connectorConfig connector configuration
   * @param streamingClientProperties streaming client properties
   * @return the client for this pipe
   * @throws IllegalArgumentException if connectorName, taskId, or pipeName is null or empty
   */
  public static SnowflakeStreamingIngestClient getClient(
      final String connectorName,
      final String taskId,
      final String pipeName,
      final Map<String, String> connectorConfig,
      final StreamingClientProperties streamingClientProperties) {

    // Validate inputs
    if (isNullOrEmpty(connectorName)) {
      throw new IllegalArgumentException("connectorName cannot be null or empty");
    }
    if (isNullOrEmpty(taskId)) {
      throw new IllegalArgumentException("taskId cannot be null or empty");
    }
    if (isNullOrEmpty(pipeName)) {
      throw new IllegalArgumentException("pipeName cannot be null or empty");
    }

    return connectors
        .computeIfAbsent(connectorName, k -> new StreamingClientPool(connectorName))
        .getClient(taskId, pipeName, connectorConfig, streamingClientProperties);
  }

  public static long getClientCountForTask(final String connectorName, final String taskId) {
    StreamingClientPool pool = connectors.get(connectorName);
    if (pool == null) {
      return 0;
    }

    return pool.getClientCountForTask(taskId);
  }

  /**
   * Releases all clients used by a specific task. Clients that are still used by other tasks remain
   * open. Only closes clients when the last task using them stops.
   *
   * @param connectorName the name of the connector
   * @param taskId the ID of the task
   */
  public static void closeTaskClients(final String connectorName, final String taskId) {
    synchronized (connectors) {
      StreamingClientPool pool = connectors.get(connectorName);
      if (pool != null) {
        pool.closeTaskClients(taskId);
      } else {
        LOGGER.warn(
            "Attempted to release task {} for unknown connector: {}", taskId, connectorName);
      }
    }
  }
}
