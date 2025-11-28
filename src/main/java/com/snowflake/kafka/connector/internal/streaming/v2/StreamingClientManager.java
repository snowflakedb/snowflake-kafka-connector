package com.snowflake.kafka.connector.internal.streaming.v2;

import static com.google.common.base.Strings.isNullOrEmpty;

import com.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestClientFactory;
import com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams;
import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.internal.KCLogger;
import com.snowflake.kafka.connector.internal.PrivateKeyTool;
import com.snowflake.kafka.connector.internal.SnowflakeURL;
import com.snowflake.kafka.connector.internal.streaming.StreamingClientProperties;
import java.security.PrivateKey;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Manages Snowflake Streaming Ingest clients with two-level isolation:
 *
 * <p>1. Connector-level: Different connectors have completely isolated clients 2. Task-level:
 * Within a connector, clients are shared by tasks but only closed when the last task using them
 * stops
 *
 * <p>This single class replaces the previous Registry + Provider architecture for simplicity.
 */
public final class StreamingClientManager {

  private static final KCLogger LOGGER = new KCLogger(StreamingClientManager.class.getName());

  private static final String STREAMING_CLIENT_V2_PREFIX_NAME = "KC_CLIENT_V2_";
  private static final String DEFAULT_CLIENT_NAME = "DEFAULT_CLIENT";
  private static final AtomicInteger createdClientId = new AtomicInteger(0);

  // Map: connectorName → ConnectorIngestClients
  private static final Map<String, ConnectorIngestClients> connectors = new ConcurrentHashMap<>();

  // Supplier reference is here so that we can swap it to mocked one in the tests
  private static volatile IngestClientSupplier ingestClientSupplier =
      new StandardIngestClientSupplier();

  private StreamingClientManager() {}

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
        .computeIfAbsent(connectorName, k -> new ConnectorIngestClients(connectorName))
        .getClient(taskId, pipeName, connectorConfig, streamingClientProperties);
  }

  public static long getClientCountForTask(final String connectorName, final String taskId) {
    ConnectorIngestClients ingestClients = connectors.get(connectorName);
    if (ingestClients == null) {
      return 0;
    }

    return ingestClients.pipeToTasks.values().stream()
        .filter(tasks -> tasks.contains(taskId))
        .count();
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
      ConnectorIngestClients clients = connectors.get(connectorName);
      if (clients != null) {
        clients.closeTaskClients(taskId);
      } else {
        LOGGER.warn(
            "Attempted to release task {} for unknown connector: {}", taskId, connectorName);
      }
    }
  }

  /** Sets a custom ingest client supplier. This method is used in tests only. */
  public static void setIngestClientSupplier(final IngestClientSupplier supplier) {
    StreamingClientManager.ingestClientSupplier = supplier;
  }

  /** Resets the ingest client supplier to default. This method is used in tests only. */
  public static void resetIngestClientSupplier() {
    StreamingClientManager.ingestClientSupplier = new StandardIngestClientSupplier();
  }

  /**
   * Manages clients for a single connector. Tracks which tasks use which pipes and only closes
   * clients when no tasks are using them.
   */
  private static final class ConnectorIngestClients {
    private final String connectorName;

    // Map: pipeName → Client
    private final Map<String, SnowflakeStreamingIngestClient> clients = new HashMap<>();

    // Map: pipeName → Set of taskIds using this pipe
    private final Map<String, Set<String>> pipeToTasks = new HashMap<>();

    ConnectorIngestClients(final String connectorName) {
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
                return createClient(pipeName, connectorConfig, streamingClientProperties);
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

  private static SnowflakeStreamingIngestClient createClient(
      final String pipeName,
      final Map<String, String> connectorConfig,
      final StreamingClientProperties streamingClientProperties) {

    String clientName = clientName(connectorConfig);
    String dbName = Utils.getDatabase(connectorConfig);
    String schemaName = Utils.getSchema(connectorConfig);

    return ingestClientSupplier.get(
        clientName, dbName, schemaName, pipeName, connectorConfig, streamingClientProperties);
  }

  private static String clientName(final Map<String, String> connectorConfig) {
    return STREAMING_CLIENT_V2_PREFIX_NAME
        + connectorConfig.getOrDefault(KafkaConnectorConfigParams.NAME, DEFAULT_CLIENT_NAME)
        + createdClientId.incrementAndGet();
  }

  static Properties getClientProperties(final Map<String, String> connectorConfig) {
    final Properties props = new Properties();
    SnowflakeURL url =
        new SnowflakeURL(connectorConfig.get(KafkaConnectorConfigParams.SNOWFLAKE_URL_NAME));
    final String privateKeyStr =
        connectorConfig.get(KafkaConnectorConfigParams.SNOWFLAKE_PRIVATE_KEY);
    final String privateKeyPassphrase =
        connectorConfig.get(KafkaConnectorConfigParams.SNOWFLAKE_PRIVATE_KEY_PASSPHRASE);
    final PrivateKey privateKey =
        PrivateKeyTool.parsePrivateKey(privateKeyStr, privateKeyPassphrase);
    final String privateKeyEncoded = Base64.getEncoder().encodeToString(privateKey.getEncoded());
    props.put("private_key", privateKeyEncoded);

    props.put("user", connectorConfig.get(KafkaConnectorConfigParams.SNOWFLAKE_USER_NAME));
    props.put("role", connectorConfig.get(KafkaConnectorConfigParams.SNOWFLAKE_ROLE_NAME));
    props.put("account", url.getAccount());
    props.put("host", url.getUrlWithoutPort());
    return props;
  }

  static final class StandardIngestClientSupplier implements IngestClientSupplier {
    @Override
    public SnowflakeStreamingIngestClient get(
        final String clientName,
        final String dbName,
        final String schemaName,
        final String pipeName,
        final Map<String, String> connectorConfig,
        final StreamingClientProperties streamingClientProperties) {

      return SnowflakeStreamingIngestClientFactory.builder(clientName, dbName, schemaName, pipeName)
          .setProperties(getClientProperties(connectorConfig))
          .setParameterOverrides(streamingClientProperties.parameterOverrides)
          .build();
    }
  }
}
