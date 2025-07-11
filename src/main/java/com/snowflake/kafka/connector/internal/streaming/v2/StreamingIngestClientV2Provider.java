package com.snowflake.kafka.connector.internal.streaming.v2;

import com.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestClientFactory;
import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.internal.SnowflakeURL;
import com.snowflake.kafka.connector.internal.streaming.StreamingClientProperties;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

/**
 * Lazily creates SnowflakeStreamingIngestClient instances. Only one instance per pipe/table is
 * created.
 *
 * <p>Synchronization is required because multiple Sink Task instances may run on the same worker.
 * Synchronization via ConcurrentHashMap is not enough because more than one operation is called
 * within single method.
 */
public class StreamingIngestClientV2Provider {

  private static final String STREAMING_CLIENT_V2_PREFIX_NAME = "KC_CLIENT_V2_";
  private static final String DEFAULT_CLIENT_NAME = "DEFAULT_CLIENT";
  private static int createdClientId = 0;

  private final Map<String, SnowflakeStreamingIngestClient> pipeToClientMap = new HashMap<>();

  public SnowflakeStreamingIngestClient getClient(
      Map<String, String> connectorConfig,
      String pipeName,
      StreamingClientProperties streamingClientProperties) {
    synchronized (pipeToClientMap) {
      Optional<SnowflakeStreamingIngestClient> existingClient =
          Optional.ofNullable(pipeToClientMap.get(pipeName)).filter(client -> !client.isClosed());

      return existingClient.orElseGet(
          () -> {
            SnowflakeStreamingIngestClient newClient =
                createClient(connectorConfig, pipeName, streamingClientProperties);
            pipeToClientMap.put(pipeName, newClient);
            return newClient;
          });
    }
  }

  public void close(String pipeName) {
    synchronized (pipeToClientMap) {
      Optional.ofNullable(pipeToClientMap.get(pipeName))
          .ifPresent(SnowflakeStreamingIngestClient::close);
      pipeToClientMap.remove(pipeName);
    }
  }

  public void closeAll() {
    synchronized (pipeToClientMap) {
      for (Map.Entry<String, SnowflakeStreamingIngestClient> entry : pipeToClientMap.entrySet()) {
        entry.getValue().close();
      }
      pipeToClientMap.clear();
    }
  }

  private SnowflakeStreamingIngestClient createClient(
      Map<String, String> connectorConfig,
      String pipeName,
      StreamingClientProperties streamingClientProperties) {
    String clientName = clientName(connectorConfig);
    String dbName = Utils.getDatabase(connectorConfig);
    String schemaName = Utils.getSchema(connectorConfig);
    return SnowflakeStreamingIngestClientFactory.builder(clientName, dbName, schemaName, pipeName)
        .setProperties(getClientProperties(connectorConfig))
        .setParameterOverrides(streamingClientProperties.parameterOverrides)
        .build();
  }

  private static String clientName(Map<String, String> connectorConfig) {
    createdClientId++;
    return STREAMING_CLIENT_V2_PREFIX_NAME
        + connectorConfig.getOrDefault(Utils.NAME, DEFAULT_CLIENT_NAME)
        + createdClientId;
  }

  private static Properties getClientProperties(Map<String, String> connectorConfig) {
    final Properties props = new Properties();
    SnowflakeURL url = new SnowflakeURL(connectorConfig.get(Utils.SF_URL));
    props.put("private_key", connectorConfig.get(Utils.SF_PRIVATE_KEY));
    props.put("user", connectorConfig.get(Utils.SF_USER));
    props.put("account", url.getAccount());
    props.put("host", url.getUrlWithoutPort());
    return props;
  }
}
