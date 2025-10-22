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
public class StreamingIngestClientProvider {

  private static final String STREAMING_CLIENT_V2_PREFIX_NAME = "KC_CLIENT_V2_";
  private static final String DEFAULT_CLIENT_NAME = "DEFAULT_CLIENT";
  private static int createdClientId = 0;
  private static final IngestClientSupplier DEFAULT_INGEST_CLIENT_SUPPLIER =
      new StandardIngestClientSupplier();
  // this gets replaced during integration testing, we're setting a supplier that produces mocked
  // IngestClient
  private static IngestClientSupplier ingestClientSupplier = DEFAULT_INGEST_CLIENT_SUPPLIER;

  private final Map<String, SnowflakeStreamingIngestClient> pipeToClientMap = new HashMap<>();

  public SnowflakeStreamingIngestClient getClient(
      Map<String, String> connectorConfig,
      String pipeName,
      StreamingClientProperties streamingClientProperties) {
    synchronized (pipeToClientMap) {
      return pipeToClientMap.computeIfAbsent(
          pipeName, k -> createClient(connectorConfig, pipeName, streamingClientProperties));
    }
  }

  public void close(String pipeName) {
    synchronized (pipeToClientMap) {
      Optional.ofNullable(pipeToClientMap.remove(pipeName))
          .ifPresent(SnowflakeStreamingIngestClient::close);
    }
  }

  public void closeAll() {
    synchronized (pipeToClientMap) {
      pipeToClientMap.values().forEach(SnowflakeStreamingIngestClient::close);
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
    return ingestClientSupplier.get(
        clientName, dbName, schemaName, pipeName, connectorConfig, streamingClientProperties);
  }

  private static String clientName(Map<String, String> connectorConfig) {
    createdClientId++;
    return STREAMING_CLIENT_V2_PREFIX_NAME
        + connectorConfig.getOrDefault(Utils.NAME, DEFAULT_CLIENT_NAME)
        + createdClientId;
  }

  static Properties getClientProperties(Map<String, String> connectorConfig) {
    final Properties props = new Properties();
    SnowflakeURL url = new SnowflakeURL(connectorConfig.get(Utils.SF_URL));
    props.put("private_key", connectorConfig.get(Utils.SF_PRIVATE_KEY));
    props.put("user", connectorConfig.get(Utils.SF_USER));
    props.put("role", connectorConfig.get(Utils.SF_ROLE));
    props.put("account", url.getAccount());
    props.put("host", url.getUrlWithoutPort());
    return props;
  }

  /** Sets a custom ingest client supplier. This method is used in tests only. */
  public static void setIngestClientSupplier(final IngestClientSupplier ingestClientSupplier) {

    StreamingIngestClientProvider.ingestClientSupplier = ingestClientSupplier;
  }

  public static void resetIngestClientSupplier() {
    StreamingIngestClientProvider.ingestClientSupplier = DEFAULT_INGEST_CLIENT_SUPPLIER;
  }

  static class StandardIngestClientSupplier implements IngestClientSupplier {

    @Override
    public SnowflakeStreamingIngestClient get(
        final String clientName,
        final String dbName,
        final String schemaName,
        final String pipeName,
        Map<String, String> connectorConfig,
        StreamingClientProperties streamingClientProperties) {
      return SnowflakeStreamingIngestClientFactory.builder(clientName, dbName, schemaName, pipeName)
          .setProperties(getClientProperties(connectorConfig))
          .setParameterOverrides(streamingClientProperties.parameterOverrides)
          .build();
    }
  }
}
