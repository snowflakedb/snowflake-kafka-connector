package com.snowflake.kafka.connector.internal.streaming.v2.client;

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
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

/** Factory for creating Snowpipe Streaming clients. Shared by all connectors. */
public class StreamingClientFactory {
  private static final KCLogger LOGGER = new KCLogger(StreamingClientFactory.class.getName());

  private static final String STREAMING_CLIENT_V2_PREFIX_NAME = "KC_CLIENT_V2_";
  private static final String DEFAULT_CLIENT_NAME = "DEFAULT_CLIENT";

  // Supplier reference is here so that we can swap it to mocked one in the tests
  private static volatile StreamingClientSupplier ingestClientSupplier =
      new StreamingClientSupplierImpl();

  private static final AtomicInteger createdClientId = new AtomicInteger(0);

  /** Sets a custom ingest client supplier. This method is used in tests only. */
  public static void setStreamingClientSupplier(final StreamingClientSupplier supplier) {
    ingestClientSupplier = supplier;
  }

  /** Resets the ingest client supplier to default. This method is used in tests only. */
  public static void resetStreamingClientSupplier() {
    ingestClientSupplier = new StreamingClientSupplierImpl();
  }

  static SnowflakeStreamingIngestClient createClient(
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

  public static Properties getClientProperties(final Map<String, String> connectorConfig) {
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

  static final class StreamingClientSupplierImpl implements StreamingClientSupplier {
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
