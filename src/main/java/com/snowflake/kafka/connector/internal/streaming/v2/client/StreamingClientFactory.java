package com.snowflake.kafka.connector.internal.streaming.v2.client;

import com.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestClientFactory;
import com.snowflake.kafka.connector.config.SinkTaskConfig;
import com.snowflake.kafka.connector.internal.KCLogger;
import com.snowflake.kafka.connector.internal.PrivateKeyTool;
import com.snowflake.kafka.connector.internal.SnowflakeURL;
import com.snowflake.kafka.connector.internal.streaming.StreamingClientProperties;
import java.security.PrivateKey;
import java.util.Base64;
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
      final SinkTaskConfig config,
      final StreamingClientProperties streamingClientProperties) {

    String clientName = clientNameFromConfig(config);
    String dbName = config.getSnowflakeDatabase();
    String schemaName = config.getSnowflakeSchema();

    return ingestClientSupplier.get(
        clientName, dbName, schemaName, pipeName, config, streamingClientProperties);
  }

  private static String clientNameFromConfig(final SinkTaskConfig config) {
    return STREAMING_CLIENT_V2_PREFIX_NAME
        + (config.getConnectorName() != null ? config.getConnectorName() : DEFAULT_CLIENT_NAME)
        + createdClientId.incrementAndGet();
  }

  public static Properties getClientProperties(final SinkTaskConfig config) {
    final Properties props = new Properties();
    if (config.getSnowflakeUrl() == null) {
      return props;
    }
    SnowflakeURL url = new SnowflakeURL(config.getSnowflakeUrl());
    final String privateKeyStr = config.getSnowflakePrivateKey();
    final String privateKeyPassphrase = config.getSnowflakePrivateKeyPassphrase();
    final PrivateKey privateKey =
        PrivateKeyTool.parsePrivateKey(privateKeyStr, privateKeyPassphrase);
    final String privateKeyEncoded = Base64.getEncoder().encodeToString(privateKey.getEncoded());
    props.put("private_key", privateKeyEncoded);

    props.put("user", config.getSnowflakeUser());
    props.put("role", config.getSnowflakeRole());
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
        final SinkTaskConfig config,
        final StreamingClientProperties streamingClientProperties) {

      return SnowflakeStreamingIngestClientFactory.builder(clientName, dbName, schemaName, pipeName)
          .setProperties(getClientProperties(config))
          .setParameterOverrides(streamingClientProperties.parameterOverrides)
          .build();
    }
  }
}
