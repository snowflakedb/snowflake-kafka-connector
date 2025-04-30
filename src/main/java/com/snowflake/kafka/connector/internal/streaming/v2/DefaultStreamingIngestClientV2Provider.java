package com.snowflake.kafka.connector.internal.streaming.v2;

import com.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestClientFactory;
import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.internal.SnowflakeURL;
import com.snowflake.kafka.connector.internal.streaming.StreamingClientProperties;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

public class DefaultStreamingIngestClientV2Provider implements StreamingIngestClientV2Provider {

  private static final String STREAMING_CLIENT_V2_PREFIX_NAME = "KC_CLIENT_V2_";
  private static final String DEFAULT_CLIENT_NAME = "DEFAULT_CLIENT";
  private static final AtomicInteger createdClientId = new AtomicInteger(0);

  public SnowflakeStreamingIngestClient getClient(
      Map<String, String> connectorConfig,
      String pipeName,
      StreamingClientProperties streamingClientProperties) {
    String clientName = clientName(connectorConfig);
    String dbName = Utils.database(connectorConfig);
    String schemaName = Utils.schema(connectorConfig);
    return SnowflakeStreamingIngestClientFactory.builder(clientName, dbName, schemaName, pipeName)
        .setProperties(getClientProperties(connectorConfig))
        .setParameterOverrides(streamingClientProperties.parameterOverrides)
        .build();
  }

  private static String clientName(Map<String, String> connectorConfig) {
    return STREAMING_CLIENT_V2_PREFIX_NAME
        + connectorConfig.getOrDefault(Utils.NAME, DEFAULT_CLIENT_NAME)
        + createdClientId.getAndIncrement();
  }

  private static Properties getClientProperties(Map<String, String> connectorConfig) {
    final Properties props = new Properties();
    SnowflakeURL url = new SnowflakeURL(connectorConfig.get(Utils.SF_URL));
    props.put("role", Utils.role(connectorConfig));
    props.put("private_key", connectorConfig.get(Utils.SF_PRIVATE_KEY));
    props.put("user", connectorConfig.get(Utils.SF_USER));
    props.put("account", url.getAccount());
    props.put("host", url.getUrlWithoutPort());
    return props;
  }
}
