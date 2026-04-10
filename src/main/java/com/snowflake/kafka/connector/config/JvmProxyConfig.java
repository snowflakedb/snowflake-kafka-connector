package com.snowflake.kafka.connector.config;

import com.google.auto.value.AutoValue;
import com.snowflake.kafka.connector.ConnectorConfigTools;
import com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Typed proxy configuration parsed from the raw connector config map. Usable both at the connector
 * level (before a task ID exists) and embedded in {@link SinkTaskConfig}.
 */
@AutoValue
public abstract class JvmProxyConfig {

  @Nullable
  public abstract String getHost();

  @Nullable
  public abstract String getPort();

  @Nullable
  public abstract String getNonProxyHosts();

  @Nullable
  public abstract String getUsername();

  @Nullable
  public abstract String getPassword();

  /** Parse proxy settings from a raw connector config map. */
  public static JvmProxyConfig from(Map<String, String> config) {
    return builder()
        .host(ConnectorConfigTools.getProperty(config, KafkaConnectorConfigParams.JVM_PROXY_HOST))
        .port(ConnectorConfigTools.getProperty(config, KafkaConnectorConfigParams.JVM_PROXY_PORT))
        .nonProxyHosts(
            ConnectorConfigTools.getProperty(
                config, KafkaConnectorConfigParams.JVM_NON_PROXY_HOSTS))
        .username(
            ConnectorConfigTools.getProperty(config, KafkaConnectorConfigParams.JVM_PROXY_USERNAME))
        .password(
            ConnectorConfigTools.getProperty(config, KafkaConnectorConfigParams.JVM_PROXY_PASSWORD))
        .build();
  }

  public static Builder builder() {
    return new AutoValue_JvmProxyConfig.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder host(String host);

    public abstract Builder port(String port);

    public abstract Builder nonProxyHosts(String nonProxyHosts);

    public abstract Builder username(String username);

    public abstract Builder password(String password);

    public abstract JvmProxyConfig build();
  }
}
