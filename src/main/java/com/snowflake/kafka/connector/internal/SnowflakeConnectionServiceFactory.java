package com.snowflake.kafka.connector.internal;

import com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams;
import java.util.Map;
import java.util.Properties;

public class SnowflakeConnectionServiceFactory {
  public static SnowflakeConnectionServiceBuilder builder() {
    return new SnowflakeConnectionServiceBuilder();
  }

  public static class SnowflakeConnectionServiceBuilder {

    private JdbcProperties jdbcProperties;
    private SnowflakeURL url;
    private String connectorName;
    private String taskID = "-1";

    // Store the full config map for downstream consumers
    private Map<String, String> config;

    // For testing only
    public Properties getProperties() {
      return this.jdbcProperties.getProperties();
    }

    public SnowflakeConnectionServiceBuilder setTaskID(String taskID) {
      this.taskID = taskID;
      return this;
    }

    public SnowflakeConnectionServiceBuilder setProperties(Map<String, String> conf) {
      if (!conf.containsKey(KafkaConnectorConfigParams.SNOWFLAKE_URL_NAME)) {
        throw SnowflakeErrors.ERROR_0017.getException();
      }
      this.url = new SnowflakeURL(conf.get(KafkaConnectorConfigParams.SNOWFLAKE_URL_NAME));
      this.connectorName = conf.get(KafkaConnectorConfigParams.NAME);
      this.config = conf;

      Properties proxyProperties = InternalUtils.generateProxyParametersIfRequired(conf);
      Properties connectionProperties =
          InternalUtils.makeJdbcDriverPropertiesFromConnectorConfiguration(conf, this.url);
      Properties jdbcPropertiesMap = InternalUtils.parseJdbcPropertiesMap(conf);
      this.jdbcProperties =
          JdbcProperties.create(connectionProperties, proxyProperties, jdbcPropertiesMap);
      return this;
    }

    /**
     * Builds a raw {@link SnowflakeConnectionService} without caching or pooling. Used as the
     * delegate inside {@link CachingSnowflakeConnectionService} and as the pool factory.
     */
    public SnowflakeConnectionService build() {
      InternalUtils.assertNotEmpty("jdbcProperties", jdbcProperties);
      InternalUtils.assertNotEmpty("url", url);
      InternalUtils.assertNotEmpty("connectorName", connectorName);

      return new StandardSnowflakeConnectionService(jdbcProperties, url, connectorName, taskID);
    }

    /**
     * Builds a {@link CachingSnowflakeConnectionService} that wraps a raw delegate and a connection
     * pool. This is the standard entry point for production code that needs JDBC access.
     */
    public SnowflakeConnectionService build(
        SnowflakeConnectionPool pool, CachingConfig cachingConfig) {
      SnowflakeConnectionService delegate = build();
      return new CachingSnowflakeConnectionService(delegate, pool, cachingConfig);
    }
  }
}
