package com.snowflake.kafka.connector.internal;

import com.snowflake.kafka.connector.config.SinkTaskConfig;
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
    private CachingConfig cachingConfig;

    // For testing only
    public Properties getProperties() {
      return this.jdbcProperties.getProperties();
    }

    public SnowflakeConnectionServiceBuilder setTaskID(String taskID) {
      this.taskID = taskID;
      return this;
    }

    public SnowflakeConnectionServiceBuilder setProperties(Map<String, String> conf) {
      return setProperties(SinkTaskConfig.from(conf, true));
    }

    public SnowflakeConnectionServiceBuilder setProperties(SinkTaskConfig parsedConfig) {
      if (parsedConfig.getSnowflakeUrl() == null || parsedConfig.getSnowflakeUrl().isEmpty()) {
        throw SnowflakeErrors.ERROR_0017.getException();
      }
      this.url = new SnowflakeURL(parsedConfig.getSnowflakeUrl());
      this.connectorName = parsedConfig.getConnectorName();
      this.cachingConfig = parsedConfig.getCachingConfig();

      Properties connectionProperties =
          InternalUtils.makeJdbcDriverProperties(parsedConfig, this.url);
      Properties proxyProperties = InternalUtils.generateProxyParametersIfRequired(parsedConfig);
      Properties jdbcPropertiesMap = InternalUtils.parseJdbcPropertiesMap(parsedConfig);
      this.jdbcProperties =
          JdbcProperties.create(connectionProperties, proxyProperties, jdbcPropertiesMap);
      return this;
    }

    public SnowflakeConnectionService build() {
      InternalUtils.assertNotEmpty("jdbcProperties", jdbcProperties);
      InternalUtils.assertNotEmpty("url", url);
      InternalUtils.assertNotEmpty("connectorName", connectorName);

      SnowflakeConnectionService baseService =
          new StandardSnowflakeConnectionService(jdbcProperties, url, connectorName, taskID);

      return new CachingSnowflakeConnectionService(baseService, cachingConfig);
    }
  }
}
