package com.snowflake.kafka.connector.internal;

import com.snowflake.kafka.connector.config.CachingConfig;
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
      return setProperties(SinkTaskConfig.from(conf, true), conf);
    }

    /**
     * @param conf raw config map, still needed for proxy settings and {@code snowflake.jdbc.*}
     *     passthrough properties that are not modeled in {@link SinkTaskConfig}.
     */
    public SnowflakeConnectionServiceBuilder setProperties(
        SinkTaskConfig parsedConfig, Map<String, String> conf) {
      if (parsedConfig.getSnowflakeUrl() == null || parsedConfig.getSnowflakeUrl().isEmpty()) {
        throw SnowflakeErrors.ERROR_0017.getException();
      }
      this.url = new SnowflakeURL(parsedConfig.getSnowflakeUrl());
      this.connectorName = parsedConfig.getConnectorName();
      this.cachingConfig = parsedConfig.getCachingConfig();

      Properties proxyProperties = InternalUtils.generateProxyParametersIfRequired(conf);
      Properties connectionProperties =
          InternalUtils.makeJdbcDriverProperties(parsedConfig, this.url);
      Properties jdbcPropertiesMap = InternalUtils.parseJdbcPropertiesMap(conf);
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
