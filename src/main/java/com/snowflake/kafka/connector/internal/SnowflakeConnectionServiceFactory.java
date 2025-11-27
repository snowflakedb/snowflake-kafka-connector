package com.snowflake.kafka.connector.internal;

import static com.snowflake.kafka.connector.internal.streaming.IngestionMethodConfig.SNOWPIPE_STREAMING;

import com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig;
import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.internal.streaming.IngestionMethodConfig;
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

    // Enable CHANGE_TRACKING on the table after creation
    private boolean enableChangeTracking = false;

    /** Underlying implementation - Check Enum {@link IngestionMethodConfig} */
    private IngestionMethodConfig ingestionMethodConfig;

    // Store the full config map to pass to connection service for caching configuration
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
      if (!conf.containsKey(Utils.SF_URL)) {
        throw SnowflakeErrors.ERROR_0017.getException();
      }
      this.url = new SnowflakeURL(conf.get(Utils.SF_URL));
      this.connectorName = conf.get(Utils.NAME);
      this.config = conf;
      this.enableChangeTracking =
          Boolean.parseBoolean(
              conf.getOrDefault(
                  SnowflakeSinkConnectorConfig.ENABLE_CHANGE_TRACKING_CONFIG,
                  Boolean.toString(SnowflakeSinkConnectorConfig.ENABLE_CHANGE_TRACKING_DEFAULT)));

      Properties proxyProperties = InternalUtils.generateProxyParametersIfRequired(conf);
      Properties connectionProperties =
          InternalUtils.makeJdbcDriverPropertiesFromConnectorConfiguration(
              conf, this.url, SNOWPIPE_STREAMING);
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
          new StandardSnowflakeConnectionService(
              jdbcProperties, url, connectorName, taskID, enableChangeTracking);

      CachingConfig cachingConfig = CachingConfig.fromConfig(config);
      return new CachingSnowflakeConnectionService(baseService, cachingConfig);
    }
  }
}
