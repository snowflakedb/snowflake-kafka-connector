package com.snowflake.kafka.connector.internal;

import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.PROVIDER_CONFIG;
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

    // whether kafka is hosted on premise or on confluent cloud.
    // This info is provided in the connector configuration
    // This property will be appeneded to user agent while calling snowpipe API in http request
    private String kafkaProvider = null;

    // Enable CHANGE_TRACKING on the table after creation
    private boolean enableChangeTracking = false;

    /** Underlying implementation - Check Enum {@link IngestionMethodConfig} */
    private IngestionMethodConfig ingestionMethodConfig;

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
      this.kafkaProvider =
          SnowflakeSinkConnectorConfig.KafkaProvider.of(conf.get(PROVIDER_CONFIG)).name();
      this.connectorName = conf.get(Utils.NAME);
      this.enableChangeTracking =
          Boolean.parseBoolean(
              conf.getOrDefault(
                  SnowflakeSinkConnectorConfig.ENABLE_CHANGE_TRACKING_CONFIG,
                  Boolean.toString(SnowflakeSinkConnectorConfig.ENABLE_CHANGE_TRACKING_DEFAULT)));
      this.ingestionMethodConfig = SNOWPIPE_STREAMING;

      Properties proxyProperties = InternalUtils.generateProxyParametersIfRequired(conf);
      Properties connectionProperties =
          InternalUtils.createProperties(conf, this.url, ingestionMethodConfig);
      Properties jdbcPropertiesMap = InternalUtils.parseJdbcPropertiesMap(conf);
      this.jdbcProperties =
          JdbcProperties.create(connectionProperties, proxyProperties, jdbcPropertiesMap);
      return this;
    }

    public SnowflakeConnectionService build() {
      InternalUtils.assertNotEmpty("jdbcProperties", jdbcProperties);
      InternalUtils.assertNotEmpty("url", url);
      InternalUtils.assertNotEmpty("connectorName", connectorName);
      return new SnowflakeConnectionServiceV1(
          jdbcProperties, url, connectorName, taskID, kafkaProvider, enableChangeTracking);
    }
  }
}
