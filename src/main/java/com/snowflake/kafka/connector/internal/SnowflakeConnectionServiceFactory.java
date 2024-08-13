package com.snowflake.kafka.connector.internal;

import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.PROVIDER_CONFIG;

import com.google.common.annotations.VisibleForTesting;
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

    /** Underlying implementation - Check Enum {@link IngestionMethodConfig} */
    private IngestionMethodConfig ingestionMethodConfig;

    @VisibleForTesting
    public SnowflakeConnectionServiceBuilder setProperties(Properties connectionProperties) {
      this.jdbcProperties = JdbcProperties.create(connectionProperties);
      this.ingestionMethodConfig = IngestionMethodConfig.SNOWPIPE;
      return this;
    }

    // For testing only
    public Properties getProperties() {
      return this.jdbcProperties.getProperties();
    }

    public SnowflakeConnectionServiceBuilder setURL(SnowflakeURL url) {
      this.url = url;
      return this;
    }

    public SnowflakeConnectionServiceBuilder setConnectorName(String name) {
      this.connectorName = name;
      return this;
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
      this.ingestionMethodConfig = IngestionMethodConfig.determineIngestionMethod(conf);

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
          jdbcProperties, url, connectorName, taskID, kafkaProvider, ingestionMethodConfig);
    }
  }
}
