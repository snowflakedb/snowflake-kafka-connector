package com.snowflake.kafka.connector.config;

import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.PROVIDER_CONFIG;

import com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

/** Validator to validate Kafka Provider values which says where kafka is hosted */
class KafkaProviderValidator implements ConfigDef.Validator {
  public KafkaProviderValidator() {}

  // This API is called by framework to ensure the validity when connector is started or when a
  // validate REST API is called
  @Override
  public void ensureValid(String name, Object value) {
    assert value instanceof String;
    final String strValue = (String) value;
    // The value can be null or empty.
    try {
      SnowflakeSinkConnectorConfig.KafkaProvider kafkaProvider =
          SnowflakeSinkConnectorConfig.KafkaProvider.of(strValue);
    } catch (final IllegalArgumentException e) {
      throw new ConfigException(PROVIDER_CONFIG, value, e.getMessage());
    }
  }

  public String toString() {
    return "Whether kafka is running on Confluent code, self hosted or other managed service."
        + " Allowed values are:"
        + String.join(",", SnowflakeSinkConnectorConfig.KafkaProvider.PROVIDER_NAMES);
  }
}
