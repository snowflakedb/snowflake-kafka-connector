package com.snowflake.kafka.connector.config;

import com.snowflake.kafka.connector.internal.SnowflakeKafkaConnectorException;
import com.snowflake.kafka.connector.internal.streaming.StreamingClientOverrides;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

/**
 * Validates that the streaming client override map is a list of {@code key:value} pairs whose keys
 * are Snowpipe Streaming SDK tuning parameters. The rejected value is not echoed back because it is
 * user input that may contain secrets or line breaks.
 */
class StreamingClientOverrideMapValidator implements ConfigDef.Validator {

  @Override
  public void ensureValid(String name, Object value) {
    try {
      StreamingClientOverrides.parse((String) value);
    } catch (SnowflakeKafkaConnectorException e) {
      throw new ConfigException(name + ": " + e.getMessage());
    }
  }

  @Override
  public String toString() {
    return "Comma-separated key:value pairs of Snowpipe Streaming SDK tuning parameters";
  }
}
