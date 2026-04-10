package com.snowflake.kafka.connector;

import static com.snowflake.kafka.connector.ConnectorConfigTools.BehaviorOnNullValues.VALIDATOR;
import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.BEHAVIOR_ON_NULL_VALUES;
import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.JMX_OPT;
import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.SNOWFLAKE_PRIVATE_KEY;
import static com.snowflake.kafka.connector.Utils.isValidSnowflakeApplicationName;
import static com.snowflake.kafka.connector.Utils.validateProxySettings;

import com.google.common.collect.ImmutableMap;
import com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams;
import com.snowflake.kafka.connector.internal.KCLogger;
import com.snowflake.kafka.connector.internal.SnowflakeErrors;
import com.snowflake.kafka.connector.internal.streaming.StreamingConfigValidator;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.config.ConfigException;

public class DefaultConnectorConfigValidator implements ConnectorConfigValidator {

  private static final KCLogger LOGGER =
      new KCLogger(DefaultConnectorConfigValidator.class.getName());

  private final StreamingConfigValidator streamingConfigValidator;

  public DefaultConnectorConfigValidator(StreamingConfigValidator streamingConfigValidator) {
    this.streamingConfigValidator = streamingConfigValidator;
  }

  public void validateConfig(Map<String, String> config) {
    Map<String, String> invalidConfigParams = new HashMap<String, String>();

    // define the input parameters / keys in one place as static constants,
    // instead of using them directly
    // define the thresholds statically in one place as static constants,
    // instead of using the values directly

    // unique name of this connector instance
    String connectorName = config.getOrDefault(KafkaConnectorConfigParams.NAME, "");
    if (connectorName.isEmpty() || !isValidSnowflakeApplicationName(connectorName)) {
      invalidConfigParams.put(
          KafkaConnectorConfigParams.NAME,
          Utils.formatString(
              "{} is empty or invalid. It should match Snowflake object identifier syntax. Please"
                  + " see the documentation.",
              KafkaConnectorConfigParams.NAME));
    }

    if (config.containsKey(KafkaConnectorConfigParams.SNOWFLAKE_TOPICS2TABLE_MAP)) {
      try {
        TopicToTableParser.parse(config.get(KafkaConnectorConfigParams.SNOWFLAKE_TOPICS2TABLE_MAP));
      } catch (IllegalArgumentException e) {
        invalidConfigParams.put(
            KafkaConnectorConfigParams.SNOWFLAKE_TOPICS2TABLE_MAP, e.getMessage());
      }
    }

    // sanity check
    if (!config.containsKey(KafkaConnectorConfigParams.SNOWFLAKE_DATABASE_NAME)) {
      invalidConfigParams.put(
          KafkaConnectorConfigParams.SNOWFLAKE_DATABASE_NAME,
          Utils.formatString(
              "{} cannot be empty.", KafkaConnectorConfigParams.SNOWFLAKE_DATABASE_NAME));
    }

    // sanity check
    if (!config.containsKey(KafkaConnectorConfigParams.SNOWFLAKE_SCHEMA_NAME)) {
      invalidConfigParams.put(
          KafkaConnectorConfigParams.SNOWFLAKE_SCHEMA_NAME,
          Utils.formatString(
              "{} cannot be empty.", KafkaConnectorConfigParams.SNOWFLAKE_SCHEMA_NAME));
    }

    if (!config.containsKey(SNOWFLAKE_PRIVATE_KEY)) {
      invalidConfigParams.put(
          SNOWFLAKE_PRIVATE_KEY, Utils.formatString("{} cannot be empty", SNOWFLAKE_PRIVATE_KEY));
    }

    if (!config.containsKey(KafkaConnectorConfigParams.SNOWFLAKE_USER_NAME)) {
      invalidConfigParams.put(
          KafkaConnectorConfigParams.SNOWFLAKE_USER_NAME,
          Utils.formatString(
              "{} cannot be empty.", KafkaConnectorConfigParams.SNOWFLAKE_USER_NAME));
    }

    if (!config.containsKey(KafkaConnectorConfigParams.SNOWFLAKE_URL_NAME)) {
      invalidConfigParams.put(
          KafkaConnectorConfigParams.SNOWFLAKE_URL_NAME,
          Utils.formatString("{} cannot be empty.", KafkaConnectorConfigParams.SNOWFLAKE_URL_NAME));
    }

    if (!config.containsKey(KafkaConnectorConfigParams.SNOWFLAKE_ROLE_NAME)) {
      invalidConfigParams.put(
          KafkaConnectorConfigParams.SNOWFLAKE_ROLE_NAME,
          Utils.formatString(
              "{} cannot be empty.", KafkaConnectorConfigParams.SNOWFLAKE_ROLE_NAME));
    }
    // jvm proxy settings
    invalidConfigParams.putAll(validateProxySettings(config));

    if (config.containsKey(BEHAVIOR_ON_NULL_VALUES)) {
      try {
        // This throws an exception if config value is invalid.
        VALIDATOR.ensureValid(BEHAVIOR_ON_NULL_VALUES, config.get(BEHAVIOR_ON_NULL_VALUES));
      } catch (ConfigException exception) {
        invalidConfigParams.put(
            BEHAVIOR_ON_NULL_VALUES,
            Utils.formatString(
                "Kafka config: {} error: {}", BEHAVIOR_ON_NULL_VALUES, exception.getMessage()));
      }
    }

    if (config.containsKey(JMX_OPT)) {
      if (!(config.get(JMX_OPT).equalsIgnoreCase("true")
          || config.get(JMX_OPT).equalsIgnoreCase("false"))) {
        invalidConfigParams.put(
            JMX_OPT,
            Utils.formatString("Kafka config: {} should either be true or false", JMX_OPT));
      }
    }

    invalidConfigParams.putAll(
        com.snowflake.kafka.connector.internal.CachingConfig.validate(config));

    // Check all config values for ingestion method == IngestionMethodConfig.SNOWPIPE_STREAMING
    invalidConfigParams.putAll(streamingConfigValidator.validate(config));

    // logs and throws exception if there are invalid params
    handleInvalidParameters(ImmutableMap.copyOf(invalidConfigParams));
  }

  private void handleInvalidParameters(ImmutableMap<String, String> invalidConfigParams) {
    // log all invalid params and throw exception
    if (!invalidConfigParams.isEmpty()) {
      String invalidParamsMessage = "";

      for (String invalidKey : invalidConfigParams.keySet()) {
        String invalidValue = invalidConfigParams.get(invalidKey);
        String errorMessage =
            Utils.formatString(
                "Config value '{}' is invalid. Error message: '{}'", invalidKey, invalidValue);
        invalidParamsMessage += errorMessage + "\n";
      }

      LOGGER.error("Invalid config: " + invalidParamsMessage);
      throw SnowflakeErrors.ERROR_0001.getException(invalidParamsMessage);
    }
  }
}
