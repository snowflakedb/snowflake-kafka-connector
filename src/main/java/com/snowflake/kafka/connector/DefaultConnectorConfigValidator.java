package com.snowflake.kafka.connector;

import static com.snowflake.kafka.connector.ConnectorConfigTools.BehaviorOnNullValues.VALIDATOR;
import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.BEHAVIOR_ON_NULL_VALUES;
import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.CACHE_PIPE_EXISTS;
import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.CACHE_PIPE_EXISTS_EXPIRE_MS;
import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.CACHE_TABLE_EXISTS;
import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.CACHE_TABLE_EXISTS_EXPIRE_MS;
import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.JMX_OPT;
import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.SNOWFLAKE_PRIVATE_KEY;
import static com.snowflake.kafka.connector.Utils.isValidSnowflakeApplicationName;
import static com.snowflake.kafka.connector.Utils.parseTopicToTableMap;
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

    if (config.containsKey(KafkaConnectorConfigParams.SNOWFLAKE_TOPICS2TABLE_MAP)
        && parseTopicToTableMap(config.get(KafkaConnectorConfigParams.SNOWFLAKE_TOPICS2TABLE_MAP))
            == null) {
      invalidConfigParams.put(
          KafkaConnectorConfigParams.SNOWFLAKE_TOPICS2TABLE_MAP,
          Utils.formatString(
              "Invalid {} config format: {}",
              KafkaConnectorConfigParams.SNOWFLAKE_TOPICS2TABLE_MAP,
              config.get(KafkaConnectorConfigParams.SNOWFLAKE_TOPICS2TABLE_MAP)));
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

    validateCacheConfig(config, invalidConfigParams);

    // Check all config values for ingestion method == IngestionMethodConfig.SNOWPIPE_STREAMING
    invalidConfigParams.putAll(streamingConfigValidator.validate(config));

    // logs and throws exception if there are invalid params
    handleInvalidParameters(ImmutableMap.copyOf(invalidConfigParams));
  }

  private void validateCacheConfig(
      Map<String, String> config, Map<String, String> invalidConfigParams) {
    // Validate table exists cache boolean flag
    if (config.containsKey(CACHE_TABLE_EXISTS)) {
      String value = config.get(CACHE_TABLE_EXISTS);
      if (!isValidBooleanString(value)) {
        invalidConfigParams.put(
            CACHE_TABLE_EXISTS,
            Utils.formatString(
                "{} must be either 'true' or 'false', got: {}", CACHE_TABLE_EXISTS, value));
      }
    }

    // Validate table exists cache expiration
    if (config.containsKey(CACHE_TABLE_EXISTS_EXPIRE_MS)) {
      try {
        long value = Long.parseLong(config.get(CACHE_TABLE_EXISTS_EXPIRE_MS));
        if (value <= 0) {
          invalidConfigParams.put(
              CACHE_TABLE_EXISTS_EXPIRE_MS,
              Utils.formatString(
                  "{} must be a positive number, got: {}", CACHE_TABLE_EXISTS_EXPIRE_MS, value));
        }
      } catch (NumberFormatException e) {
        invalidConfigParams.put(
            CACHE_TABLE_EXISTS_EXPIRE_MS,
            Utils.formatString(
                "{} must be a valid long number, got: {}",
                CACHE_TABLE_EXISTS_EXPIRE_MS,
                config.get(CACHE_TABLE_EXISTS_EXPIRE_MS)));
      }
    }

    // Validate pipe exists cache boolean flag
    if (config.containsKey(CACHE_PIPE_EXISTS)) {
      String value = config.get(CACHE_PIPE_EXISTS);
      if (!isValidBooleanString(value)) {
        invalidConfigParams.put(
            CACHE_PIPE_EXISTS,
            Utils.formatString(
                "{} must be either 'true' or 'false', got: {}", CACHE_PIPE_EXISTS, value));
      }
    }

    // Validate pipe exists cache expiration
    if (config.containsKey(CACHE_PIPE_EXISTS_EXPIRE_MS)) {
      try {
        long value = Long.parseLong(config.get(CACHE_PIPE_EXISTS_EXPIRE_MS));
        if (value <= 0) {
          invalidConfigParams.put(
              CACHE_PIPE_EXISTS_EXPIRE_MS,
              Utils.formatString(
                  "{} must be a positive number, got: {}", CACHE_PIPE_EXISTS_EXPIRE_MS, value));
        }
      } catch (NumberFormatException e) {
        invalidConfigParams.put(
            CACHE_PIPE_EXISTS_EXPIRE_MS,
            Utils.formatString(
                "{} must be a valid long number, got: {}",
                CACHE_PIPE_EXISTS_EXPIRE_MS,
                config.get(CACHE_PIPE_EXISTS_EXPIRE_MS)));
      }
    }
  }

  private static boolean isValidBooleanString(String value) {
    return "true".equalsIgnoreCase(value) || "false".equalsIgnoreCase(value);
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
