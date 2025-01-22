package com.snowflake.kafka.connector;

import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.*;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.BehaviorOnNullValues.VALIDATOR;
import static com.snowflake.kafka.connector.Utils.*;

import com.google.common.collect.ImmutableMap;
import com.snowflake.kafka.connector.internal.BufferThreshold;
import com.snowflake.kafka.connector.internal.KCLogger;
import com.snowflake.kafka.connector.internal.SnowflakeErrors;
import com.snowflake.kafka.connector.internal.streaming.IngestionMethodConfig;
import com.snowflake.kafka.connector.internal.streaming.StreamingConfigValidator;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.config.ConfigException;

public class DefaultConnectorConfigValidator implements ConnectorConfigValidator {

  private static final KCLogger LOGGER =
      new KCLogger(DefaultConnectorConfigValidator.class.getName());

  private final StreamingConfigValidator streamingConfigValidator;
  private final StreamingConfigValidator icebergConfigValidator;

  public DefaultConnectorConfigValidator(
      StreamingConfigValidator streamingConfigValidator,
      StreamingConfigValidator icebergConfigValidator) {
    this.streamingConfigValidator = streamingConfigValidator;
    this.icebergConfigValidator = icebergConfigValidator;
  }

  /**
   * Validate input configuration
   *
   * @param config configuration Map
   * @return connector name
   */
  public String validateConfig(Map<String, String> config) {
    Map<String, String> invalidConfigParams = new HashMap<String, String>();

    // define the input parameters / keys in one place as static constants,
    // instead of using them directly
    // define the thresholds statically in one place as static constants,
    // instead of using the values directly

    // unique name of this connector instance
    String connectorName = config.getOrDefault(SnowflakeSinkConnectorConfig.NAME, "");
    if (connectorName.isEmpty() || !isValidSnowflakeApplicationName(connectorName)) {
      invalidConfigParams.put(
          SnowflakeSinkConnectorConfig.NAME,
          Utils.formatString(
              "{} is empty or invalid. It should match Snowflake object identifier syntax. Please"
                  + " see the documentation.",
              SnowflakeSinkConnectorConfig.NAME));
    }

    // If config doesnt have ingestion method defined, default is snowpipe or if snowpipe is
    // explicitly passed in as ingestion method
    // Below checks are just for snowpipe.
    if (isSnowpipeIngestion(config)) {
      invalidConfigParams.putAll(
          BufferThreshold.validateBufferThreshold(config, IngestionMethodConfig.SNOWPIPE));

      if (config.containsKey(SnowflakeSinkConnectorConfig.ENABLE_SCHEMATIZATION_CONFIG)
          && Boolean.parseBoolean(
              config.get(SnowflakeSinkConnectorConfig.ENABLE_SCHEMATIZATION_CONFIG))) {
        invalidConfigParams.put(
            SnowflakeSinkConnectorConfig.ENABLE_SCHEMATIZATION_CONFIG,
            Utils.formatString(
                "Schematization is only available with {}.",
                IngestionMethodConfig.SNOWPIPE_STREAMING.toString()));
      }
      if (config.containsKey(SnowflakeSinkConnectorConfig.SNOWPIPE_STREAMING_MAX_CLIENT_LAG)) {
        invalidConfigParams.put(
            SnowflakeSinkConnectorConfig.SNOWPIPE_STREAMING_MAX_CLIENT_LAG,
            Utils.formatString(
                "{} is only available with ingestion type: {}.",
                SnowflakeSinkConnectorConfig.SNOWPIPE_STREAMING_MAX_CLIENT_LAG,
                IngestionMethodConfig.SNOWPIPE_STREAMING.toString()));
      }
      if (config.containsKey(
          SnowflakeSinkConnectorConfig.SNOWPIPE_STREAMING_MAX_MEMORY_LIMIT_IN_BYTES)) {
        invalidConfigParams.put(
            SnowflakeSinkConnectorConfig.SNOWPIPE_STREAMING_MAX_MEMORY_LIMIT_IN_BYTES,
            Utils.formatString(
                "{} is only available with ingestion type: {}.",
                SnowflakeSinkConnectorConfig.SNOWPIPE_STREAMING_MAX_MEMORY_LIMIT_IN_BYTES,
                IngestionMethodConfig.SNOWPIPE_STREAMING.toString()));
      }
      if (config.containsKey(
          SnowflakeSinkConnectorConfig.SNOWPIPE_STREAMING_ENABLE_SINGLE_BUFFER)) {
        invalidConfigParams.put(
            SnowflakeSinkConnectorConfig.SNOWPIPE_STREAMING_ENABLE_SINGLE_BUFFER,
            Utils.formatString(
                "{} is only available with ingestion type: {}.",
                SnowflakeSinkConnectorConfig.SNOWPIPE_STREAMING_ENABLE_SINGLE_BUFFER,
                IngestionMethodConfig.SNOWPIPE_STREAMING.toString()));
      }
      if (config.containsKey(
          SnowflakeSinkConnectorConfig.SNOWPIPE_STREAMING_CLIENT_PROVIDER_OVERRIDE_MAP)) {
        invalidConfigParams.put(
            SnowflakeSinkConnectorConfig.SNOWPIPE_STREAMING_CLIENT_PROVIDER_OVERRIDE_MAP,
            Utils.formatString(
                "{} is only available with ingestion type: {}.",
                SnowflakeSinkConnectorConfig.SNOWPIPE_STREAMING_CLIENT_PROVIDER_OVERRIDE_MAP,
                IngestionMethodConfig.SNOWPIPE_STREAMING.toString()));
      }
      if (config.containsKey(
              SnowflakeSinkConnectorConfig.ENABLE_STREAMING_CLIENT_OPTIMIZATION_CONFIG)
          && Boolean.parseBoolean(
              config.get(
                  SnowflakeSinkConnectorConfig.ENABLE_STREAMING_CLIENT_OPTIMIZATION_CONFIG))) {
        invalidConfigParams.put(
            SnowflakeSinkConnectorConfig.ENABLE_STREAMING_CLIENT_OPTIMIZATION_CONFIG,
            Utils.formatString(
                "Streaming client optimization is only available with {}.",
                IngestionMethodConfig.SNOWPIPE_STREAMING.toString()));
      }
      if (config.containsKey(
          SnowflakeSinkConnectorConfig.ENABLE_CHANNEL_OFFSET_TOKEN_MIGRATION_CONFIG)) {
        invalidConfigParams.put(
            SnowflakeSinkConnectorConfig.ENABLE_CHANNEL_OFFSET_TOKEN_MIGRATION_CONFIG,
            Utils.formatString(
                "Streaming client Channel migration is only available with {}.",
                IngestionMethodConfig.SNOWPIPE_STREAMING.toString()));
      }
      if (config.containsKey(
          SnowflakeSinkConnectorConfig.ENABLE_CHANNEL_OFFSET_TOKEN_VERIFICATION_FUNCTION_CONFIG)) {
        invalidConfigParams.put(
            SnowflakeSinkConnectorConfig.ENABLE_CHANNEL_OFFSET_TOKEN_VERIFICATION_FUNCTION_CONFIG,
            Utils.formatString(
                "Streaming channel offset verification function is only available with {}.",
                IngestionMethodConfig.SNOWPIPE_STREAMING.toString()));
      }
    }

    if (config.containsKey(SnowflakeSinkConnectorConfig.TOPICS_TABLES_MAP)
        && parseTopicToTableMap(config.get(SnowflakeSinkConnectorConfig.TOPICS_TABLES_MAP))
            == null) {
      invalidConfigParams.put(
          SnowflakeSinkConnectorConfig.TOPICS_TABLES_MAP,
          Utils.formatString(
              "Invalid {} config format: {}",
              SnowflakeSinkConnectorConfig.TOPICS_TABLES_MAP,
              config.get(SnowflakeSinkConnectorConfig.TOPICS_TABLES_MAP)));
    }

    // sanity check
    if (!config.containsKey(SnowflakeSinkConnectorConfig.SNOWFLAKE_DATABASE)) {
      invalidConfigParams.put(
          SnowflakeSinkConnectorConfig.SNOWFLAKE_DATABASE,
          Utils.formatString(
              "{} cannot be empty.", SnowflakeSinkConnectorConfig.SNOWFLAKE_DATABASE));
    }

    // sanity check
    if (!config.containsKey(SnowflakeSinkConnectorConfig.SNOWFLAKE_SCHEMA)) {
      invalidConfigParams.put(
          SnowflakeSinkConnectorConfig.SNOWFLAKE_SCHEMA,
          Utils.formatString("{} cannot be empty.", SnowflakeSinkConnectorConfig.SNOWFLAKE_SCHEMA));
    }

    switch (config
        .getOrDefault(SnowflakeSinkConnectorConfig.AUTHENTICATOR_TYPE, Utils.SNOWFLAKE_JWT)
        .toLowerCase()) {
        // TODO: SNOW-889748 change to enum
      case Utils.SNOWFLAKE_JWT:
        if (!config.containsKey(SnowflakeSinkConnectorConfig.SNOWFLAKE_PRIVATE_KEY)) {
          invalidConfigParams.put(
              SnowflakeSinkConnectorConfig.SNOWFLAKE_PRIVATE_KEY,
              Utils.formatString(
                  "{} cannot be empty when using {} authenticator.",
                  SnowflakeSinkConnectorConfig.SNOWFLAKE_PRIVATE_KEY,
                  Utils.SNOWFLAKE_JWT));
        }
        break;
      case Utils.OAUTH:
        if (!config.containsKey(SnowflakeSinkConnectorConfig.OAUTH_CLIENT_ID)) {
          invalidConfigParams.put(
              SnowflakeSinkConnectorConfig.OAUTH_CLIENT_ID,
              Utils.formatString(
                  "{} cannot be empty when using {} authenticator.",
                  SnowflakeSinkConnectorConfig.OAUTH_CLIENT_ID,
                  Utils.OAUTH));
        }
        if (!config.containsKey(SnowflakeSinkConnectorConfig.OAUTH_CLIENT_SECRET)) {
          invalidConfigParams.put(
              SnowflakeSinkConnectorConfig.OAUTH_CLIENT_SECRET,
              Utils.formatString(
                  "{} cannot be empty when using {} authenticator.",
                  SnowflakeSinkConnectorConfig.OAUTH_CLIENT_SECRET,
                  Utils.OAUTH));
        }
        if (!config.containsKey(SnowflakeSinkConnectorConfig.OAUTH_REFRESH_TOKEN)) {
          invalidConfigParams.put(
              SnowflakeSinkConnectorConfig.OAUTH_REFRESH_TOKEN,
              Utils.formatString(
                  "{} cannot be empty when using {} authenticator.",
                  SnowflakeSinkConnectorConfig.OAUTH_REFRESH_TOKEN,
                  Utils.OAUTH));
        }
        break;
      default:
        invalidConfigParams.put(
            SnowflakeSinkConnectorConfig.AUTHENTICATOR_TYPE,
            Utils.formatString(
                "{} should be one of {} or {}.",
                SnowflakeSinkConnectorConfig.AUTHENTICATOR_TYPE,
                Utils.SNOWFLAKE_JWT,
                Utils.OAUTH));
    }

    if (!config.containsKey(SnowflakeSinkConnectorConfig.SNOWFLAKE_USER)) {
      invalidConfigParams.put(
          SnowflakeSinkConnectorConfig.SNOWFLAKE_USER,
          Utils.formatString("{} cannot be empty.", SnowflakeSinkConnectorConfig.SNOWFLAKE_USER));
    }

    if (!config.containsKey(SnowflakeSinkConnectorConfig.SNOWFLAKE_URL)) {
      invalidConfigParams.put(
          SnowflakeSinkConnectorConfig.SNOWFLAKE_URL,
          Utils.formatString("{} cannot be empty.", SnowflakeSinkConnectorConfig.SNOWFLAKE_URL));
    }
    // jvm proxy settings
    invalidConfigParams.putAll(validateProxySettings(config));

    // set jdbc logging directory
    Utils.setJDBCLoggingDirectory();

    // validate whether kafka provider config is a valid value
    if (config.containsKey(SnowflakeSinkConnectorConfig.PROVIDER_CONFIG)) {
      try {
        SnowflakeSinkConnectorConfig.KafkaProvider.of(
            config.get(SnowflakeSinkConnectorConfig.PROVIDER_CONFIG));
      } catch (IllegalArgumentException exception) {
        invalidConfigParams.put(
            SnowflakeSinkConnectorConfig.PROVIDER_CONFIG,
            Utils.formatString("Kafka provider config error:{}", exception.getMessage()));
      }
    }

    if (config.containsKey(BEHAVIOR_ON_NULL_VALUES_CONFIG)) {
      try {
        // This throws an exception if config value is invalid.
        VALIDATOR.ensureValid(
            BEHAVIOR_ON_NULL_VALUES_CONFIG, config.get(BEHAVIOR_ON_NULL_VALUES_CONFIG));
      } catch (ConfigException exception) {
        invalidConfigParams.put(
            BEHAVIOR_ON_NULL_VALUES_CONFIG,
            Utils.formatString(
                "Kafka config:{} error:{}",
                BEHAVIOR_ON_NULL_VALUES_CONFIG,
                exception.getMessage()));
      }
    }

    if (config.containsKey(JMX_OPT)) {
      if (!(config.get(JMX_OPT).equalsIgnoreCase("true")
          || config.get(JMX_OPT).equalsIgnoreCase("false"))) {
        invalidConfigParams.put(
            JMX_OPT, Utils.formatString("Kafka config:{} should either be true or false", JMX_OPT));
      }
    }

    if (config.containsKey(SNOWPIPE_ENABLE_REPROCESS_FILES_CLEANUP)) {
      if (!(config.get(SNOWPIPE_ENABLE_REPROCESS_FILES_CLEANUP).equalsIgnoreCase("true")
          || config.get(SNOWPIPE_ENABLE_REPROCESS_FILES_CLEANUP).equalsIgnoreCase("false"))) {
        invalidConfigParams.put(
            SNOWPIPE_ENABLE_REPROCESS_FILES_CLEANUP,
            Utils.formatString(
                "Kafka config:{} should either be true or false",
                SNOWPIPE_ENABLE_REPROCESS_FILES_CLEANUP));
      }
    }

    // Check all config values for ingestion method == IngestionMethodConfig.SNOWPIPE_STREAMING
    invalidConfigParams.putAll(streamingConfigValidator.validate(config));
    invalidConfigParams.putAll(icebergConfigValidator.validate(config));

    // logs and throws exception if there are invalid params
    handleInvalidParameters(ImmutableMap.copyOf(invalidConfigParams));

    return connectorName;
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
