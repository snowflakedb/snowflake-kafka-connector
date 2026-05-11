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
import static com.snowflake.kafka.connector.Utils.validateProxySettings;

import com.google.common.collect.ImmutableMap;
import com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams;
import com.snowflake.kafka.connector.config.AuthenticatorType;
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
        TopicToTableParser.parseAndValidate(
            config.get(KafkaConnectorConfigParams.SNOWFLAKE_TOPICS2TABLE_MAP));
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

    AuthenticatorType authenticator;
    try {
      authenticator =
          AuthenticatorType.fromConfig(
              config.getOrDefault(
                  KafkaConnectorConfigParams.SNOWFLAKE_AUTHENTICATOR,
                  AuthenticatorType.SNOWFLAKE_JWT.toConfigValue()));
    } catch (IllegalArgumentException e) {
      invalidConfigParams.put(KafkaConnectorConfigParams.SNOWFLAKE_AUTHENTICATOR, e.getMessage());
      authenticator = null;
    }
    if (authenticator != null) {
      switch (authenticator) {
        case OAUTH:
          validateOAuthConfig(config, invalidConfigParams);
          break;
        case SNOWFLAKE_JWT:
          if (!config.containsKey(SNOWFLAKE_PRIVATE_KEY)) {
            invalidConfigParams.put(
                SNOWFLAKE_PRIVATE_KEY,
                Utils.formatString("{} cannot be empty", SNOWFLAKE_PRIVATE_KEY));
          }
          break;
        default:
          throw new IllegalStateException("Unhandled authenticator type: " + authenticator);
      }
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

    validateCompatibilitySettings(config, invalidConfigParams);

    // Check all config values for ingestion method == IngestionMethodConfig.SNOWPIPE_STREAMING
    invalidConfigParams.putAll(streamingConfigValidator.validate(config));

    // logs and throws exception if there are invalid params
    handleInvalidParameters(ImmutableMap.copyOf(invalidConfigParams));
  }

  private void validateOAuthConfig(
      Map<String, String> config, Map<String, String> invalidConfigParams) {
    String clientId = config.getOrDefault(KafkaConnectorConfigParams.SNOWFLAKE_OAUTH_CLIENT_ID, "");
    if (clientId.isBlank()) {
      invalidConfigParams.put(
          KafkaConnectorConfigParams.SNOWFLAKE_OAUTH_CLIENT_ID,
          Utils.formatString(
              "{} must be non-empty when using oauth authenticator",
              KafkaConnectorConfigParams.SNOWFLAKE_OAUTH_CLIENT_ID));
    }

    String clientSecret =
        config.getOrDefault(KafkaConnectorConfigParams.SNOWFLAKE_OAUTH_CLIENT_SECRET, "");
    if (clientSecret.isBlank()) {
      invalidConfigParams.put(
          KafkaConnectorConfigParams.SNOWFLAKE_OAUTH_CLIENT_SECRET,
          Utils.formatString(
              "{} must be non-empty when using oauth authenticator",
              KafkaConnectorConfigParams.SNOWFLAKE_OAUTH_CLIENT_SECRET));
    }

    boolean includeScope =
        Boolean.parseBoolean(
            config.getOrDefault(
                KafkaConnectorConfigParams.SNOWFLAKE_OAUTH_INCLUDE_SCOPE,
                String.valueOf(KafkaConnectorConfigParams.SNOWFLAKE_OAUTH_INCLUDE_SCOPE_DEFAULT)));
    String scope = config.getOrDefault(KafkaConnectorConfigParams.SNOWFLAKE_OAUTH_SCOPE, "");
    if (!scope.isBlank() && !includeScope) {
      invalidConfigParams.put(
          KafkaConnectorConfigParams.SNOWFLAKE_OAUTH_SCOPE,
          Utils.formatString(
              "{} is only used when {} is true",
              KafkaConnectorConfigParams.SNOWFLAKE_OAUTH_SCOPE,
              KafkaConnectorConfigParams.SNOWFLAKE_OAUTH_INCLUDE_SCOPE));
    }
  }

  private void validateCompatibilitySettings(
      Map<String, String> config, Map<String, String> invalidConfigParams) {
    boolean validateCompat =
        Boolean.parseBoolean(
            config.getOrDefault(
                KafkaConnectorConfigParams.SNOWFLAKE_STREAMING_VALIDATE_COMPATIBILITY_WITH_CLASSIC,
                String.valueOf(
                    KafkaConnectorConfigParams
                        .SNOWFLAKE_STREAMING_VALIDATE_COMPATIBILITY_WITH_CLASSIC_DEFAULT)));
    if (!validateCompat) {
      return;
    }

    String optOutHint =
        " To skip this check, set "
            + KafkaConnectorConfigParams.SNOWFLAKE_STREAMING_VALIDATE_COMPATIBILITY_WITH_CLASSIC
            + "=false.";

    // snowflake.validation must be client_side
    String validation =
        config.getOrDefault(
            KafkaConnectorConfigParams.SNOWFLAKE_VALIDATION,
            KafkaConnectorConfigParams.SNOWFLAKE_VALIDATION_DEFAULT);
    if (!"client_side".equals(validation)) {
      invalidConfigParams.put(
          KafkaConnectorConfigParams.SNOWFLAKE_VALIDATION,
          KafkaConnectorConfigParams.SNOWFLAKE_STREAMING_VALIDATE_COMPATIBILITY_WITH_CLASSIC
              + " is enabled but "
              + KafkaConnectorConfigParams.SNOWFLAKE_VALIDATION
              + " is set to '"
              + validation
              + "'. For KC v3 compatibility, set "
              + KafkaConnectorConfigParams.SNOWFLAKE_VALIDATION
              + "=client_side."
              + optOutHint);
    }

    // snowflake.compatibility.enable.column.identifier.normalization must be true
    String columnNormalization =
        config.getOrDefault(
            KafkaConnectorConfigParams
                .SNOWFLAKE_COMPATIBILITY_ENABLE_COLUMN_IDENTIFIER_NORMALIZATION,
            String.valueOf(
                KafkaConnectorConfigParams
                    .SNOWFLAKE_COMPATIBILITY_ENABLE_COLUMN_IDENTIFIER_NORMALIZATION_DEFAULT));
    if (!"true".equalsIgnoreCase(columnNormalization)) {
      invalidConfigParams.put(
          KafkaConnectorConfigParams.SNOWFLAKE_COMPATIBILITY_ENABLE_COLUMN_IDENTIFIER_NORMALIZATION,
          KafkaConnectorConfigParams.SNOWFLAKE_STREAMING_VALIDATE_COMPATIBILITY_WITH_CLASSIC
              + " is enabled but "
              + KafkaConnectorConfigParams
                  .SNOWFLAKE_COMPATIBILITY_ENABLE_COLUMN_IDENTIFIER_NORMALIZATION
              + " is set to '"
              + columnNormalization
              + "'. For KC v3 compatibility, set "
              + KafkaConnectorConfigParams
                  .SNOWFLAKE_COMPATIBILITY_ENABLE_COLUMN_IDENTIFIER_NORMALIZATION
              + "=true."
              + optOutHint);
    }

    // snowflake.compatibility.enable.autogenerated.table.name.sanitization must be true
    String tableSanitization =
        config.getOrDefault(
            KafkaConnectorConfigParams
                .SNOWFLAKE_COMPATIBILITY_ENABLE_AUTOGENERATED_TABLE_NAME_SANITIZATION,
            String.valueOf(
                KafkaConnectorConfigParams
                    .SNOWFLAKE_COMPATIBILITY_ENABLE_AUTOGENERATED_TABLE_NAME_SANITIZATION_DEFAULT));
    if (!"true".equalsIgnoreCase(tableSanitization)) {
      invalidConfigParams.put(
          KafkaConnectorConfigParams
              .SNOWFLAKE_COMPATIBILITY_ENABLE_AUTOGENERATED_TABLE_NAME_SANITIZATION,
          KafkaConnectorConfigParams.SNOWFLAKE_STREAMING_VALIDATE_COMPATIBILITY_WITH_CLASSIC
              + " is enabled but "
              + KafkaConnectorConfigParams
                  .SNOWFLAKE_COMPATIBILITY_ENABLE_AUTOGENERATED_TABLE_NAME_SANITIZATION
              + " is set to '"
              + tableSanitization
              + "'. For KC v3 compatibility, set "
              + KafkaConnectorConfigParams
                  .SNOWFLAKE_COMPATIBILITY_ENABLE_AUTOGENERATED_TABLE_NAME_SANITIZATION
              + "=true."
              + optOutHint);
    }

    // snowflake.enable.schematization must be explicitly set (any value)
    if (!config.containsKey(KafkaConnectorConfigParams.SNOWFLAKE_ENABLE_SCHEMATIZATION)) {
      invalidConfigParams.put(
          KafkaConnectorConfigParams.SNOWFLAKE_ENABLE_SCHEMATIZATION,
          KafkaConnectorConfigParams.SNOWFLAKE_STREAMING_VALIDATE_COMPATIBILITY_WITH_CLASSIC
              + " is enabled but "
              + KafkaConnectorConfigParams.SNOWFLAKE_ENABLE_SCHEMATIZATION
              + " is not explicitly set. The default changed from false (KC v3) to true (KC v4)."
              + " Please set "
              + KafkaConnectorConfigParams.SNOWFLAKE_ENABLE_SCHEMATIZATION
              + " explicitly to confirm your intended behavior."
              + optOutHint);
    }

    // snowflake.streaming.classic.offset.migration must be explicitly set
    if (!config.containsKey(KafkaConnectorConfigParams.SNOWFLAKE_SSV1_OFFSET_MIGRATION)) {
      invalidConfigParams.put(
          KafkaConnectorConfigParams.SNOWFLAKE_SSV1_OFFSET_MIGRATION,
          KafkaConnectorConfigParams.SNOWFLAKE_STREAMING_VALIDATE_COMPATIBILITY_WITH_CLASSIC
              + " is enabled but "
              + KafkaConnectorConfigParams.SNOWFLAKE_SSV1_OFFSET_MIGRATION
              + " is not explicitly set. If migrating from KC v3, set it to 'strict' or"
              + " 'best_effort' so that committed offsets from the previous connector version are"
              + " carried over. If migrating from file-based Snowpipe, set it to 'skip'."
              + optOutHint);
    }

    // snowflake.streaming.classic.offset.migration.include.connector.name is only relevant
    // when offset migration is active (strict or best_effort), not when skipped.
    String offsetMigration =
        config.getOrDefault(
            KafkaConnectorConfigParams.SNOWFLAKE_SSV1_OFFSET_MIGRATION,
            KafkaConnectorConfigParams.SNOWFLAKE_SSV1_OFFSET_MIGRATION_DEFAULT);
    boolean offsetMigrationActive =
        "strict".equalsIgnoreCase(offsetMigration)
            || "best_effort".equalsIgnoreCase(offsetMigration);
    if (offsetMigrationActive
        && !config.containsKey(
            KafkaConnectorConfigParams.SNOWFLAKE_SSV1_OFFSET_MIGRATION_INCLUDE_CONNECTOR_NAME)) {
      invalidConfigParams.put(
          KafkaConnectorConfigParams.SNOWFLAKE_SSV1_OFFSET_MIGRATION_INCLUDE_CONNECTOR_NAME,
          KafkaConnectorConfigParams.SNOWFLAKE_STREAMING_VALIDATE_COMPATIBILITY_WITH_CLASSIC
              + " is enabled but "
              + KafkaConnectorConfigParams.SNOWFLAKE_SSV1_OFFSET_MIGRATION_INCLUDE_CONNECTOR_NAME
              + " is not explicitly set. Whether the SSv1 channel name included the connector"
              + " name depends on the KC v3 configuration that was used. Please set "
              + KafkaConnectorConfigParams.SNOWFLAKE_SSV1_OFFSET_MIGRATION_INCLUDE_CONNECTOR_NAME
              + " explicitly to match how the previous connector was configured."
              + optOutHint);
    }
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
