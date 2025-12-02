package com.snowflake.kafka.connector.internal.streaming;

import static com.snowflake.kafka.connector.ConnectorConfigTools.BOOLEAN_VALIDATOR;
import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.ERRORS_LOG_ENABLE_CONFIG;
import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.ERRORS_TOLERANCE_CONFIG;
import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.SNOWFLAKE_STREAMING_ICEBERG_ENABLED;
import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.SNOWFLAKE_STREAMING_MAX_CLIENT_LAG;
import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.VALUE_CONVERTER;
import static com.snowflake.kafka.connector.Utils.isIcebergEnabled;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.snowflake.kafka.connector.ConnectorConfigTools;
import com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams;
import com.snowflake.kafka.connector.Utils;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.common.config.ConfigException;

public class DefaultStreamingConfigValidator implements StreamingConfigValidator {

  private static final String STRING_CONVERTER_KEYWORD = "StringConverter";
  private static final String BYTE_ARRAY_CONVERTER_KEYWORD = "ByteArrayConverter";

  @Override
  public ImmutableMap<String, String> validate(Map<String, String> inputConfig) {
    Map<String, String> invalidParams = new HashMap<>();

    // Validate Iceberg config
    if (isIcebergEnabled(inputConfig)) {
      invalidParams.put(
          SNOWFLAKE_STREAMING_ICEBERG_ENABLED,
          "Ingestion to Iceberg table is currently unsupported.");
    }

    validateRole(inputConfig)
        .ifPresent(errorEntry -> invalidParams.put(errorEntry.getKey(), errorEntry.getValue()));

    // Validate error handling configs
    if (inputConfig.containsKey(ERRORS_TOLERANCE_CONFIG)) {
      try {
        ConnectorConfigTools.ErrorTolerance.VALIDATOR.ensureValid(
            ERRORS_TOLERANCE_CONFIG, inputConfig.get(ERRORS_TOLERANCE_CONFIG));
      } catch (ConfigException e) {
        invalidParams.put(
            ERRORS_TOLERANCE_CONFIG,
            Utils.formatString(
                "{} configuration error: {}", ERRORS_TOLERANCE_CONFIG, e.getMessage()));
      }
    }
    if (inputConfig.containsKey(ERRORS_LOG_ENABLE_CONFIG)) {
      try {
        BOOLEAN_VALIDATOR.ensureValid(
            ERRORS_LOG_ENABLE_CONFIG, inputConfig.get(ERRORS_LOG_ENABLE_CONFIG));
      } catch (ConfigException e) {
        invalidParams.put(ERRORS_LOG_ENABLE_CONFIG, e.getMessage());
      }
    }
    if (inputConfig.containsKey(SNOWFLAKE_STREAMING_MAX_CLIENT_LAG)) {
      ensureValidLong(inputConfig, SNOWFLAKE_STREAMING_MAX_CLIENT_LAG, invalidParams);
    }

    // Validate schematization config
    invalidParams.putAll(validateSchematizationConfig(inputConfig));

    return ImmutableMap.copyOf(invalidParams);
  }

  private static Optional<Map.Entry<String, String>> validateRole(Map<String, String> inputConfig) {
    if (!inputConfig.containsKey(KafkaConnectorConfigParams.SNOWFLAKE_ROLE_NAME)
        || Strings.isNullOrEmpty(inputConfig.get(KafkaConnectorConfigParams.SNOWFLAKE_ROLE_NAME))) {
      String missingRole =
          String.format(
              "Config: %s should be present for Snowpipe Streaming",
              KafkaConnectorConfigParams.SNOWFLAKE_ROLE_NAME);
      return Optional.of(Map.entry(KafkaConnectorConfigParams.SNOWFLAKE_ROLE_NAME, missingRole));
    }
    return Optional.empty();
  }

  private static void ensureValidLong(
      Map<String, String> inputConfig, String param, Map<String, String> invalidParams) {
    try {
      Long.parseLong(inputConfig.get(param));
    } catch (NumberFormatException exception) {
      invalidParams.put(
          param,
          Utils.formatString(
              param + " configuration must be a parsable long. Given configuration" + " was: {}",
              inputConfig.get(param)));
    }
  }

  /**
   * Validates if the configs are allowed values when schematization is enabled.
   *
   * <p>return a map of invalid params
   */
  private static Map<String, String> validateSchematizationConfig(Map<String, String> inputConfig) {
    Map<String, String> invalidParams = new HashMap<>();

    if (inputConfig.get(VALUE_CONVERTER) != null
        && (inputConfig.get(VALUE_CONVERTER).contains(STRING_CONVERTER_KEYWORD)
            || inputConfig.get(VALUE_CONVERTER).contains(BYTE_ARRAY_CONVERTER_KEYWORD))) {
      invalidParams.put(
          inputConfig.get(VALUE_CONVERTER),
          Utils.formatString(
              "The value converter:{} is not supported.", inputConfig.get(VALUE_CONVERTER)));
    }

    return invalidParams;
  }
}
