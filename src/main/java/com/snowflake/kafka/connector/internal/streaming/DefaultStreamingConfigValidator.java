package com.snowflake.kafka.connector.internal.streaming;

import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.BOOLEAN_VALIDATOR;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.CUSTOM_SNOWFLAKE_CONVERTERS;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.ERRORS_LOG_ENABLE_CONFIG;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.ERRORS_TOLERANCE_CONFIG;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.KEY_CONVERTER_CONFIG_FIELD;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.SNOWPIPE_STREAMING_MAX_CLIENT_LAG;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.SNOWPIPE_STREAMING_MAX_MEMORY_LIMIT_IN_BYTES;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.VALUE_CONVERTER_CONFIG_FIELD;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.snowflake.kafka.connector.DefaultConnectorConfigValidator;
import com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig;
import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.internal.KCLogger;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.kafka.common.config.ConfigException;

public class DefaultStreamingConfigValidator implements StreamingConfigValidator {

  private static final KCLogger LOGGER =
      new KCLogger(DefaultConnectorConfigValidator.class.getName());

  private static final Set<String> DISALLOWED_CONVERTERS_STREAMING = CUSTOM_SNOWFLAKE_CONVERTERS;
  private static final String STRING_CONVERTER_KEYWORD = "StringConverter";
  private static final String BYTE_ARRAY_CONVERTER_KEYWORD = "ByteArrayConverter";

  @Override
  public ImmutableMap<String, String> validate(Map<String, String> inputConfig) {
    Map<String, String> invalidParams = new HashMap<>();

    invalidParams.putAll(validateConfigConverters(KEY_CONVERTER_CONFIG_FIELD, inputConfig));
    invalidParams.putAll(validateConfigConverters(VALUE_CONVERTER_CONFIG_FIELD, inputConfig));

    validateRole(inputConfig)
        .ifPresent(errorEntry -> invalidParams.put(errorEntry.getKey(), errorEntry.getValue()));

    // Validate error handling configs
    if (inputConfig.containsKey(ERRORS_TOLERANCE_CONFIG)) {
      try {
        SnowflakeSinkConnectorConfig.ErrorTolerance.VALIDATOR.ensureValid(
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
    if (inputConfig.containsKey(SNOWPIPE_STREAMING_MAX_CLIENT_LAG)) {
      ensureValidLong(inputConfig, SNOWPIPE_STREAMING_MAX_CLIENT_LAG, invalidParams);
    }

    if (inputConfig.containsKey(SNOWPIPE_STREAMING_MAX_MEMORY_LIMIT_IN_BYTES)) {
      ensureValidLong(inputConfig, SNOWPIPE_STREAMING_MAX_MEMORY_LIMIT_IN_BYTES, invalidParams);
    }

    // Validate schematization config
    invalidParams.putAll(validateSchematizationConfig(inputConfig));

    return ImmutableMap.copyOf(invalidParams);
  }

  private static Optional<Map.Entry<String, String>> validateRole(Map<String, String> inputConfig) {
    if (!inputConfig.containsKey(Utils.SF_ROLE)
        || Strings.isNullOrEmpty(inputConfig.get(Utils.SF_ROLE))) {
      String missingRole =
          String.format("Config: %s should be present for Snowpipe Streaming", Utils.SF_ROLE);
      return Optional.of(Map.entry(Utils.SF_ROLE, missingRole));
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

    if (inputConfig.containsKey(SnowflakeSinkConnectorConfig.ENABLE_SCHEMATIZATION_CONFIG)) {
      try {
        BOOLEAN_VALIDATOR.ensureValid(
            SnowflakeSinkConnectorConfig.ENABLE_SCHEMATIZATION_CONFIG,
            inputConfig.get(SnowflakeSinkConnectorConfig.ENABLE_SCHEMATIZATION_CONFIG));
      } catch (ConfigException e) {
        invalidParams.put(
            SnowflakeSinkConnectorConfig.ENABLE_SCHEMATIZATION_CONFIG, e.getMessage());
        return invalidParams;
      }

      boolean isSchematizationEnabled =
          Boolean.parseBoolean(
              inputConfig.get(SnowflakeSinkConnectorConfig.ENABLE_SCHEMATIZATION_CONFIG));

      // Validate that schematization and streaming V2 are mutually exclusive
      if (isSchematizationEnabled) {
        invalidParams.put(
            SnowflakeSinkConnectorConfig.ENABLE_SCHEMATIZATION_CONFIG,
            "Schematization is not yet supported with Snowpipe Streaming: High-Performance"
                + " Architecture. ");
      }

      if (isSchematizationEnabled
          && inputConfig.get(VALUE_CONVERTER_CONFIG_FIELD) != null
          && (inputConfig.get(VALUE_CONVERTER_CONFIG_FIELD).contains(STRING_CONVERTER_KEYWORD)
              || inputConfig
                  .get(VALUE_CONVERTER_CONFIG_FIELD)
                  .contains(BYTE_ARRAY_CONVERTER_KEYWORD))) {
        invalidParams.put(
            inputConfig.get(VALUE_CONVERTER_CONFIG_FIELD),
            Utils.formatString(
                "The value converter:{} is not supported with schematization.",
                inputConfig.get(VALUE_CONVERTER_CONFIG_FIELD)));
      }
    }

    return invalidParams;
  }

  /**
   * Validates if key and value converters are allowed values if {@link
   * IngestionMethodConfig#SNOWPIPE_STREAMING} is used.
   *
   * <p>Map if invalid parameters
   */
  private static Map<String, String> validateConfigConverters(
      final String inputConfigConverterField, Map<String, String> inputConfig) {
    Map<String, String> invalidParams = new HashMap<>();

    if (inputConfig.containsKey(inputConfigConverterField)
        && DISALLOWED_CONVERTERS_STREAMING.contains(inputConfig.get(inputConfigConverterField))) {
      invalidParams.put(
          inputConfigConverterField,
          Utils.formatString(
              "Config: {} has provided value: {}. If ingestionMethod is: {}, Snowflake Custom"
                  + " Converters are not allowed. \n"
                  + "Invalid Converters: {}",
              inputConfigConverterField,
              inputConfig.get(inputConfigConverterField),
              IngestionMethodConfig.SNOWPIPE_STREAMING,
              Iterables.toString(DISALLOWED_CONVERTERS_STREAMING)));
    }

    return invalidParams;
  }
}
