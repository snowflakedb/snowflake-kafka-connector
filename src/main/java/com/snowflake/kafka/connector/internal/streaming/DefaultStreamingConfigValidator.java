package com.snowflake.kafka.connector.internal.streaming;

import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.BOOLEAN_VALIDATOR;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.CUSTOM_SNOWFLAKE_CONVERTERS;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.ENABLE_CHANNEL_OFFSET_TOKEN_MIGRATION_CONFIG;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.ENABLE_CHANNEL_OFFSET_TOKEN_VERIFICATION_FUNCTION_CONFIG;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.ERRORS_LOG_ENABLE_CONFIG;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.ERRORS_TOLERANCE_CONFIG;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.INGESTION_METHOD_OPT;
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

    // For snowpipe_streaming, role should be non empty
    if (inputConfig.containsKey(INGESTION_METHOD_OPT)) {
      try {
        // This throws an exception if config value is invalid.
        IngestionMethodConfig.VALIDATOR.ensureValid(
            INGESTION_METHOD_OPT, inputConfig.get(INGESTION_METHOD_OPT));
        if (inputConfig
            .get(INGESTION_METHOD_OPT)
            .equalsIgnoreCase(IngestionMethodConfig.SNOWPIPE_STREAMING.toString())) {
          invalidParams.putAll(validateConfigConverters(KEY_CONVERTER_CONFIG_FIELD, inputConfig));
          invalidParams.putAll(validateConfigConverters(VALUE_CONVERTER_CONFIG_FIELD, inputConfig));

          validateRole(inputConfig)
              .ifPresent(
                  errorEntry -> invalidParams.put(errorEntry.getKey(), errorEntry.getValue()));

          /**
           * Only checking in streaming since we are utilizing the values before we send it to
           * DLQ/output to log file
           */
          if (inputConfig.containsKey(ERRORS_TOLERANCE_CONFIG)) {
            SnowflakeSinkConnectorConfig.ErrorTolerance.VALIDATOR.ensureValid(
                ERRORS_TOLERANCE_CONFIG, inputConfig.get(ERRORS_TOLERANCE_CONFIG));
          }
          if (inputConfig.containsKey(ERRORS_LOG_ENABLE_CONFIG)) {
            BOOLEAN_VALIDATOR.ensureValid(
                ERRORS_LOG_ENABLE_CONFIG, inputConfig.get(ERRORS_LOG_ENABLE_CONFIG));
          }
          if (inputConfig.containsKey(ENABLE_CHANNEL_OFFSET_TOKEN_MIGRATION_CONFIG)) {
            BOOLEAN_VALIDATOR.ensureValid(
                ENABLE_CHANNEL_OFFSET_TOKEN_MIGRATION_CONFIG,
                inputConfig.get(ENABLE_CHANNEL_OFFSET_TOKEN_MIGRATION_CONFIG));
          }
          if (inputConfig.containsKey(ENABLE_CHANNEL_OFFSET_TOKEN_VERIFICATION_FUNCTION_CONFIG)) {
            BOOLEAN_VALIDATOR.ensureValid(
                ENABLE_CHANNEL_OFFSET_TOKEN_VERIFICATION_FUNCTION_CONFIG,
                inputConfig.get(ENABLE_CHANNEL_OFFSET_TOKEN_VERIFICATION_FUNCTION_CONFIG));
          }

          if (inputConfig.containsKey(SNOWPIPE_STREAMING_MAX_CLIENT_LAG)) {
            ensureValidLong(inputConfig, SNOWPIPE_STREAMING_MAX_CLIENT_LAG, invalidParams);
          }

          if (inputConfig.containsKey(SNOWPIPE_STREAMING_MAX_MEMORY_LIMIT_IN_BYTES)) {
            ensureValidLong(
                inputConfig, SNOWPIPE_STREAMING_MAX_MEMORY_LIMIT_IN_BYTES, invalidParams);
          }

          // Valid schematization for Snowpipe Streaming
          invalidParams.putAll(validateSchematizationConfig(inputConfig));
        }
      } catch (ConfigException exception) {
        invalidParams.put(
            INGESTION_METHOD_OPT,
            Utils.formatString(
                "Kafka config:{} error:{}", INGESTION_METHOD_OPT, exception.getMessage()));
      }
    }

    return ImmutableMap.copyOf(invalidParams);
  }

  private static Optional<Map.Entry<String, String>> validateRole(Map<String, String> inputConfig) {
    if (Utils.isSnowpipeStreamingV2Enabled(inputConfig)) {
      return validateRoleForSSv2(inputConfig);
    } else {
      return validateRoleForSSv1(inputConfig);
    }
  }

  private static Optional<Map.Entry<String, String>> validateRoleForSSv1(
      Map<String, String> inputConfig) {
    if (!inputConfig.containsKey(Utils.SF_ROLE)
        || Strings.isNullOrEmpty(inputConfig.get(Utils.SF_ROLE))) {
      String roleMissingForSSv1 =
          String.format(
              "Config:%s should be present if ingestionMethod is:%s",
              Utils.SF_ROLE, inputConfig.get(INGESTION_METHOD_OPT));
      return Optional.of(Map.entry(Utils.SF_ROLE, roleMissingForSSv1));
    }
    return Optional.empty();
  }

  private static Optional<Map.Entry<String, String>> validateRoleForSSv2(
      Map<String, String> inputConfig) {
    if (inputConfig.containsKey(Utils.SF_ROLE)) {
      String rolePresentForSSv2 =
          String.format(
              "'%s' must not be present when '%s' is enabled",
              Utils.SF_ROLE, SnowflakeSinkConnectorConfig.SNOWPIPE_STREAMING_V2_ENABLED);
      return Optional.of(Map.entry(Utils.SF_ROLE, rolePresentForSSv2));
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
      BOOLEAN_VALIDATOR.ensureValid(
          SnowflakeSinkConnectorConfig.ENABLE_SCHEMATIZATION_CONFIG,
          inputConfig.get(SnowflakeSinkConnectorConfig.ENABLE_SCHEMATIZATION_CONFIG));

      if (Boolean.parseBoolean(
              inputConfig.get(SnowflakeSinkConnectorConfig.ENABLE_SCHEMATIZATION_CONFIG))
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
              "Config:{} has provided value:{}. If ingestionMethod is:{}, Snowflake Custom"
                  + " Converters are not allowed. \n"
                  + "Invalid Converters:{}",
              inputConfigConverterField,
              inputConfig.get(inputConfigConverterField),
              IngestionMethodConfig.SNOWPIPE_STREAMING,
              Iterables.toString(DISALLOWED_CONVERTERS_STREAMING)));
    }

    return invalidParams;
  }
}
