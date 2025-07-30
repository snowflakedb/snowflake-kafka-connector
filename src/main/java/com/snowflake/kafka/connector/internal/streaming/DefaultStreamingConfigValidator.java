package com.snowflake.kafka.connector.internal.streaming;

import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.*;
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
import com.snowflake.kafka.connector.internal.BufferThreshold;
import com.snowflake.kafka.connector.internal.KCLogger;
import com.snowflake.kafka.connector.internal.parameters.InternalBufferParameters;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.config.ConfigException;

public class DefaultStreamingConfigValidator implements StreamingConfigValidator {

  private static final KCLogger LOGGER =
      new KCLogger(DefaultConnectorConfigValidator.class.getName());

  private final SingleBufferConfigValidator singleBufferConfigValidator =
      new SingleBufferConfigValidator();
  private final DoubleBufferConfigValidator doubleBufferConfigValidator =
      new DoubleBufferConfigValidator();

  private static final Set<String> DISALLOWED_CONVERTERS_STREAMING = CUSTOM_SNOWFLAKE_CONVERTERS;
  private static final String STRING_CONVERTER_KEYWORD = "StringConverter";
  private static final String BYTE_ARRAY_CONVERTER_KEYWORD = "ByteArrayConverter";

  @Override
  public ImmutableMap<String, String> validate(Map<String, String> inputConfig) {
    Map<String, String> invalidParams = new HashMap<>();

    // For snowpipe_streaming, role should be non empty
    if (inputConfig.containsKey(INGESTION_METHOD_OPT)) {
      if (InternalBufferParameters.isSingleBufferEnabled(inputConfig)) {
        singleBufferConfigValidator.logDoubleBufferingParametersWarning(inputConfig);
      } else {
        LOGGER.warn(
            "Double buffered mode set by '{}=false' is deprecated and will be removed in the future"
                + " release.",
            SNOWPIPE_STREAMING_ENABLE_SINGLE_BUFFER);
        invalidParams.putAll(doubleBufferConfigValidator.validate(inputConfig));
      }

      try {
        // This throws an exception if config value is invalid.
        IngestionMethodConfig.VALIDATOR.ensureValid(
            INGESTION_METHOD_OPT, inputConfig.get(INGESTION_METHOD_OPT));
        if (inputConfig
            .get(INGESTION_METHOD_OPT)
            .equalsIgnoreCase(IngestionMethodConfig.SNOWPIPE_STREAMING.toString())) {
          invalidParams.putAll(validateConfigConverters(KEY_CONVERTER_CONFIG_FIELD, inputConfig));
          invalidParams.putAll(validateConfigConverters(VALUE_CONVERTER_CONFIG_FIELD, inputConfig));

          // Validate if snowflake role is present
          if (!inputConfig.containsKey(Utils.SF_ROLE)
              || Strings.isNullOrEmpty(inputConfig.get(Utils.SF_ROLE))) {
            invalidParams.put(
                Utils.SF_ROLE,
                Utils.formatString(
                    "Config:{} should be present if ingestionMethod is:{}",
                    Utils.SF_ROLE,
                    inputConfig.get(INGESTION_METHOD_OPT)));
          }

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

          if (inputConfig.containsKey(SNOWPIPE_STREAMING_ENABLE_SINGLE_BUFFER)) {
            BOOLEAN_VALIDATOR.ensureValid(
                SNOWPIPE_STREAMING_ENABLE_SINGLE_BUFFER,
                inputConfig.get(SNOWPIPE_STREAMING_ENABLE_SINGLE_BUFFER));
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

  private static void ensureValidIntWithMinimum(
      Map<String, String> inputConfig,
      String param,
      int minimumValue,
      Map<String, String> invalidParams) {
    try {
      int value = Integer.parseInt(inputConfig.get(param));
      if (value < minimumValue) {
        invalidParams.put(
            param,
            Utils.formatString(
                param + " configuration must be at least {}. Given configuration was: {}",
                minimumValue,
                value));
      }
    } catch (NumberFormatException exception) {
      invalidParams.put(
          param,
          Utils.formatString(
              param + " configuration must be a parsable int. Given configuration was: {}",
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

  /** Config validations specific to single buffer architecture */
  private static class SingleBufferConfigValidator {

    private static final KCLogger LOGGER =
        new KCLogger(SingleBufferConfigValidator.class.getName());

    private void logDoubleBufferingParametersWarning(Map<String, String> config) {
      if (InternalBufferParameters.isSingleBufferEnabled(config)) {
        List<String> ignoredParameters = Arrays.asList(BUFFER_FLUSH_TIME_SEC, BUFFER_COUNT_RECORDS);
        ignoredParameters.stream()
            .filter(config::containsKey)
            .forEach(
                param ->
                    LOGGER.warn(
                        "{} parameter value is ignored because internal buffer is disabled. To go"
                            + " back to previous behaviour set "
                            + SNOWPIPE_STREAMING_ENABLE_SINGLE_BUFFER
                            + " to false",
                        param));
      }
    }
  }

  /** Config validations specific to double buffer architecture */
  private static class DoubleBufferConfigValidator {
    private Map<String, String> validate(Map<String, String> inputConfig) {
      Map<String, String> invalidParams = new HashMap<>();

      // check if buffer thresholds are within permissible range
      invalidParams.putAll(
          BufferThreshold.validateBufferThreshold(
              inputConfig, IngestionMethodConfig.SNOWPIPE_STREAMING));

      return invalidParams;
    }
  }
}
