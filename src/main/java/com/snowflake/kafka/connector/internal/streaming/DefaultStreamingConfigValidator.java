package com.snowflake.kafka.connector.internal.streaming;

import static com.snowflake.kafka.connector.ConnectorConfigTools.BOOLEAN_VALIDATOR;
import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.ERRORS_LOG_ENABLE_CONFIG;
import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.ERRORS_TOLERANCE_CONFIG;
import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.VALUE_CONVERTER;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.snowflake.kafka.connector.ConnectorConfigTools;
import com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams;
import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.config.SinkTaskConfig;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.config.ConfigException;

public class DefaultStreamingConfigValidator implements StreamingConfigValidator {

  private static final String STRING_CONVERTER_KEYWORD = "StringConverter";
  private static final String BYTE_ARRAY_CONVERTER_KEYWORD = "ByteArrayConverter";

  @Override
  public ImmutableMap<String, String> validate(
      SinkTaskConfig parsedConfig, Map<String, String> rawConfig) {
    Map<String, String> invalidParams = new HashMap<>();

    if (Strings.isNullOrEmpty(parsedConfig.getSnowflakeRole())) {
      invalidParams.put(
          KafkaConnectorConfigParams.SNOWFLAKE_ROLE_NAME,
          String.format(
              "Config: %s should be present for Snowpipe Streaming",
              KafkaConnectorConfigParams.SNOWFLAKE_ROLE_NAME));
    }

    // Validate error handling configs (format-level: use raw strings)
    if (rawConfig.containsKey(ERRORS_TOLERANCE_CONFIG)) {
      try {
        ConnectorConfigTools.ErrorTolerance.VALIDATOR.ensureValid(
            ERRORS_TOLERANCE_CONFIG, rawConfig.get(ERRORS_TOLERANCE_CONFIG));
      } catch (ConfigException e) {
        invalidParams.put(
            ERRORS_TOLERANCE_CONFIG,
            Utils.formatString(
                "{} configuration error: {}", ERRORS_TOLERANCE_CONFIG, e.getMessage()));
      }
    }
    if (rawConfig.containsKey(ERRORS_LOG_ENABLE_CONFIG)) {
      try {
        BOOLEAN_VALIDATOR.ensureValid(
            ERRORS_LOG_ENABLE_CONFIG, rawConfig.get(ERRORS_LOG_ENABLE_CONFIG));
      } catch (ConfigException e) {
        invalidParams.put(ERRORS_LOG_ENABLE_CONFIG, e.getMessage());
      }
    }

    // Validate schematization + converter compatibility
    String valueConverter = rawConfig.get(VALUE_CONVERTER);
    if (parsedConfig.isEnableSchematization()
        && valueConverter != null
        && (valueConverter.contains(STRING_CONVERTER_KEYWORD)
            || valueConverter.contains(BYTE_ARRAY_CONVERTER_KEYWORD))) {
      invalidParams.put(
          valueConverter,
          Utils.formatString(
              "The value converter:{} is not supported when schematization is enabled.",
              valueConverter));
    }

    return ImmutableMap.copyOf(invalidParams);
  }
}
