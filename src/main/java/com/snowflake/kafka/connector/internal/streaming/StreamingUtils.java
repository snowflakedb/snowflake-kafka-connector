package com.snowflake.kafka.connector.internal.streaming;

import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.BOOLEAN_VALIDATOR;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.CUSTOM_SNOWFLAKE_CONVERTERS;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_CONFIG;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.ERRORS_LOG_ENABLE_CONFIG;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.ERRORS_TOLERANCE_CONFIG;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.ErrorTolerance;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.INGESTION_METHOD_OPT;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.KEY_CONVERTER_CONFIG_FIELD;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.VALUE_CONVERTER_CONFIG_FIELD;

import com.google.common.base.Strings;
import com.google.common.collect.Iterables;
import com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig;
import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.internal.BufferThreshold;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import net.snowflake.ingest.utils.Constants;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.record.DefaultRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/* Utility class/Helper methods for streaming related ingestion. */
public class StreamingUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(StreamingUtils.class);

  // Streaming Ingest API related fields

  protected static final Duration DURATION_BETWEEN_GET_OFFSET_TOKEN_RETRY = Duration.ofSeconds(1);

  protected static final int MAX_GET_OFFSET_TOKEN_RETRIES = 3;

  // Buffer related defaults and minimum set at connector level by clients/customers.
  public static final long STREAMING_BUFFER_FLUSH_TIME_MINIMUM_SEC =
      Duration.ofSeconds(1).getSeconds();

  public static final long STREAMING_BUFFER_FLUSH_TIME_DEFAULT_SEC =
      Duration.ofSeconds(10).getSeconds();

  protected static final long STREAMING_BUFFER_COUNT_RECORDS_DEFAULT = 10_000L;

  /**
   * Keeping this default as ~ 20MB.
   *
   * <p>Logic behind this optimium value is we will do gzip compression and json to UTF conversion
   * which will account to almost 95% compression.
   *
   * <p>1 MB is an ideal size for streaming ingestion so 95% if 20MB = 1MB
   */
  protected static final long STREAMING_BUFFER_BYTES_DEFAULT = 20_000_000;

  private static final Set<String> DISALLOWED_CONVERTERS_STREAMING = CUSTOM_SNOWFLAKE_CONVERTERS;
  private static final String STRING_CONVERTER_KEYWORD = "StringConverter";
  private static final String BYTE_ARRAY_CONVERTER_KEYWORD = "ByteArrayConverter";

  // excluding key, value and headers: 5 bytes length + 10 bytes timestamp + 5 bytes offset + 1
  // byte attributes. (This is not for record metadata, this is before we transform to snowflake
  // understood JSON)
  // This is overhead size for calculating while buffering Kafka records.
  public static final int MAX_RECORD_OVERHEAD_BYTES = DefaultRecord.MAX_RECORD_OVERHEAD;

  /* Maps streaming client's property keys to what we got from snowflake KC config file. */
  public static Map<String, String> convertConfigForStreamingClient(
      Map<String, String> connectorConfig) {
    Map<String, String> streamingPropertiesMap = new HashMap<>();
    connectorConfig.computeIfPresent(
        Utils.SF_URL,
        (key, value) -> {
          streamingPropertiesMap.put(Constants.ACCOUNT_URL, value);
          return value;
        });

    connectorConfig.computeIfPresent(
        Utils.SF_ROLE,
        (key, value) -> {
          streamingPropertiesMap.put(Constants.ROLE, value);
          return value;
        });

    connectorConfig.computeIfPresent(
        Utils.SF_USER,
        (key, value) -> {
          streamingPropertiesMap.put(Constants.USER, value);
          return value;
        });

    connectorConfig.computeIfPresent(
        Utils.SF_PRIVATE_KEY,
        (key, value) -> {
          streamingPropertiesMap.put(Constants.PRIVATE_KEY, value);
          return value;
        });

    connectorConfig.computeIfPresent(
        Utils.PRIVATE_KEY_PASSPHRASE,
        (key, value) -> {
          if (!value.isEmpty()) {
            streamingPropertiesMap.put(Constants.PRIVATE_KEY_PASSPHRASE, value);
          }
          return value;
        });
    return streamingPropertiesMap;
  }

  /* Returns true if sf connector config has error.tolerance = ALL */
  public static boolean tolerateErrors(Map<String, String> sfConnectorConfig) {
    String errorsTolerance =
        sfConnectorConfig.getOrDefault(ERRORS_TOLERANCE_CONFIG, ErrorTolerance.NONE.toString());

    return ErrorTolerance.valueOf(errorsTolerance.toUpperCase()).equals(ErrorTolerance.ALL);
  }

  /* Returns true if connector config has errors.log.enable = true */
  public static boolean logErrors(Map<String, String> sfConnectorConfig) {
    return Boolean.parseBoolean(sfConnectorConfig.getOrDefault(ERRORS_LOG_ENABLE_CONFIG, "false"));
  }

  /* Returns dlq topic name if connector config has errors.deadletterqueue.topic.name set */
  public static String getDlqTopicName(Map<String, String> sfConnectorConfig) {
    return sfConnectorConfig.getOrDefault(ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_CONFIG, "");
  }

  /**
   * Check if Streaming snowpipe related config provided by config(customer's config) has valid and
   * allowed values
   *
   * @param inputConfig given in connector json file
   * @return true if valid, also logs error if invalid.
   */
  public static boolean isStreamingSnowpipeConfigValid(final Map<String, String> inputConfig) {

    boolean configIsValid = true;

    // For snowpipe_streaming, role should be non empty
    if (inputConfig.containsKey(INGESTION_METHOD_OPT)) {
      try {
        // This throws an exception if config value is invalid.
        IngestionMethodConfig.VALIDATOR.ensureValid(
            INGESTION_METHOD_OPT, inputConfig.get(INGESTION_METHOD_OPT));
        if (inputConfig
            .get(INGESTION_METHOD_OPT)
            .equalsIgnoreCase(IngestionMethodConfig.SNOWPIPE_STREAMING.toString())) {

          // check if buffer thresholds are within permissible range
          if (!BufferThreshold.validateBufferThreshold(
              inputConfig, IngestionMethodConfig.SNOWPIPE_STREAMING)) {
            configIsValid = false;
          }

          if (!validateConfigConverters(KEY_CONVERTER_CONFIG_FIELD, inputConfig)) {
            configIsValid = false;
          }

          if (!validateConfigConverters(VALUE_CONVERTER_CONFIG_FIELD, inputConfig)) {
            configIsValid = false;
          }

          // Validate if snowflake role is present
          if (!inputConfig.containsKey(Utils.SF_ROLE)
              || Strings.isNullOrEmpty(inputConfig.get(Utils.SF_ROLE))) {
            LOGGER.error(
                "Config:{} should be present if ingestionMethod is:{}",
                Utils.SF_ROLE,
                inputConfig.get(INGESTION_METHOD_OPT));
            configIsValid = false;
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

          // Valid schematization for Snowpipe Streaming
          if (!validateSchematizationConfig(inputConfig)) {
            configIsValid = false;
          }
        }
      } catch (ConfigException exception) {
        LOGGER.error("Kafka config:{} error:{}", INGESTION_METHOD_OPT, exception.getMessage());
        configIsValid = false;
      }
    }
    return configIsValid;
  }

  /**
   * Validates if key and value converters are allowed values if {@link
   * IngestionMethodConfig#SNOWPIPE_STREAMING} is used.
   *
   * <p>return true if allowed, false otherwise.
   */
  private static boolean validateConfigConverters(
      final String inputConfigConverterField, Map<String, String> inputConfig) {
    if (inputConfig.containsKey(inputConfigConverterField)) {
      if (DISALLOWED_CONVERTERS_STREAMING.contains(inputConfig.get(inputConfigConverterField))) {
        LOGGER.error(
            "Config:{} has provided value:{}. If ingestionMethod is:{}, Snowflake Custom"
                + " Converters are not allowed. \n"
                + "Invalid Converters:{}",
            inputConfigConverterField,
            inputConfig.get(inputConfigConverterField),
            IngestionMethodConfig.SNOWPIPE_STREAMING,
            Iterables.toString(DISALLOWED_CONVERTERS_STREAMING));
        return false;
      }
    }
    return true;
  }

  /**
   * Validates if the configs are allowed values when schematization is enabled.
   *
   * <p>return true if allowed, false otherwise.
   */
  private static boolean validateSchematizationConfig(Map<String, String> inputConfig) {
    if (inputConfig.containsKey(SnowflakeSinkConnectorConfig.ENABLE_SCHEMATIZATION_CONFIG)) {
      BOOLEAN_VALIDATOR.ensureValid(
          SnowflakeSinkConnectorConfig.ENABLE_SCHEMATIZATION_CONFIG,
          inputConfig.get(SnowflakeSinkConnectorConfig.ENABLE_SCHEMATIZATION_CONFIG));

      if (inputConfig.get(VALUE_CONVERTER_CONFIG_FIELD) != null
          && (inputConfig.get(VALUE_CONVERTER_CONFIG_FIELD).contains(STRING_CONVERTER_KEYWORD)
              || inputConfig
                  .get(VALUE_CONVERTER_CONFIG_FIELD)
                  .contains(BYTE_ARRAY_CONVERTER_KEYWORD))) {
        LOGGER.error(
            "The value converter:{} is not supported with schematization.",
            inputConfig.get(VALUE_CONVERTER_CONFIG_FIELD));
        return false;
      }
    }
    return true;
  }
}
