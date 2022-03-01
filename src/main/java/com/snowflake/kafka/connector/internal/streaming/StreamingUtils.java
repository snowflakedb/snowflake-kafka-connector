package com.snowflake.kafka.connector.internal.streaming;

import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.*;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.ERRORS_TOLERANCE_CONFIG;

import com.snowflake.kafka.connector.Utils;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import net.snowflake.ingest.utils.Constants;

/* Utility class/Helper methods for streaming related ingestion. */
public class StreamingUtils {
  // Streaming Ingest API related fields

  protected static final Duration DURATION_BETWEEN_GET_OFFSET_TOKEN_RETRY = Duration.ofSeconds(1);

  protected static final int MAX_GET_OFFSET_TOKEN_RETRIES = 3;

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
}
