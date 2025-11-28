package com.snowflake.kafka.connector.internal.streaming;

import static com.snowflake.kafka.connector.ConnectorConfigTools.ErrorTolerance;
import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_CONFIG;
import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.ERRORS_LOG_ENABLE_CONFIG;
import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.ERRORS_TOLERANCE_CONFIG;

import com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import net.snowflake.ingest.utils.Constants;

/* Utility class/Helper methods for streaming related ingestion. */
public class StreamingUtils {
  public static final long STREAMING_BUFFER_FLUSH_TIME_MINIMUM_SEC =
      Duration.ofSeconds(1).getSeconds();

  // TODO: Modify STREAMING_CONSTANT to Constants. after SNOW-352846 is released
  public static final String STREAMING_CONSTANT_AUTHORIZATION_TYPE = "authorization_type";

  /* Creates streaming client properties from snowflake KC config file. */
  public static Properties convertConfigForStreamingClient(Map<String, String> connectorConfig) {
    Properties streamingProperties = new Properties();

    connectorConfig.computeIfPresent(
        KafkaConnectorConfigParams.SNOWFLAKE_URL_NAME,
        (key, value) -> {
          streamingProperties.put(Constants.ACCOUNT_URL, value);
          return value;
        });

    connectorConfig.computeIfPresent(
        KafkaConnectorConfigParams.SNOWFLAKE_ROLE_NAME,
        (key, value) -> {
          streamingProperties.put(Constants.ROLE, value);
          return value;
        });

    connectorConfig.computeIfPresent(
        KafkaConnectorConfigParams.SNOWFLAKE_USER_NAME,
        (key, value) -> {
          streamingProperties.put(Constants.USER, value);
          return value;
        });

    connectorConfig.computeIfPresent(
        KafkaConnectorConfigParams.SNOWFLAKE_PRIVATE_KEY,
        (key, value) -> {
          streamingProperties.put(Constants.PRIVATE_KEY, value);
          return value;
        });

    connectorConfig.computeIfPresent(
        KafkaConnectorConfigParams.SNOWFLAKE_PRIVATE_KEY_PASSPHRASE,
        (key, value) -> {
          if (!value.isEmpty()) {
            streamingProperties.put(Constants.PRIVATE_KEY_PASSPHRASE, value);
          }
          return value;
        });

    return streamingProperties;
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
