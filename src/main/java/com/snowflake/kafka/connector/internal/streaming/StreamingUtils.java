package com.snowflake.kafka.connector.internal.streaming;

import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_CONFIG;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.ERRORS_LOG_ENABLE_CONFIG;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.ERRORS_TOLERANCE_CONFIG;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.ErrorTolerance;

import com.snowflake.kafka.connector.Utils;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import net.snowflake.ingest.streaming.OffsetTokenVerificationFunction;
import net.snowflake.ingest.utils.Constants;

/* Utility class/Helper methods for streaming related ingestion. */
public class StreamingUtils {
  public static final long STREAMING_BUFFER_FLUSH_TIME_MINIMUM_SEC =
      Duration.ofSeconds(1).getSeconds();

  // TODO: Modify STREAMING_CONSTANT to Constants. after SNOW-352846 is released
  public static final String STREAMING_CONSTANT_AUTHORIZATION_TYPE = "authorization_type";
  public static final String STREAMING_CONSTANT_JWT = "JWT";
  public static final String STREAMING_CONSTANT_OAUTH = "OAuth";
  public static final String STREAMING_CONSTANT_OAUTH_CLIENT_ID = "oauth_client_id";
  public static final String STREAMING_CONSTANT_OAUTH_CLIENT_SECRET = "oauth_client_secret";
  public static final String STREAMING_CONSTANT_OAUTH_REFRESH_TOKEN = "oauth_refresh_token";
  public static final String STREAMING_CONSTANT_OAUTH_TOKEN_ENDPOINT = "oauth_token_endpoint";

  // Offset verification function to verify that the current start offset has to incremental,
  // note that there are some false positives when SMT is used.
  public static final OffsetTokenVerificationFunction offsetTokenVerificationFunction =
      (prevBatchEndOffset, curBatchStartOffset, curBatchEndOffset, rowCount) -> {
        if (prevBatchEndOffset != null && curBatchStartOffset != null) {
          long curStart = Long.parseLong(curBatchStartOffset);
          long prevEnd = Long.parseLong(prevBatchEndOffset);
          return curStart > prevEnd;
        }
        return true;
      };

  /* Creates streaming client properties from snowflake KC config file. */
  public static Properties convertConfigForStreamingClient(Map<String, String> connectorConfig) {
    Properties streamingProperties = new Properties();

    connectorConfig.computeIfPresent(
        Utils.SF_URL,
        (key, value) -> {
          streamingProperties.put(Constants.ACCOUNT_URL, value);
          return value;
        });

    connectorConfig.computeIfPresent(
        Utils.SF_ROLE,
        (key, value) -> {
          streamingProperties.put(Constants.ROLE, value);
          return value;
        });

    connectorConfig.computeIfPresent(
        Utils.SF_USER,
        (key, value) -> {
          streamingProperties.put(Constants.USER, value);
          return value;
        });

    connectorConfig.computeIfPresent(
        Utils.SF_AUTHENTICATOR,
        (key, value) -> {
          if (value.equals(Utils.SNOWFLAKE_JWT)) {
            streamingProperties.put(STREAMING_CONSTANT_AUTHORIZATION_TYPE, STREAMING_CONSTANT_JWT);
          }
          if (value.equals(Utils.OAUTH)) {
            streamingProperties.put(
                STREAMING_CONSTANT_AUTHORIZATION_TYPE, STREAMING_CONSTANT_OAUTH);
          }
          return value;
        });

    connectorConfig.computeIfPresent(
        Utils.SF_PRIVATE_KEY,
        (key, value) -> {
          streamingProperties.put(Constants.PRIVATE_KEY, value);
          return value;
        });

    connectorConfig.computeIfPresent(
        Utils.PRIVATE_KEY_PASSPHRASE,
        (key, value) -> {
          if (!value.isEmpty()) {
            streamingProperties.put(Constants.PRIVATE_KEY_PASSPHRASE, value);
          }
          return value;
        });

    connectorConfig.computeIfPresent(
        Utils.SF_OAUTH_CLIENT_ID,
        (key, value) -> {
          streamingProperties.put(STREAMING_CONSTANT_OAUTH_CLIENT_ID, value);
          return value;
        });

    connectorConfig.computeIfPresent(
        Utils.SF_OAUTH_CLIENT_SECRET,
        (key, value) -> {
          streamingProperties.put(STREAMING_CONSTANT_OAUTH_CLIENT_SECRET, value);
          return value;
        });

    connectorConfig.computeIfPresent(
        Utils.SF_OAUTH_REFRESH_TOKEN,
        (key, value) -> {
          streamingProperties.put(STREAMING_CONSTANT_OAUTH_REFRESH_TOKEN, value);
          return value;
        });

    connectorConfig.computeIfPresent(
        Utils.SF_OAUTH_TOKEN_ENDPOINT,
        (key, value) -> {
          streamingProperties.put(STREAMING_CONSTANT_OAUTH_TOKEN_ENDPOINT, value);
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
