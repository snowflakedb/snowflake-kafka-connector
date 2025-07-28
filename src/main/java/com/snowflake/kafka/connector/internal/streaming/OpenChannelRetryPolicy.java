package com.snowflake.kafka.connector.internal.streaming;

import com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig;
import com.snowflake.kafka.connector.internal.KCLogger;
import dev.failsafe.Failsafe;
import dev.failsafe.Fallback;
import dev.failsafe.RetryPolicy;
import dev.failsafe.function.CheckedSupplier;
import java.time.Duration;
import java.util.Map;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import net.snowflake.ingest.utils.SFException;

/**
 * Policy class that encapsulates retry logic for opening streaming channels with exponential
 * backoff and jitter.
 *
 * <p>This class provides a clean interface to execute channel opening operations with automatic
 * retry on various streaming-related exceptions.
 */
class OpenChannelRetryPolicy {

  private static final KCLogger LOGGER = new KCLogger(OpenChannelRetryPolicy.class.getName());

  // Retry policy constants
  /** Initial delay before the first retry attempt. */
  private static final Duration INITIAL_DELAY = Duration.ofSeconds(3);

  /** Maximum delay between retry attempts. */
  private static final Duration MAX_DELAY = Duration.ofMinutes(1);

  /** Exponential backoff multiplier (retry delays: 3s, 6s, 12s, 24s, 48s, 60s max). */
  private static final double BACKOFF_MULTIPLIER = 2.0;

  /** Random jitter added to retry delays to prevent thundering herd. */
  private static final Duration JITTER_DURATION = Duration.ofMillis(200);

  /**
   * Executes the provided channel opening action with retry handling.
   *
   * <p>On streaming-related exceptions, it will retry with exponential backoff and jitter. If all
   * retries are exhausted, it will throw the last encountered exception.
   *
   * @param channelOpenAction the action to execute (typically openChannelForTable call)
   * @param channelName the channel name for logging purposes
   * @param connectorConfig the connector configuration map
   * @return the result of the channel opening operation
   */
  static SnowflakeStreamingIngestChannel executeWithRetry(
      CheckedSupplier<SnowflakeStreamingIngestChannel> channelOpenAction,
      String channelName,
      Map<String, String> connectorConfig) {

    // Get the configurable max delay, using default if not specified
    int maxRetryAttempts =
        Integer.parseInt(
            connectorConfig.getOrDefault(
                SnowflakeSinkConnectorConfig.OPEN_CHANNEL_MAX_RETRY_ATTEMPTS,
                String.valueOf(
                    SnowflakeSinkConnectorConfig.OPEN_CHANNEL_MAX_RETRY_ATTEMPTS_DEFAULT)));

    Fallback<SnowflakeStreamingIngestChannel> fallback =
        Fallback.ofException(
            e -> {
              LOGGER.error(
                  "Open channel {} - max retry attempts reached. Last exception: {}",
                  channelName,
                  e.getLastException().getMessage(),
                  e.getLastException());
              throw e.getLastException();
            });

    RetryPolicy<SnowflakeStreamingIngestChannel> retryPolicy =
        RetryPolicy.<SnowflakeStreamingIngestChannel>builder()
            .handle(SFException.class)
            .withDelay(INITIAL_DELAY)
            .withBackoff(INITIAL_DELAY, MAX_DELAY, BACKOFF_MULTIPLIER)
            .withJitter(JITTER_DURATION)
            .withMaxAttempts(maxRetryAttempts)
            .onRetry(
                event ->
                    LOGGER.warn(
                        "Open channel {} retry attempt #{} due to: {}",
                        channelName,
                        event.getAttemptCount(),
                        event.getLastException().getMessage()))
            .onRetriesExceeded(
                event ->
                    LOGGER.error(
                        "Open channel {} retries exceeded. Last exception: {}",
                        channelName,
                        event.getException().getMessage()))
            .build();

    return Failsafe.with(fallback).compose(retryPolicy).get(channelOpenAction);
  }
}
