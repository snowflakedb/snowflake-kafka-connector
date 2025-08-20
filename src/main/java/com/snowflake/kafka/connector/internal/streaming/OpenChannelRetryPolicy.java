package com.snowflake.kafka.connector.internal.streaming;

import com.snowflake.kafka.connector.internal.KCLogger;
import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import dev.failsafe.function.CheckedSupplier;
import java.time.Duration;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import net.snowflake.ingest.utils.SFException;

/**
 * Policy class that encapsulates retry logic for opening streaming channels with exponential
 * backoff and jitter.
 *
 * <p>This class provides a clean interface to execute channel opening operations with automatic
 * retry on HTTP 429 (rate limiting) errors from Snowflake streaming service.
 */
class OpenChannelRetryPolicy {

  private static final KCLogger LOGGER = new KCLogger(OpenChannelRetryPolicy.class.getName());

  private static final String RATE_LIMIT_MESSAGE_PART = "HTTP Status: 429";

  // Retry policy constants
  /** Initial delay before the first retry attempt. */
  private static final Duration INITIAL_DELAY = Duration.ofSeconds(2);

  /** Maximum delay between retry attempts. */
  private static final Duration MAX_DELAY = Duration.ofSeconds(8);

  /** Exponential backoff multiplier (retry delays: 2s, 4s, 8s max). */
  private static final double BACKOFF_MULTIPLIER = 2.0;

  /** Random jitter added to retry delays to prevent thundering herd. */
  private static final Duration JITTER_DURATION = Duration.ofMillis(200);

  /**
   * Executes the provided channel opening action with retry handling.
   *
   * <p>On SFException containing "429" (HTTP rate limiting), it will retry with exponential backoff
   * and jitter with unlimited retry attempts. Other exceptions are not retried.
   *
   * @param channelOpenAction the action to execute (typically openChannelForTable call)
   * @param channelName the channel name for logging purposes
   * @return the result of the channel opening operation
   */
  static SnowflakeStreamingIngestChannel executeWithRetry(
      CheckedSupplier<SnowflakeStreamingIngestChannel> channelOpenAction, String channelName) {

    RetryPolicy<SnowflakeStreamingIngestChannel> retryPolicy =
        RetryPolicy.<SnowflakeStreamingIngestChannel>builder()
            .handleIf(OpenChannelRetryPolicy::isRetryableError)
            .withBackoff(INITIAL_DELAY, MAX_DELAY, BACKOFF_MULTIPLIER)
            .withJitter(JITTER_DURATION)
            .withMaxAttempts(-1)
            .onRetry(
                event ->
                    LOGGER.warn(
                        "Open channel {} retry attempt #{} due to: {}",
                        channelName,
                        event.getAttemptCount(),
                        event.getLastException().getMessage()))
            .build();

    return Failsafe.with(retryPolicy).get(channelOpenAction);
  }

  private static boolean isRetryableError(Throwable e) {
    return e instanceof SFException && e.getMessage().contains(RATE_LIMIT_MESSAGE_PART);
  }
}
