package com.snowflake.kafka.connector.internal.streaming;

import static java.time.temporal.ChronoUnit.SECONDS;

import com.snowflake.kafka.connector.internal.KCLogger;
import dev.failsafe.Failsafe;
import dev.failsafe.FailsafeExecutor;
import dev.failsafe.Fallback;
import dev.failsafe.RetryPolicy;
import dev.failsafe.function.CheckedSupplier;
import java.time.Duration;

/**
 * Class that separates Failsafe specific logic (retries and fallback) from the actual channel logic
 */
public class LatestCommitedOffsetTokenExecutor {

  private static final KCLogger LOGGER =
      new KCLogger(LatestCommitedOffsetTokenExecutor.class.getName());

  private static final Duration DURATION_BETWEEN_GET_OFFSET_TOKEN_RETRY = Duration.ofSeconds(1);
  protected static final int MAX_GET_OFFSET_TOKEN_RETRIES = 3;

  public static FailsafeExecutor<Long> getExecutor(
      String channelName,
      Class<? extends Throwable> exceptionClass,
      CheckedSupplier<Long> fallbackSupplier) {
    RetryPolicy<Long> retryPolicy = createRetryPolicy(exceptionClass);
    Fallback<Long> fallback = createFallback(channelName, exceptionClass, fallbackSupplier);

    return Failsafe.with(fallback)
        .onFailure(
            event ->
                LOGGER.error(
                    "[OFFSET_TOKEN_RETRY_FAILSAFE] Failure to fetch offsetToken even after retry"
                        + " and fallback from snowflake for channel:{}, elapsedTimeSeconds:{}",
                    channelName,
                    event.getElapsedTime().get(SECONDS),
                    event.getException()))
        .compose(retryPolicy);
  }

  private static RetryPolicy<Long> createRetryPolicy(
      Class<? extends Throwable> retryExceptionClass) {
    return RetryPolicy.<Long>builder()
        .handle(retryExceptionClass)
        .withDelay(DURATION_BETWEEN_GET_OFFSET_TOKEN_RETRY)
        .withMaxAttempts(MAX_GET_OFFSET_TOKEN_RETRIES)
        .onRetry(
            event ->
                LOGGER.warn(
                    "[OFFSET_TOKEN_RETRY_POLICY] retry for getLatestCommittedOffsetToken. Retry"
                        + " no:{}, message:{}",
                    event.getAttemptCount(),
                    event.getLastException().getMessage()))
        .build();
  }

  private static Fallback<Long> createFallback(
      String channelName,
      Class<? extends Throwable> exceptionClass,
      CheckedSupplier<Long> fallbackSupplier) {
    return Fallback.builder(fallbackSupplier)
        .handle(exceptionClass)
        .onFailure(
            event ->
                LOGGER.error(
                    "[OFFSET_TOKEN_FALLBACK] Failed to open Channel/fetch offsetToken for"
                        + " channel:{}, exception:{}",
                    channelName,
                    event.getException().toString()))
        .build();
  }
}
