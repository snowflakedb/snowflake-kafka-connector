package com.snowflake.kafka.connector.internal.streaming.v2;

import com.snowflake.ingest.streaming.AppendResult;
import com.snowflake.ingest.streaming.SFException;
import com.snowflake.kafka.connector.internal.KCLogger;
import dev.failsafe.Failsafe;
import dev.failsafe.Fallback;
import dev.failsafe.RetryPolicy;
import dev.failsafe.function.CheckedSupplier;
import java.time.Duration;

/**
 * Policy class that encapsulates Failsafe logic for insert row operations with channel reopening
 * fallback functionality.
 *
 * <p>This class provides a clean interface to execute append row operations with automatic channel
 * recovery on {@link SFException}.
 */
class AppendRowWithRetryAndFallbackPolicy {

  private static final KCLogger LOGGER =
      new KCLogger(AppendRowWithRetryAndFallbackPolicy.class.getName());

  // Retry policy constants
  /** Delay before next retry attempt. */
  private static final Duration DELAY = Duration.ofSeconds(5);

  /** Random jitter added to retry delays to prevent potential partition starving. */
  private static final Duration JITTER_DURATION = Duration.ofMillis(200);

  /**
   * Executes the provided append row action with fallback handling.
   *
   * <p>On {@link SFException}, it will execute the fallback supplier to reopen the channel and
   * reset offsets, then throw a custom exception.
   *
   * @param appendRowAction the action to execute (typically channel.appendRow call)
   * @param fallbackSupplier the fallback action to execute on failure (channel reopening logic)
   * @param channelName the channel name for logging purposes
   * @return the result of the append row operation
   */
  static AppendResult executeWithRetryAndFallback(
      CheckedSupplier<AppendResult> appendRowAction,
      FallbackSupplierWithException fallbackSupplier,
      String channelName) {

    Fallback<AppendResult> reopenChannelFallbackExecutor =
        Fallback.<AppendResult>builder(
                executionAttemptedEvent -> {
                  fallbackSupplier.execute(executionAttemptedEvent.getLastException());
                })
            .handle(SFException.class)
            .onFailedAttempt(
                event ->
                    LOGGER.warn(
                        "Failed Attempt to invoke the appendRow API for channel: {}. Exception: {}",
                        channelName,
                        event.getLastException()))
            .onFailure(
                event ->
                    LOGGER.error(
                        "{} Failed to open Channel or fetching offsetToken for channel:{}."
                            + " Exception: {}",
                        "APPEND_ROW_FALLBACK",
                        channelName,
                        event.getException()))
            .build();

    RetryPolicy<AppendResult> memoryBackpressureRetryPolicy =
        RetryPolicy.<AppendResult>builder()
            .handleIf(AppendRowWithRetryAndFallbackPolicy::isMemoryBackpressure)
            .withDelay(DELAY)
            .withJitter(JITTER_DURATION)
            .withMaxAttempts(-1)
            .onRetry(
                event ->
                    LOGGER.warn(
                        "Failed attempt #{} to invoke appendRow API for channel: {}. Exception: {}",
                        event.getAttemptCount(),
                        channelName,
                        event.getLastException().getMessage()))
            .build();

    return Failsafe.with(reopenChannelFallbackExecutor)
        .compose(memoryBackpressureRetryPolicy)
        .get(appendRowAction);
  }

  private static boolean isMemoryBackpressure(Throwable e) {
    return e instanceof SFException
        && "MemoryThresholdExceeded".equals(((SFException) e).getErrorCodeName());
  }

  /**
   * Functional interface for fallback supplier that can throw exceptions.
   *
   * <p>This is used to encapsulate the channel reopening logic that needs to be executed when the
   * primary append row operation fails.
   */
  @FunctionalInterface
  interface FallbackSupplierWithException {
    /**
     * Executes the fallback logic.
     *
     * @param exception the original exception that caused the fallback to be triggered
     * @throws Exception if the fallback operation fails
     */
    void execute(Throwable exception) throws Exception;
  }
}
