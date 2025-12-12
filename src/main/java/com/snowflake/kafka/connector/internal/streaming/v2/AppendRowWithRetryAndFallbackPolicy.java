package com.snowflake.kafka.connector.internal.streaming.v2;

import com.snowflake.ingest.streaming.SFException;
import com.snowflake.kafka.connector.internal.KCLogger;
import dev.failsafe.Failsafe;
import dev.failsafe.Fallback;
import dev.failsafe.RetryPolicy;
import dev.failsafe.function.CheckedRunnable;
import java.time.Duration;
import java.util.Set;

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
  private static final Duration RETRY_DELAY = Duration.ofSeconds(5);

  /** Delay before fallback attempt (channel reopening). */
  private static final Duration FALLBACK_DELAY = Duration.ofMillis(500);

  /** Random jitter added to retry delays to prevent potential partition starving. */
  private static final Duration JITTER_DURATION = Duration.ofMillis(200);

  /**
   * Executes the given action after a delay with jitter to prevent retry storms.
   *
   * @param action the action to execute after the delay
   * @param channelName the channel name for logging purposes
   */
  private static void withDelay(CheckedRunnable action, String channelName) throws Throwable {
    try {
      long delayMs =
          FALLBACK_DELAY.toMillis() + (long) (Math.random() * JITTER_DURATION.toMillis());

      LOGGER.info("Delaying channel recovery by {}ms for channel: {}", delayMs, channelName);
      Thread.sleep(delayMs);

      LOGGER.info("Executing channel recovery for channel: {}", channelName);
      action.run();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } catch (SFException e) {
      // Re-throw SFException unchanged so Fallback can handle it properly
      throw e;
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Executes the provided append row action with fallback handling.
   *
   * <p>On {@link SFException}, it will execute the fallback supplier to reopen the channel and
   * reset offsets after a simple blocking delay with jitter to prevent retry storms.
   *
   * @param appendRowAction the action to execute (typically channel.appendRow call)
   * @param fallbackSupplier the fallback action to execute on failure (channel reopening logic)
   * @param channelName the channel name for logging purposes
   * @return the result of the append row operation
   */
  static void executeWithRetryAndFallback(
      CheckedRunnable appendRowAction,
      FallbackSupplierWithException fallbackSupplier,
      String channelName) {

    Fallback<Void> reopenChannelFallbackExecutor =
        Fallback.<Void>builder(
                executionAttemptedEvent -> {
                  withDelay(
                      () -> fallbackSupplier.execute(executionAttemptedEvent.getLastException()),
                      channelName);
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

    RetryPolicy<Void> memoryBackpressureRetryPolicy =
        RetryPolicy.<Void>builder()
            .handleIf(AppendRowWithRetryAndFallbackPolicy::isRetryableError)
            .withDelay(RETRY_DELAY)
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

    Failsafe.with(reopenChannelFallbackExecutor)
        .compose(memoryBackpressureRetryPolicy)
        .run(appendRowAction);
  }

  private static final Set<String> RETRYABLE_ERROR_CODE_NAMES =
      Set.of(
          // 429 Too Many Requests
          "ReceiverSaturated",
          "MemoryThresholdExceeded",
          "MemoryThresholdExceededInContainer",
          // 503 Service Unavailable
          "HttpRetryableClientError");

  private static boolean isRetryableError(Throwable e) {
    if (!(e instanceof SFException)) {
      return false;
    }
    return RETRYABLE_ERROR_CODE_NAMES.contains(((SFException) e).getErrorCodeName());
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
