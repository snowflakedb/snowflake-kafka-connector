package com.snowflake.kafka.connector.internal.streaming.v2;

import com.google.common.annotations.VisibleForTesting;
import com.snowflake.ingest.streaming.SFException;
import com.snowflake.kafka.connector.internal.KCLogger;
import dev.failsafe.Failsafe;
import dev.failsafe.Fallback;
import dev.failsafe.function.CheckedRunnable;
import java.time.Duration;

/**
 * Policy class that encapsulates Failsafe logic for insert row operations with channel reopening
 * fallback functionality.
 *
 * <p>This class provides a clean interface to execute append row operations with automatic channel
 * recovery on non-retryable {@link SFException}. The fallback supplier determines the recovery
 * strategy:
 *
 * <ul>
 *   <li>Retryable backpressure errors throw {@link BackpressureException}
 *   <li>Client-invalid errors trigger client recreation via the fallback, which throws {@link
 *       ClientRecreationException}
 *   <li>Other errors trigger channel-level recovery (reopen channel)
 * </ul>
 *
 * Both {@link BackpressureException} and {@link ClientRecreationException} propagate out of
 * Failsafe to signal the batch-level insert loop.
 */
class AppendRowWithFallbackPolicy {

  private static final KCLogger LOGGER = new KCLogger(AppendRowWithFallbackPolicy.class.getName());

  /**
   * Delay before fallback attempt (channel reopening / client recreation). Sized for SSv2 pipe
   * failovers (1-3 min) — with MAX_CONSECUTIVE_RECOVERIES=30 upstream, 5s per attempt covers ~2.5
   * min without spamming Snowflake during the outage.
   *
   * <p>Package-private + non-final so tests can override it to avoid long sleeps.
   */
  @VisibleForTesting static Duration FALLBACK_DELAY = Duration.ofSeconds(5);

  /** Random jitter added to fallback delays to prevent retry storms. */
  @VisibleForTesting static Duration JITTER_DURATION = Duration.ofMillis(500);

  /**
   * Executes the given action after a delay with jitter to prevent retry storms.
   *
   * @param action the action to execute after the delay
   * @param channelName the channel name for logging purposes
   */
  private static void withDelay(CheckedRunnable action, String channelName) throws Throwable {
    long delayMs = FALLBACK_DELAY.toMillis() + (long) (Math.random() * JITTER_DURATION.toMillis());
    LOGGER.info("Delaying channel recovery by {}ms for channel: {}", delayMs, channelName);
    try {
      Thread.sleep(delayMs);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return;
    }
    LOGGER.info("Executing channel recovery for channel: {}", channelName);
    action.run();
  }

  /**
   * Executes the provided append row action with fallback handling.
   *
   * <p>On retryable {@link SFException} (backpressure errors), throws {@link BackpressureException}
   * to signal the batch-level insert loop that the batch should be abandoned and offsets should be
   * rewound. The channel remains valid.
   *
   * <p>On non-retryable {@link SFException}, it will execute the fallback supplier to reopen the
   * channel and reset offsets after a simple blocking delay with jitter to prevent retry storms.
   *
   * @param appendRowAction the action to execute (typically channel.appendRow call)
   * @param fallbackSupplier the fallback action to execute on non-retryable failure (channel
   *     reopening logic)
   * @param channelName the channel name for logging purposes
   */
  /**
   * @return true if the append row action succeeded normally, false if the fallback was executed
   *     (meaning the record was NOT inserted). When this returns false, callers must NOT advance
   *     processedOffset — the fallback's recovery logic has already reset offset state.
   */
  static boolean executeWithFallback(
      CheckedRunnable appendRowAction,
      FallbackSupplierWithException fallbackSupplier,
      String channelName) {

    boolean[] succeeded = {true};

    Fallback<Void> reopenChannelFallbackExecutor =
        Fallback.<Void>builder(
                executionAttemptedEvent -> {
                  Throwable lastException = executionAttemptedEvent.getLastException();

                  // Check if this is a retryable backpressure error
                  if (BackpressureException.isRetryableError(lastException)) {
                    // The channel is still valid; throw BackpressureException to signal
                    // the batch-level insert loop to abandon the batch and rewind offsets
                    throw new BackpressureException((SFException) lastException);
                  }

                  // Non-retryable error: proceed with channel reopening
                  succeeded[0] = false;
                  withDelay(() -> fallbackSupplier.execute(lastException), channelName);
                })
            .handle(SFException.class)
            .onFailedAttempt(
                event ->
                    LOGGER.warn(
                        "Failed Attempt to invoke the appendRow API for channel: {}. Exception: {}",
                        channelName,
                        event.getLastException()))
            .onFailure(
                event -> {
                  if (event.getException() instanceof BackpressureException) {
                    LOGGER.warn(
                        "Backpressure on channel {}: {}",
                        channelName,
                        event.getException().getMessage());
                  } else if (event.getException() instanceof ClientRecreationException) {
                    LOGGER.warn(
                        "Client recreation triggered on channel {}: {}",
                        channelName,
                        event.getException().getMessage());
                  } else {
                    LOGGER.error(
                        "{} Failed to open Channel or fetching offsetToken for channel:{}."
                            + " Exception: {}",
                        "APPEND_ROW_FALLBACK",
                        channelName,
                        event.getException());
                  }
                })
            .build();

    Failsafe.with(reopenChannelFallbackExecutor).run(appendRowAction);
    return succeeded[0];
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
