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
   * Initial delay before the first recovery attempt. Backoff doubles each attempt up to {@link
   * #MAX_DELAY}. Package-private + non-final so tests can override.
   */
  @VisibleForTesting static Duration BASE_DELAY = Duration.ofSeconds(1);

  /** Cap on the exponential backoff delay. */
  @VisibleForTesting static Duration MAX_DELAY = Duration.ofSeconds(30);

  /** Jitter factor (±) applied to each delay to prevent retry storms across partitions. */
  @VisibleForTesting static double JITTER_FACTOR = 0.2;

  /**
   * Computes an exponential backoff delay for the given consecutive attempt number, capped at
   * {@link #MAX_DELAY} with ±{@link #JITTER_FACTOR} jitter. Mirrors the formula used by Failsafe's
   * {@code RetryPolicy.withBackoff(...).withJitter(...)}.
   *
   * @param attempt 1-based consecutive recovery attempt number
   */
  static Duration computeBackoffDelay(int attempt) {
    long baseMs = BASE_DELAY.toMillis();
    long capMs = MAX_DELAY.toMillis();
    long shift = Math.min(Math.max(attempt - 1, 0), 30); // bound to avoid long overflow
    long delayMs = Math.min(baseMs << shift, capMs);
    long jitterMs = (long) ((Math.random() * 2 - 1) * delayMs * JITTER_FACTOR);
    return Duration.ofMillis(Math.max(0, delayMs + jitterMs));
  }

  /**
   * Executes the provided append row action with fallback handling.
   *
   * <p>On retryable {@link SFException} (backpressure errors), throws {@link BackpressureException}
   * to signal the batch-level insert loop that the batch should be abandoned and offsets should be
   * rewound. The channel remains valid.
   *
   * <p>On non-retryable {@link SFException}, executes the fallback supplier to reopen the channel
   * and reset offsets. The supplier owns its own backoff delay (see {@link #computeBackoffDelay}).
   *
   * @param appendRowAction the action to execute (typically channel.appendRow call)
   * @param fallbackSupplier the fallback action to execute on non-retryable failure
   * @param channelName the channel name for logging purposes
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

                  // Non-retryable error: proceed with channel reopening. The fallback supplier is
                  // responsible for any backoff delay before reopen — see callers'
                  // computeBackoffDelay usage. Done here in the supplier (not the policy) because
                  // the supplier owns the consecutive-attempt counter that drives the backoff.
                  succeeded[0] = false;
                  fallbackSupplier.execute(lastException);
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
