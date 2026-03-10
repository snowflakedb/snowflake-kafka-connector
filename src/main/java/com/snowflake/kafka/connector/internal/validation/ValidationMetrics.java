/*
 * Copyright (c) 2026 Snowflake Computing Inc. All rights reserved.
 */

package com.snowflake.kafka.connector.internal.validation;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Metrics for client-side validation operations. Tracks validation success/failure rates, latency,
 * and schema evolution triggers.
 *
 * <p>Thread-safe: All metrics are thread-safe via Dropwizard Metrics library.
 */
public class ValidationMetrics {
  private final Counter recordsProcessed;
  private final Counter recordsFailed;
  private final Counter schemaEvolutionTriggered;
  private final Histogram validationLatency;

  // Counters by error type
  private final Counter typeErrors;
  private final Counter structuralErrors;
  private final Counter exceptionErrors;

  // Track total validation time for rate calculation
  private final AtomicLong totalValidationTimeMs = new AtomicLong(0);

  public ValidationMetrics(MetricRegistry registry, String channelName) {
    String prefix = "snowflake.kafka.connector.validation." + sanitizeName(channelName) + ".";

    this.recordsProcessed = registry.counter(prefix + "records.processed");
    this.recordsFailed = registry.counter(prefix + "records.failed");
    this.schemaEvolutionTriggered = registry.counter(prefix + "schema.evolution.triggered");
    this.validationLatency = registry.histogram(prefix + "latency.ms");

    this.typeErrors = registry.counter(prefix + "errors.type");
    this.structuralErrors = registry.counter(prefix + "errors.structural");
    this.exceptionErrors = registry.counter(prefix + "errors.exception");
  }

  /**
   * Record successful validation.
   *
   * @param durationMs validation duration in milliseconds
   */
  public void recordValidationSuccess(long durationMs) {
    recordsProcessed.inc();
    validationLatency.update(durationMs);
    totalValidationTimeMs.addAndGet(durationMs);
  }

  /**
   * Record validation failure.
   *
   * @param durationMs validation duration in milliseconds
   * @param errorType type of error: "type_error", "structural_error", "exception"
   */
  public void recordValidationFailure(long durationMs, String errorType) {
    recordsProcessed.inc();
    recordsFailed.inc();
    validationLatency.update(durationMs);
    totalValidationTimeMs.addAndGet(durationMs);

    // Track by error type
    switch (errorType) {
      case "type_error":
        typeErrors.inc();
        break;
      case "structural_error":
        structuralErrors.inc();
        break;
      case "exception":
        exceptionErrors.inc();
        break;
      default:
        // Unknown error type
        exceptionErrors.inc();
    }
  }

  /** Record schema evolution trigger. */
  public void recordSchemaEvolution() {
    schemaEvolutionTriggered.inc();
  }

  /** Get total records processed count. */
  public long getRecordsProcessed() {
    return recordsProcessed.getCount();
  }

  /** Get total records failed count. */
  public long getRecordsFailed() {
    return recordsFailed.getCount();
  }

  /** Get total schema evolution triggers. */
  public long getSchemaEvolutionTriggered() {
    return schemaEvolutionTriggered.getCount();
  }

  /** Get total validation time in milliseconds. */
  public long getTotalValidationTimeMs() {
    return totalValidationTimeMs.get();
  }

  /** Sanitize channel name for metric name (replace special characters with underscores). */
  private String sanitizeName(String name) {
    return name.replaceAll("[^a-zA-Z0-9_]", "_");
  }
}
