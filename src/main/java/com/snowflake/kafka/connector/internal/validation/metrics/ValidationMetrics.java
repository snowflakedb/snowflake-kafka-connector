/*
 * Copyright (c) 2026 Snowflake Computing Inc. All rights reserved.
 *
 * Stub implementation for validation metrics tracking.
 */

package com.snowflake.kafka.connector.internal.validation.metrics;

/**
 * Tracks validation metrics for observability. This is a simple stub implementation that records
 * metrics in memory. Future versions could integrate with JMX or other monitoring systems.
 */
public class ValidationMetrics {
  private long validationSuccessCount = 0;
  private long validationFailureCount = 0;
  private long totalValidationTimeMs = 0;

  public void recordValidationSuccess(long durationMs) {
    validationSuccessCount++;
    totalValidationTimeMs += durationMs;
  }

  public void recordValidationFailure(long durationMs, String errorType) {
    validationFailureCount++;
    totalValidationTimeMs += durationMs;
  }

  public void recordSchemaEvolution(boolean success) {
    // Stub method for schema evolution tracking
    // In full implementation, this would track schema evolution events
  }

  public long getValidationSuccessCount() {
    return validationSuccessCount;
  }

  public long getValidationFailureCount() {
    return validationFailureCount;
  }

  public long getTotalValidationTimeMs() {
    return totalValidationTimeMs;
  }

  public double getAverageValidationTimeMs() {
    long total = validationSuccessCount + validationFailureCount;
    return total == 0 ? 0.0 : (double) totalValidationTimeMs / total;
  }
}
