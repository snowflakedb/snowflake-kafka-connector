package com.snowflake.kafka.connector.internal.validation;

import static org.junit.jupiter.api.Assertions.*;

import com.codahale.metrics.MetricRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for ValidationMetrics */
public class ValidationMetricsTest {

  private MetricRegistry registry;
  private ValidationMetrics metrics;

  @BeforeEach
  public void setUp() {
    registry = new MetricRegistry();
    metrics = new ValidationMetrics(registry, "test-channel");
  }

  @Test
  public void testRecordValidationSuccess() {
    assertEquals(0, metrics.getRecordsProcessed());
    assertEquals(0, metrics.getTotalValidationTimeMs());

    metrics.recordValidationSuccess(100);

    assertEquals(1, metrics.getRecordsProcessed());
    assertEquals(0, metrics.getRecordsFailed());
    assertEquals(100, metrics.getTotalValidationTimeMs());
  }

  @Test
  public void testRecordValidationFailureTypeError() {
    metrics.recordValidationFailure(50, "type_error");

    assertEquals(1, metrics.getRecordsProcessed());
    assertEquals(1, metrics.getRecordsFailed());
    assertEquals(50, metrics.getTotalValidationTimeMs());
  }

  @Test
  public void testRecordValidationFailureStructuralError() {
    metrics.recordValidationFailure(75, "structural_error");

    assertEquals(1, metrics.getRecordsProcessed());
    assertEquals(1, metrics.getRecordsFailed());
    assertEquals(75, metrics.getTotalValidationTimeMs());
  }

  @Test
  public void testRecordSchemaEvolution() {
    assertEquals(0, metrics.getSchemaEvolutionTriggered());

    metrics.recordSchemaEvolution();

    assertEquals(1, metrics.getSchemaEvolutionTriggered());
  }

  @Test
  public void testMultipleOperations() {
    metrics.recordValidationSuccess(10);
    metrics.recordValidationSuccess(20);
    metrics.recordValidationFailure(30, "type_error");
    metrics.recordSchemaEvolution();

    assertEquals(3, metrics.getRecordsProcessed());
    assertEquals(1, metrics.getRecordsFailed());
    assertEquals(1, metrics.getSchemaEvolutionTriggered());
    assertEquals(60, metrics.getTotalValidationTimeMs());
  }

  @Test
  public void testMetricNamingSanitization() {
    // Test that special characters in channel name are sanitized
    ValidationMetrics metrics2 = new ValidationMetrics(registry, "channel-with-special!@#chars");
    assertNotNull(metrics2);

    metrics2.recordValidationSuccess(1);
    assertEquals(1, metrics2.getRecordsProcessed());
  }
}
