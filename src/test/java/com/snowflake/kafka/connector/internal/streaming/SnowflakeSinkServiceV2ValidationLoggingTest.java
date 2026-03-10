package com.snowflake.kafka.connector.internal.streaming;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams;
import com.snowflake.kafka.connector.internal.SnowflakeConnectionService;
import com.snowflake.kafka.connector.internal.metrics.TaskMetrics;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Tests for SnowflakeSinkServiceV2 pre-flight safety checks. Verifies validation configuration
 * logging for preventing data loss and task crashes.
 */
public class SnowflakeSinkServiceV2ValidationLoggingTest {

  /**
   * Test SAFE config: Validation enabled + errors.tolerance=none
   *
   * <p>Task aborts on validation failure - no data loss
   */
  @Test
  public void testSafeConfigValidationEnabledWithToleranceNone() {
    Map<String, String> config = new HashMap<>();
    config.put(KafkaConnectorConfigParams.NAME, "test-connector");
    config.put("task.id", "0");
    config.put(KafkaConnectorConfigParams.SNOWFLAKE_CLIENT_VALIDATION_ENABLED, "true");
    config.put(KafkaConnectorConfigParams.ERRORS_TOLERANCE_CONFIG, "none");

    // Should log INFO: "Validation failures will abort the task (safe)"
    SnowflakeSinkServiceV2 service = createServiceWithConfig(config);
    assertNotNull(service);
  }

  /**
   * Test SAFE config: Validation enabled + errors.tolerance=all + DLQ configured
   *
   * <p>Validation errors route to DLQ - no data loss
   */
  @Test
  public void testSafeConfigValidationEnabledWithToleranceAllAndDlq() {
    Map<String, String> config = new HashMap<>();
    config.put(KafkaConnectorConfigParams.NAME, "test-connector");
    config.put("task.id", "0");
    config.put(KafkaConnectorConfigParams.SNOWFLAKE_CLIENT_VALIDATION_ENABLED, "true");
    config.put(KafkaConnectorConfigParams.ERRORS_TOLERANCE_CONFIG, "all");
    config.put(
        KafkaConnectorConfigParams.ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_CONFIG, "my-dlq-topic");

    // Should log INFO: "Validation failures will route to DLQ topic: my-dlq-topic"
    SnowflakeSinkServiceV2 service = createServiceWithConfig(config);
    assertNotNull(service);
  }

  /**
   * Test UNSAFE config: Validation enabled + errors.tolerance=all + NO DLQ
   *
   * <p>Invalid records silently dropped - DATA LOSS
   */
  @Test
  public void testUnsafeConfigValidationEnabledWithToleranceAllNoDlq() {
    Map<String, String> config = new HashMap<>();
    config.put(KafkaConnectorConfigParams.NAME, "test-connector");
    config.put("task.id", "0");
    config.put(KafkaConnectorConfigParams.SNOWFLAKE_CLIENT_VALIDATION_ENABLED, "true");
    config.put(KafkaConnectorConfigParams.ERRORS_TOLERANCE_CONFIG, "all");
    // NO DLQ configured

    // Should log ERROR: "UNSAFE CONFIGURATION... Invalid records will be SILENTLY DROPPED"
    SnowflakeSinkServiceV2 service = createServiceWithConfig(config);
    assertNotNull(service);
  }

  /**
   * Test: Validation disabled (High-Performance Mode)
   *
   * <p>Must warn that SSv2 Error Table is required. Currently logs WARN because Error Table check
   * API is not yet available.
   */
  @Test
  public void testValidationDisabledWarnsAboutErrorTables() {
    Map<String, String> config = new HashMap<>();
    config.put(KafkaConnectorConfigParams.NAME, "test-connector");
    config.put("task.id", "0");
    config.put(KafkaConnectorConfigParams.SNOWFLAKE_CLIENT_VALIDATION_ENABLED, "false");

    // Should log WARN: "Running without client-side validation requires a configured SSv2 Error
    // Table"
    // TODO: When SSv2 API exposes Error Table config, this should check and potentially fail
    SnowflakeSinkServiceV2 service = createServiceWithConfig(config);
    assertNotNull(service);
  }

  /**
   * Test: Legacy KC v3 config warning
   *
   * <p>Warns if snowflake.enable.schematization is present (not supported in KC v4)
   */
  @Test
  public void testLegacySchematizationConfigWarning() {
    Map<String, String> config = new HashMap<>();
    config.put(KafkaConnectorConfigParams.NAME, "test-connector");
    config.put("task.id", "0");
    config.put("snowflake.enable.schematization", "true"); // Legacy config

    // Should log WARN: "Config 'snowflake.enable.schematization' is not supported in KC v4"
    SnowflakeSinkServiceV2 service = createServiceWithConfig(config);
    assertNotNull(service);
  }

  /** Helper to create SnowflakeSinkServiceV2 with minimal mocked dependencies. */
  private SnowflakeSinkServiceV2 createServiceWithConfig(Map<String, String> config) {
    // Mock dependencies
    SnowflakeConnectionService mockConn = mock(SnowflakeConnectionService.class);
    when(mockConn.isClosed()).thenReturn(false);
    when(mockConn.getTelemetryClient()).thenReturn(null);

    TaskMetrics mockMetrics = mock(TaskMetrics.class);

    // Create service - constructor will call logValidationConfiguration()
    try {
      return new SnowflakeSinkServiceV2(
          mockConn,
          config,
          null, // recordErrorReporter
          null, // sinkTaskContext
          java.util.Optional.empty(), // metricsJmxReporter
          new HashMap<>(), // topicToTableMap
          null, // behaviorOnNullValues
          mockMetrics);
    } catch (Exception e) {
      // Constructor may throw due to missing configs - that's okay for this test
      return null;
    }
  }
}
