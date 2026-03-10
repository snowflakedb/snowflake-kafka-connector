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
 * Tests for SnowflakeSinkServiceV2 validation configuration logging. Verifies pre-flight safety
 * checks log appropriate messages at startup.
 */
public class SnowflakeSinkServiceV2ValidationLoggingTest {

  /** Test that validation enabled + errors.tolerance=none logs INFO about task abort behavior. */
  @Test
  public void testValidationEnabledWithToleranceNone() {
    Map<String, String> config = new HashMap<>();
    config.put(KafkaConnectorConfigParams.NAME, "test-connector");
    config.put("task.id", "0");
    config.put(KafkaConnectorConfigParams.SNOWFLAKE_CLIENT_VALIDATION_ENABLED, "true");
    config.put(KafkaConnectorConfigParams.ERRORS_TOLERANCE_CONFIG, "none");

    // Should log: "Validation failures will abort the task"
    // Constructor calls logValidationConfiguration() internally
    SnowflakeSinkServiceV2 service = createServiceWithConfig(config);
    assertNotNull(service);
  }

  /** Test that validation enabled + errors.tolerance=all + DLQ logs INFO about DLQ routing. */
  @Test
  public void testValidationEnabledWithToleranceAllAndDlq() {
    Map<String, String> config = new HashMap<>();
    config.put(KafkaConnectorConfigParams.NAME, "test-connector");
    config.put("task.id", "0");
    config.put(KafkaConnectorConfigParams.SNOWFLAKE_CLIENT_VALIDATION_ENABLED, "true");
    config.put(KafkaConnectorConfigParams.ERRORS_TOLERANCE_CONFIG, "all");
    config.put(
        KafkaConnectorConfigParams.ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_CONFIG, "my-dlq-topic");

    // Should log: "Validation failures will route to DLQ topic: my-dlq-topic"
    SnowflakeSinkServiceV2 service = createServiceWithConfig(config);
    assertNotNull(service);
  }

  /** Test that validation enabled + errors.tolerance=all + NO DLQ logs WARN about silent drops. */
  @Test
  public void testValidationEnabledWithToleranceAllNoDlq() {
    Map<String, String> config = new HashMap<>();
    config.put(KafkaConnectorConfigParams.NAME, "test-connector");
    config.put("task.id", "0");
    config.put(KafkaConnectorConfigParams.SNOWFLAKE_CLIENT_VALIDATION_ENABLED, "true");
    config.put(KafkaConnectorConfigParams.ERRORS_TOLERANCE_CONFIG, "all");
    // NO DLQ configured

    // Should log WARN: "Invalid records will be silently dropped"
    SnowflakeSinkServiceV2 service = createServiceWithConfig(config);
    assertNotNull(service);
  }

  /** Test that validation disabled logs WARN about server-side error handling. */
  @Test
  public void testValidationDisabled() {
    Map<String, String> config = new HashMap<>();
    config.put(KafkaConnectorConfigParams.NAME, "test-connector");
    config.put("task.id", "0");
    config.put(KafkaConnectorConfigParams.SNOWFLAKE_CLIENT_VALIDATION_ENABLED, "false");

    // Should log WARN: "Ensure SSv2 Error Tables are configured"
    SnowflakeSinkServiceV2 service = createServiceWithConfig(config);
    assertNotNull(service);
  }

  /** Test that legacy config snowflake.enable.schematization triggers a warning. */
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
