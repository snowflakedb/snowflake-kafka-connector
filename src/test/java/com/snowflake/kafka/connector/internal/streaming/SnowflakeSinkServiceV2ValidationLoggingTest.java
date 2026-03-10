package com.snowflake.kafka.connector.internal.streaming;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams;
import com.snowflake.kafka.connector.internal.SnowflakeConnectionService;
import com.snowflake.kafka.connector.internal.metrics.TaskMetrics;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for SnowflakeSinkServiceV2 pre-flight safety checks. Verifies validation configuration
 * logging for preventing data loss and task crashes.
 */
public class SnowflakeSinkServiceV2ValidationLoggingTest {

  private TestAppender testAppender;
  private Logger logger;

  @BeforeEach
  public void setUp() {
    // Capture logs from SnowflakeSinkServiceV2
    logger = Logger.getLogger(SnowflakeSinkServiceV2.class);
    testAppender = new TestAppender();
    logger.addAppender(testAppender);
    logger.setLevel(Level.INFO);
  }

  @AfterEach
  public void tearDown() {
    logger.removeAppender(testAppender);
  }

  /**
   * Test SAFE config: Validation enabled + errors.tolerance=none
   *
   * <p>Task aborts on validation failure - no data loss
   */
  @Test
  public void testSafeConfigValidationEnabledWithToleranceNone() {
    Map<String, String> config = new HashMap<>();
    config.put(KafkaConnectorConfigParams.NAME, "test-connector");
    config.put("task_id", "0");
    config.put(KafkaConnectorConfigParams.SNOWFLAKE_CLIENT_VALIDATION_ENABLED, "true");
    config.put(KafkaConnectorConfigParams.ERRORS_TOLERANCE_CONFIG, "none");

    SnowflakeSinkServiceV2 service = createServiceWithConfig(config);
    assertNotNull(service);

    // Verify INFO log contains expected message
    assertTrue(
        testAppender.containsMessage(Level.INFO, "Client-side validation enabled"),
        "Should log INFO about validation enabled");
    assertTrue(
        testAppender.containsMessage(
            Level.INFO, "Validation failures will abort the task (safe"),
        "Should log that task will abort on validation failure");
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
    config.put("task_id", "0");
    config.put(KafkaConnectorConfigParams.SNOWFLAKE_CLIENT_VALIDATION_ENABLED, "true");
    config.put(KafkaConnectorConfigParams.ERRORS_TOLERANCE_CONFIG, "all");
    config.put(
        KafkaConnectorConfigParams.ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_CONFIG, "my-dlq-topic");

    SnowflakeSinkServiceV2 service = createServiceWithConfig(config);
    assertNotNull(service);

    // Verify INFO log contains expected message with DLQ topic name
    assertTrue(
        testAppender.containsMessage(Level.INFO, "Client-side validation enabled"),
        "Should log INFO about validation enabled");
    assertTrue(
        testAppender.containsMessage(Level.INFO, "Validation failures will route to DLQ topic"),
        "Should log that failures route to DLQ");
    assertTrue(
        testAppender.containsMessage(Level.INFO, "my-dlq-topic"),
        "Should log the DLQ topic name");
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
    config.put("task_id", "0");
    config.put(KafkaConnectorConfigParams.SNOWFLAKE_CLIENT_VALIDATION_ENABLED, "true");
    config.put(KafkaConnectorConfigParams.ERRORS_TOLERANCE_CONFIG, "all");
    // NO DLQ configured

    SnowflakeSinkServiceV2 service = createServiceWithConfig(config);
    assertNotNull(service);

    // Verify ERROR log about unsafe configuration
    assertTrue(
        testAppender.containsMessage(Level.ERROR, "UNSAFE CONFIGURATION"),
        "Should log ERROR about unsafe configuration");
    assertTrue(
        testAppender.containsMessage(Level.ERROR, "SILENTLY DROPPED"),
        "Should warn about silent data loss");
    assertTrue(
        testAppender.containsMessage(Level.ERROR, "causing data loss"),
        "Should explicitly mention data loss");
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
    config.put("task_id", "0");
    config.put(KafkaConnectorConfigParams.SNOWFLAKE_CLIENT_VALIDATION_ENABLED, "false");

    SnowflakeSinkServiceV2 service = createServiceWithConfig(config);
    assertNotNull(service);

    // Verify WARN log about High-Performance Mode
    assertTrue(
        testAppender.containsMessage(Level.WARN, "CLIENT-SIDE VALIDATION DISABLED"),
        "Should log WARN about validation disabled");
    assertTrue(
        testAppender.containsMessage(Level.WARN, "High-Performance Mode"),
        "Should mention High-Performance Mode");
    assertTrue(
        testAppender.containsMessage(Level.WARN, "SSv2 Error Table"),
        "Should mention SSv2 Error Table requirement");
    assertTrue(
        testAppender.containsMessage(Level.WARN, "silently dropped"),
        "Should warn about silent data loss risk");
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
    config.put("task_id", "0");
    config.put("snowflake.enable.schematization", "true"); // Legacy config

    SnowflakeSinkServiceV2 service = createServiceWithConfig(config);
    assertNotNull(service);

    // Verify WARN log about legacy config
    assertTrue(
        testAppender.containsMessage(Level.WARN, "snowflake.enable.schematization"),
        "Should mention legacy config name");
    assertTrue(
        testAppender.containsMessage(Level.WARN, "not supported in KC v4"),
        "Should explain config is not supported");
    assertTrue(
        testAppender.containsMessage(Level.WARN, "ENABLE_SCHEMA_EVOLUTION"),
        "Should mention server-side schema evolution");
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
      // Constructor may throw due to missing configs - print and return null
      System.err.println("Failed to create service: " + e.getMessage());
      e.printStackTrace();
      return null;
    }
  }

  /** Test appender that captures log events for verification. */
  private static class TestAppender extends AppenderSkeleton {
    private final List<LoggingEvent> events = new ArrayList<>();

    @Override
    protected void append(LoggingEvent event) {
      events.add(event);
    }

    @Override
    public void close() {
      // No-op
    }

    @Override
    public boolean requiresLayout() {
      return false;
    }

    public boolean containsMessage(Level level, String messageFragment) {
      return events.stream()
          .anyMatch(
              event ->
                  event.getLevel().equals(level)
                      && event.getRenderedMessage().contains(messageFragment));
    }

    public List<LoggingEvent> getEvents() {
      return new ArrayList<>(events);
    }
  }
}
