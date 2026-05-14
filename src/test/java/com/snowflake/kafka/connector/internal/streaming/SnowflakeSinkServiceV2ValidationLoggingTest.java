package com.snowflake.kafka.connector.internal.streaming;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.snowflake.kafka.connector.config.SinkTaskConfig;
import com.snowflake.kafka.connector.config.SinkTaskConfigTestBuilder;
import com.snowflake.kafka.connector.config.SnowflakeValidation;
import com.snowflake.kafka.connector.internal.SnowflakeConnectionService;
import com.snowflake.kafka.connector.internal.metrics.TaskMetrics;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
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
    SinkTaskConfig config =
        SinkTaskConfigTestBuilder.builder()
            .connectorName("test-connector")
            .taskId("0")
            .validation(SnowflakeValidation.CLIENT_SIDE)
            .tolerateErrors(false)
            .build();

    SnowflakeSinkServiceV2 service = createServiceWithConfig(config);
    assertNotNull(service);

    // Verify INFO log contains expected message
    assertTrue(
        testAppender.containsMessage(Level.INFO, "Client-side validation enabled"),
        "Should log INFO about validation enabled");
    assertTrue(
        testAppender.containsMessage(Level.INFO, "Validation failures will abort the task (safe"),
        "Should log that task will abort on validation failure");
  }

  /**
   * Test SAFE config: Validation enabled + errors.tolerance=all + DLQ configured
   *
   * <p>Validation errors route to DLQ - no data loss
   */
  @Test
  public void testSafeConfigValidationEnabledWithToleranceAllAndDlq() {
    SinkTaskConfig config =
        SinkTaskConfigTestBuilder.builder()
            .connectorName("test-connector")
            .taskId("0")
            .validation(SnowflakeValidation.CLIENT_SIDE)
            .tolerateErrors(true)
            .dlqTopicName("my-dlq-topic")
            .build();

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
        testAppender.containsMessage(Level.INFO, "my-dlq-topic"), "Should log the DLQ topic name");
  }

  /**
   * Test UNSAFE config: Validation enabled + errors.tolerance=all + NO DLQ
   *
   * <p>Invalid records silently dropped - DATA LOSS
   */
  @Test
  public void testUnsafeConfigValidationEnabledWithToleranceAllNoDlq() {
    SinkTaskConfig config =
        SinkTaskConfigTestBuilder.builder()
            .connectorName("test-connector")
            .taskId("0")
            .validation(SnowflakeValidation.CLIENT_SIDE)
            .tolerateErrors(true)
            .dlqTopicName("")
            .build();

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
   * Test: Validation disabled with ERROR_LOGGING enabled on existing table.
   *
   * <p>Should NOT warn about missing error logging when ERROR_LOGGING is present.
   */
  @Test
  public void testValidationDisabledWithErrorLoggingEnabled() {
    SinkTaskConfig config =
        SinkTaskConfigTestBuilder.builder()
            .connectorName("test-connector")
            .taskId("0")
            .validation(SnowflakeValidation.SERVER_SIDE)
            .topicToTableMap(Map.of("topic1", "table1"))
            .build();

    SnowflakeSinkServiceV2 service =
        createServiceWithConfig(
            config,
            mockConn -> {
              when(mockConn.tableExist("table1")).thenReturn(true);
              when(mockConn.hasErrorLoggingEnabled("table1")).thenReturn(true);
            });
    assertNotNull(service);

    assertFalse(
        testAppender.containsMessage(Level.WARN, "does not have ERROR_LOGGING"),
        "Should NOT warn about missing error logging when it is enabled");
    assertTrue(
        testAppender.containsMessage(Level.INFO, "error table is active"),
        "Should log INFO confirming error table is active");
  }

  /**
   * Test: Validation disabled, multiple tables — one enabled, one disabled.
   *
   * <p>Verifies per-table iteration: only the disabled table gets a warning; the enabled table gets
   * an INFO confirmation.
   */
  @Test
  public void testValidationDisabledMultipleTablesPartialErrorLogging() {
    SinkTaskConfig config =
        SinkTaskConfigTestBuilder.builder()
            .connectorName("test-connector")
            .taskId("0")
            .validation(SnowflakeValidation.SERVER_SIDE)
            .topicToTableMap(Map.of("topic_ok", "table_ok", "topic_bad", "table_bad"))
            .build();

    SnowflakeSinkServiceV2 service =
        createServiceWithConfig(
            config,
            mockConn -> {
              when(mockConn.tableExist("table_ok")).thenReturn(true);
              when(mockConn.hasErrorLoggingEnabled("table_ok")).thenReturn(true);
              when(mockConn.tableExist("table_bad")).thenReturn(true);
              when(mockConn.hasErrorLoggingEnabled("table_bad")).thenReturn(false);
            });
    assertNotNull(service);

    assertTrue(
        testAppender.containsMessage(Level.WARN, "table_bad"),
        "Should warn about the table missing ERROR_LOGGING");
    assertFalse(
        testAppender.containsMessage(Level.WARN, "table_ok"),
        "Should NOT warn about the table that has ERROR_LOGGING enabled");
    assertTrue(
        testAppender.containsMessage(Level.INFO, "table_ok"),
        "Should log INFO confirmation for the table with ERROR_LOGGING enabled");
  }

  /**
   * Test: Validation disabled WITHOUT ERROR_LOGGING on existing table.
   *
   * <p>Should warn about the specific table and suggest ALTER TABLE.
   */
  @Test
  public void testValidationDisabledWithoutErrorLogging() {
    SinkTaskConfig config =
        SinkTaskConfigTestBuilder.builder()
            .connectorName("test-connector")
            .taskId("0")
            .validation(SnowflakeValidation.SERVER_SIDE)
            .topicToTableMap(Map.of("topic1", "table1"))
            .build();

    SnowflakeSinkServiceV2 service =
        createServiceWithConfig(
            config,
            mockConn -> {
              when(mockConn.tableExist("table1")).thenReturn(true);
              when(mockConn.hasErrorLoggingEnabled("table1")).thenReturn(false);
            });
    assertNotNull(service);

    assertTrue(testAppender.containsMessage(Level.WARN, "table1"), "Should mention the table name");
    assertTrue(
        testAppender.containsMessage(Level.WARN, "does not have ERROR_LOGGING"),
        "Should warn about missing error logging");
    assertTrue(
        testAppender.containsMessage(Level.WARN, "ALTER TABLE"),
        "Should suggest ALTER TABLE command");
  }

  /**
   * Test: Validation disabled, table does not exist yet.
   *
   * <p>Should NOT warn about error logging — table will be auto-created with ERROR_LOGGING = TRUE.
   */
  @Test
  public void testValidationDisabledTableNotExists() {
    SinkTaskConfig config =
        SinkTaskConfigTestBuilder.builder()
            .connectorName("test-connector")
            .taskId("0")
            .validation(SnowflakeValidation.SERVER_SIDE)
            .topicToTableMap(Map.of("topic1", "table1"))
            .build();

    SnowflakeSinkServiceV2 service =
        createServiceWithConfig(
            config,
            mockConn -> {
              when(mockConn.tableExist("table1")).thenReturn(false);
            });
    assertNotNull(service);

    assertFalse(
        testAppender.containsMessage(Level.WARN, "does not have ERROR_LOGGING"),
        "Should NOT warn about error logging for non-existent table");
  }

  /**
   * Test: Validation disabled, table is Iceberg.
   *
   * <p>Should warn that Iceberg tables do not support ERROR_LOGGING and not check
   * hasErrorLoggingEnabled.
   */
  @Test
  public void testValidationDisabledIcebergTableWarning() {
    SinkTaskConfig config =
        SinkTaskConfigTestBuilder.builder()
            .connectorName("test-connector")
            .taskId("0")
            .validation(SnowflakeValidation.SERVER_SIDE)
            .topicToTableMap(Map.of("topic1", "iceberg_table"))
            .build();

    SnowflakeSinkServiceV2 service =
        createServiceWithConfig(
            config,
            mockConn -> {
              when(mockConn.tableExist("iceberg_table")).thenReturn(true);
              when(mockConn.isIcebergTable("iceberg_table")).thenReturn(true);
            });
    assertNotNull(service);

    assertTrue(
        testAppender.containsMessage(Level.WARN, "Iceberg table"),
        "Should warn that the table is Iceberg");
    assertTrue(
        testAppender.containsMessage(Level.WARN, "do not support ERROR_LOGGING"),
        "Should warn that Iceberg does not support ERROR_LOGGING");
    assertFalse(
        testAppender.containsMessage(Level.WARN, "does not have ERROR_LOGGING"),
        "Should NOT emit the generic missing-ERROR_LOGGING warning for Iceberg tables");
  }

  /** Helper to create SnowflakeSinkServiceV2 with minimal mocked dependencies. */
  private SnowflakeSinkServiceV2 createServiceWithConfig(SinkTaskConfig config) {
    return createServiceWithConfig(config, mockConn -> {});
  }

  /** Helper with optional mock setup for connection service. */
  private SnowflakeSinkServiceV2 createServiceWithConfig(
      SinkTaskConfig config, Consumer<SnowflakeConnectionService> mockSetup) {
    SnowflakeConnectionService mockConn = mock(SnowflakeConnectionService.class);
    when(mockConn.isClosed()).thenReturn(false);
    when(mockConn.getTelemetryClient()).thenReturn(null);
    mockSetup.accept(mockConn);

    TaskMetrics mockMetrics = mock(TaskMetrics.class);

    try {
      return new SnowflakeSinkServiceV2(
          mockConn,
          config,
          null, // recordErrorReporter
          null, // sinkTaskContext
          java.util.Optional.empty(), // metricsJmxReporter
          mockMetrics);
    } catch (Exception e) {
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
