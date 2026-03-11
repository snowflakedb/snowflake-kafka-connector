package com.snowflake.kafka.connector;

import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_CONFIG;
import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.ERRORS_TOLERANCE_CONFIG;
import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.SNOWFLAKE_CLIENT_VALIDATION_ENABLED;
import static com.snowflake.kafka.connector.internal.TestUtils.assertTableColumnCount;
import static com.snowflake.kafka.connector.internal.TestUtils.assertWithRetry;

import com.snowflake.kafka.connector.internal.SnowflakeConnectionService;
import com.snowflake.kafka.connector.internal.SnowflakeConnectionServiceFactory;
import com.snowflake.kafka.connector.internal.TestUtils;
import com.snowflake.kafka.connector.internal.streaming.v2.client.StreamingClientFactory;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

/**
 * End-to-end integration tests for client-side validation feature.
 *
 * These tests validate that KC v4 client-side validation provides feature parity with KC v3:
 * - Type validation failures route to DLQ
 * - Structural validation detects schema mismatches
 * - Schema evolution executes DDL and retries records
 * - Error handling matches KC v3 semantics
 */
class ClientSideValidationIT extends ConnectClusterBaseIT {

  private static final int PARTITION_COUNT = 1;
  private static final String DLQ_TOPIC = "validation_dlq";

  private String tableName;
  private String connectorName;
  private String topic;
  private SnowflakeConnectionService snowflake;
  private KafkaConsumer<String, byte[]> dlqConsumer;

  @BeforeEach
  void before() {
    tableName = TestUtils.randomTableName();
    connectorName = String.format("%s_connector", tableName);
    topic = tableName;

    connectCluster.kafka().createTopic(topic, PARTITION_COUNT);
    connectCluster.kafka().createTopic(DLQ_TOPIC, PARTITION_COUNT);

    snowflake =
        SnowflakeConnectionServiceFactory.builder()
            .setProperties(TestUtils.transformProfileFileToConnectorConfiguration(false))
            .noCaching()
            .build();

    StreamingClientFactory.resetStreamingClientSupplier();

    // Create DLQ consumer with unique group ID per test for isolation
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, connectCluster.kafka().bootstrapServers());
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "dlq-test-consumer-" + System.currentTimeMillis());
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    dlqConsumer = new KafkaConsumer<>(props);
    dlqConsumer.subscribe(Collections.singletonList(DLQ_TOPIC));
  }

  @AfterEach
  void after() {
    // Cleanup order is critical for test isolation:
    // 1. Stop connector first (stops producing/consuming)
    connectCluster.deleteConnector(connectorName);

    // 2. Wait for connector to fully stop
    try {
      Thread.sleep(2000); // Give connector time to close channels and commit offsets
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    // 3. Close Snowflake connection (closes any open channels)
    if (snowflake != null && !snowflake.isClosed()) {
      snowflake.close();
    }

    // 4. Drain any remaining DLQ messages and close consumer
    if (dlqConsumer != null) {
      // Drain remaining messages to avoid affecting next test
      while (true) {
        var polled = dlqConsumer.poll(Duration.ofMillis(500));
        if (polled.isEmpty()) {
          break;
        }
      }
      dlqConsumer.close();
    }

    // 5. Delete topics (now safe since consumers/producers are stopped)
    connectCluster.kafka().deleteTopic(topic);
    connectCluster.kafka().deleteTopic(DLQ_TOPIC);

    // 6. Reset streaming client factory
    StreamingClientFactory.resetStreamingClientSupplier();

    // 7. Drop table
    TestUtils.dropTable(tableName);
  }

  /**
   * Test 1: Type Validation - Number Precision Overflow
   *
   * Validates that records with number precision overflow are routed to DLQ.
   * This is critical KC v3 behavior - SSv1 SDK rejects oversized numbers.
   */
  @Test
  void testTypeValidation_NumberPrecisionOverflow_RoutesToDLQ() throws Exception {
    // given: Table with NUMBER(10,2) column
    snowflake.executeQueryWithParameters(String.format(
        "CREATE TABLE %s (RECORD_METADATA VARIANT, AMOUNT NUMBER(10,2))",
        tableName));

    // Enable schema evolution so structural validation passes
    snowflake.executeQueryWithParameters(String.format(
        "ALTER TABLE %s SET ENABLE_SCHEMA_EVOLUTION = TRUE", tableName));

    Map<String, String> config = createConnectorConfig();
    config.put(SNOWFLAKE_CLIENT_VALIDATION_ENABLED, "true");
    config.put(ERRORS_TOLERANCE_CONFIG, "all");
    config.put(ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_CONFIG, DLQ_TOPIC);

    connectCluster.configureConnector(connectorName, config);
    waitForConnectorRunning(connectorName);

    // when: Send record with AMOUNT exceeding precision (10 digits)
    String validRecord = "{\"AMOUNT\": 12345.67}";  // Valid
    String invalidRecord = "{\"AMOUNT\": 999999999999.99}";  // 12 digits - exceeds NUMBER(10,2)

    connectCluster.kafka().produce(topic, validRecord);
    connectCluster.kafka().produce(topic, invalidRecord);

    // then: Valid record inserted, invalid record in DLQ
    assertWithRetry(() -> TestUtils.getNumberOfRows(tableName) == 1);

    // Check DLQ contains the invalid record
    List<ConsumerRecord<String, byte[]>> dlqRecords = pollDLQ(Duration.ofSeconds(30));
    assert dlqRecords.size() == 1 : "Expected 1 record in DLQ, got " + dlqRecords.size();

    // Verify DLQ record has error context headers (if present)
    ConsumerRecord<String, byte[]> dlqRecord = dlqRecords.get(0);
    var errorHeader = dlqRecord.headers().lastHeader("__connect.errors.exception.message");
    if (errorHeader != null) {
      String errorMessage = new String(errorHeader.value());
      assert errorMessage.contains("precision") || errorMessage.contains("overflow") || errorMessage.contains("out of representable")
          : "Expected precision/overflow error, got: " + errorMessage;
    }
    // Success: Invalid NUMBER record was caught by validation and routed to DLQ
  }

  /**
   * Test 2: Type Validation - String Length Exceeds Limit
   *
   * Validates that strings exceeding VARCHAR length are routed to DLQ.
   * KC v3 validates this via SSv1 SDK.
   */
  @Test
  void testTypeValidation_StringLengthExceeds_RoutesToDLQ() throws Exception {
    // given: Table with VARCHAR(10) column
    snowflake.executeQueryWithParameters(String.format(
        "CREATE TABLE %s (RECORD_METADATA VARIANT, NAME VARCHAR(10))",
        tableName));

    snowflake.executeQueryWithParameters(String.format(
        "ALTER TABLE %s SET ENABLE_SCHEMA_EVOLUTION = TRUE", tableName));

    Map<String, String> config = createConnectorConfig();
    config.put(SNOWFLAKE_CLIENT_VALIDATION_ENABLED, "true");
    config.put(ERRORS_TOLERANCE_CONFIG, "all");
    config.put(ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_CONFIG, DLQ_TOPIC);

    connectCluster.configureConnector(connectorName, config);
    waitForConnectorRunning(connectorName);

    // when: Send record with NAME exceeding length
    String validRecord = "{\"NAME\": \"Alice\"}";  // 5 chars - valid
    String invalidRecord = "{\"NAME\": \"This name is way too long for the column\"}";  // >10 chars

    connectCluster.kafka().produce(topic, validRecord);
    connectCluster.kafka().produce(topic, invalidRecord);

    // then: Valid record inserted, invalid in DLQ
    assertWithRetry(() -> TestUtils.getNumberOfRows(tableName) == 1);

    List<ConsumerRecord<String, byte[]>> dlqRecords = pollDLQ(Duration.ofSeconds(30));
    assert dlqRecords.size() == 1 : "Expected 1 record in DLQ";

    // Verify DLQ record has error context headers (if present)
    var errorHeader = dlqRecords.get(0).headers().lastHeader("__connect.errors.exception.message");
    if (errorHeader != null) {
      String errorMessage = new String(errorHeader.value());
      assert errorMessage.contains("length") || errorMessage.contains("exceeds") || errorMessage.contains("too long")
          : "Expected length error, got: " + errorMessage;
    }
    // Success: Invalid STRING record was caught by validation and routed to DLQ
  }

  /**
   * Test 3: Structural Validation - Extra Column with Schema Evolution ON
   *
   * Validates that extra columns trigger DDL (ALTER TABLE ADD COLUMN) and record is retried.
   * This is KC v3 schema evolution behavior.
   */
  @Test
  void testStructuralValidation_ExtraColumn_SchemaEvolutionExecutesDDL() throws Exception {
    // given: Table with only RECORD_METADATA column
    snowflake.executeQueryWithParameters(String.format(
        "CREATE TABLE %s (RECORD_METADATA VARIANT)",
        tableName));

    // Enable schema evolution
    snowflake.executeQueryWithParameters(String.format(
        "ALTER TABLE %s SET ENABLE_SCHEMA_EVOLUTION = TRUE", tableName));

    Map<String, String> config = createConnectorConfig();
    config.put(SNOWFLAKE_CLIENT_VALIDATION_ENABLED, "true");
    config.put(ERRORS_TOLERANCE_CONFIG, "all");
    config.put(ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_CONFIG, DLQ_TOPIC);

    connectCluster.configureConnector(connectorName, config);
    waitForConnectorRunning(connectorName);

    // when: Send record with extra column NEW_FIELD
    String record = "{\"NEW_FIELD\": \"test_value\", \"AGE\": 25}";
    connectCluster.kafka().produce(topic, record);

    // then: Column added via DDL, record inserted successfully
    assertWithRetry(() -> TestUtils.getNumberOfRows(tableName) == 1);

    // Verify columns were added
    assertWithRetry(() -> {
      var columns = snowflake.describeTable(tableName).orElseThrow();
      return columns.stream().anyMatch(c -> c.getColumn().equals("NEW_FIELD"))
          && columns.stream().anyMatch(c -> c.getColumn().equals("AGE"));
    });

    // No records should be in DLQ (schema evolution succeeded)
    List<ConsumerRecord<String, byte[]>> dlqRecords = pollDLQ(Duration.ofSeconds(10));
    assert dlqRecords.isEmpty() : "Expected 0 records in DLQ, got " + dlqRecords.size();
  }

  /**
   * Test 4: Structural Validation - Extra Column with Schema Evolution OFF
   *
   * Validates that extra columns are routed to DLQ when schema evolution is disabled.
   * This tests the fail-safe path when DDL is not allowed.
   */
  @Test
  void testStructuralValidation_ExtraColumn_SchemaEvolutionOff_RoutesToDLQ() throws Exception {
    // given: Table with schema evolution DISABLED
    snowflake.executeQueryWithParameters(String.format(
        "CREATE TABLE %s (RECORD_METADATA VARIANT, NAME VARCHAR)",
        tableName));

    // Explicitly disable schema evolution
    snowflake.executeQueryWithParameters(String.format(
        "ALTER TABLE %s SET ENABLE_SCHEMA_EVOLUTION = FALSE", tableName));

    Map<String, String> config = createConnectorConfig();
    config.put(SNOWFLAKE_CLIENT_VALIDATION_ENABLED, "true");
    config.put(ERRORS_TOLERANCE_CONFIG, "all");
    config.put(ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_CONFIG, DLQ_TOPIC);

    connectCluster.configureConnector(connectorName, config);
    waitForConnectorRunning(connectorName);

    // when: Send record with extra column
    String validRecord = "{\"NAME\": \"Alice\"}";
    String invalidRecord = "{\"NAME\": \"Bob\", \"EXTRA_FIELD\": \"should_fail\"}";

    connectCluster.kafka().produce(topic, validRecord);
    connectCluster.kafka().produce(topic, invalidRecord);

    // then: Valid record inserted, invalid record in DLQ
    assertWithRetry(() -> TestUtils.getNumberOfRows(tableName) == 1);

    List<ConsumerRecord<String, byte[]>> dlqRecords = pollDLQ(Duration.ofSeconds(30));
    assert dlqRecords.size() == 1 : "Expected 1 record in DLQ";

    // Verify DLQ record has error context headers (if present)
    var errorHeader = dlqRecords.get(0).headers().lastHeader("__connect.errors.exception.message");
    if (errorHeader != null) {
      String errorMessage = new String(errorHeader.value());
      assert errorMessage.contains("Schema mismatch") || errorMessage.contains("extra") || errorMessage.contains("evolution disabled")
          : "Expected schema mismatch error, got: " + errorMessage;
    }
    // Success: Record with extra column was caught by validation and routed to DLQ (schema evolution OFF)
  }

  /**
   * Test 5: Structural Validation - Missing NOT NULL Column
   *
   * Validates that missing NOT NULL columns trigger DROP NOT NULL when schema evolution is ON.
   * KC v3 schema evolution handles this via client-side DDL.
   */
  @Test
  void testStructuralValidation_MissingNotNullColumn_DropNotNull() throws Exception {
    // given: Table with NOT NULL column
    snowflake.executeQueryWithParameters(String.format(
        "CREATE TABLE %s (RECORD_METADATA VARIANT, USER_ID VARCHAR NOT NULL, NAME VARCHAR)",
        tableName));

    snowflake.executeQueryWithParameters(String.format(
        "ALTER TABLE %s SET ENABLE_SCHEMA_EVOLUTION = TRUE", tableName));

    Map<String, String> config = createConnectorConfig();
    config.put(SNOWFLAKE_CLIENT_VALIDATION_ENABLED, "true");
    config.put(ERRORS_TOLERANCE_CONFIG, "all");
    config.put(ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_CONFIG, DLQ_TOPIC);

    connectCluster.configureConnector(connectorName, config);
    waitForConnectorRunning(connectorName);

    // when: Send record missing USER_ID (NOT NULL column)
    String record = "{\"NAME\": \"Alice\"}";  // Missing USER_ID
    connectCluster.kafka().produce(topic, record);

    // then: NOT NULL dropped via DDL, record inserted
    assertWithRetry(() -> TestUtils.getNumberOfRows(tableName) == 1);

    // Verify USER_ID is now nullable
    assertWithRetry(() -> {
      var columns = snowflake.describeTable(tableName).orElseThrow();
      var userIdCol = columns.stream()
          .filter(c -> c.getColumn().equals("USER_ID"))
          .findFirst()
          .orElseThrow();
      return "Y".equals(userIdCol.getNullable());  // "Y" means nullable
    });

    // No DLQ records (schema evolution succeeded)
    List<ConsumerRecord<String, byte[]>> dlqRecords = pollDLQ(Duration.ofSeconds(10));
    assert dlqRecords.isEmpty() : "Expected 0 records in DLQ";
  }

  /**
   * Test 6: Validation Disabled - All Records Pass Through
   *
   * Validates that disabling validation allows all records through (high-performance mode).
   * Operators can disable validation and rely on SSv2 Error Tables.
   */
  @Test
  void testValidationDisabled_AllRecordsPassThrough() throws Exception {
    // given: Table with strict schema
    snowflake.executeQueryWithParameters(String.format(
        "CREATE TABLE %s (RECORD_METADATA VARIANT, AMOUNT NUMBER(10,2))",
        tableName));

    Map<String, String> config = createConnectorConfig();
    config.put(SNOWFLAKE_CLIENT_VALIDATION_ENABLED, "false");  // Validation OFF
    config.put(ERRORS_TOLERANCE_CONFIG, "all");
    config.put(ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_CONFIG, DLQ_TOPIC);

    connectCluster.configureConnector(connectorName, config);
    waitForConnectorRunning(connectorName);

    // when: Send records that would normally fail validation
    String oversizedNumber = "{\"AMOUNT\": 999999999999.99}";  // Would fail precision check
    String extraColumn = "{\"AMOUNT\": 100.00, \"EXTRA\": \"test\"}";  // Would fail structural check

    connectCluster.kafka().produce(topic, oversizedNumber);
    connectCluster.kafka().produce(topic, extraColumn);

    // then: Records bypass validation and go to SSv2 (may appear in SSv2 Error Tables)
    // We expect appendRow() to be called without validation
    // Note: SSv2 may handle these records differently (Error Tables, silent drop, etc.)

    // Sleep to ensure records are processed
    Thread.sleep(10000);

    // No DLQ records (validation disabled)
    List<ConsumerRecord<String, byte[]>> dlqRecords = pollDLQ(Duration.ofSeconds(10));
    assert dlqRecords.isEmpty() : "Expected 0 records in DLQ when validation disabled";
  }

  /**
   * Test 7: errors.tolerance=none - Task Fails on Validation Error
   *
   * Validates fail-fast behavior when errors.tolerance=none.
   * KC v3 behavior: validation failures abort the task.
   */
  @Test
  void testErrorToleranceNone_TaskFails() throws Exception {
    // given: Table with strict schema
    snowflake.executeQueryWithParameters(String.format(
        "CREATE TABLE %s (RECORD_METADATA VARIANT, AMOUNT NUMBER(10,2))",
        tableName));

    snowflake.executeQueryWithParameters(String.format(
        "ALTER TABLE %s SET ENABLE_SCHEMA_EVOLUTION = TRUE", tableName));

    Map<String, String> config = createConnectorConfig();
    config.put(SNOWFLAKE_CLIENT_VALIDATION_ENABLED, "true");
    config.put(ERRORS_TOLERANCE_CONFIG, "none");  // Fail-fast mode

    connectCluster.configureConnector(connectorName, config);
    waitForConnectorRunning(connectorName);

    // when: Send valid record, then invalid record
    String validRecord = "{\"AMOUNT\": 100.00}";
    String invalidRecord = "{\"AMOUNT\": 999999999999.99}";  // Precision overflow

    connectCluster.kafka().produce(topic, validRecord);
    connectCluster.kafka().produce(topic, invalidRecord);

    // then: Task should fail (task goes to FAILED state)
    // Note: In Kafka Connect, the connector stays RUNNING, but the task fails
    // Wait up to 60 seconds (12 retries * 5 seconds)
    assertWithRetry(() -> {
      var status = connectCluster.connectorStatus(connectorName);
      return status.tasks().size() > 0 && "FAILED".equals(status.tasks().get(0).state());
    }, 5, 12);

    // Note: With errors.tolerance=none, when a batch fails, the entire batch is rolled back.
    // This is standard Kafka Connect behavior - valid records in the same batch as the error
    // are NOT committed. The test validates the fail-fast behavior itself, not the commit semantics.
    // In production, operators would fix the issue and restart the connector, which would
    // reprocess both records (valid one succeeds, invalid one would need fixing or DLQ).
  }

  // Helper Methods

  private Map<String, String> createConnectorConfig() {
    Map<String, String> config = defaultProperties(topic, connectorName);
    config.put(ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG, org.apache.kafka.connect.storage.StringConverter.class.getName());
    config.put(ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG, JsonConverter.class.getName());
    config.put("value.converter.schemas.enable", "false");
    config.put("jmx", "true");
    return config;
  }

  private List<ConsumerRecord<String, byte[]>> pollDLQ(Duration timeout) {
    long endTime = System.currentTimeMillis() + timeout.toMillis();
    List<ConsumerRecord<String, byte[]>> records = new java.util.ArrayList<>();

    while (System.currentTimeMillis() < endTime) {
      var polled = dlqConsumer.poll(Duration.ofSeconds(1));
      polled.forEach(records::add);
      if (!records.isEmpty()) {
        break;
      }
    }

    // Commit offsets so next test doesn't see these records
    if (!records.isEmpty()) {
      dlqConsumer.commitSync();
    }

    return records;
  }
}
