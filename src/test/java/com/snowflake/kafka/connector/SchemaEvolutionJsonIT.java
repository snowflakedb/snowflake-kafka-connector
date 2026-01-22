package com.snowflake.kafka.connector;

import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.SNOWFLAKE_TOPICS2TABLE_MAP;
import static com.snowflake.kafka.connector.internal.TestUtils.assertColumnNullable;
import static com.snowflake.kafka.connector.internal.TestUtils.assertTableColumnCount;
import static com.snowflake.kafka.connector.internal.TestUtils.assertWithRetry;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.snowflake.kafka.connector.internal.TestUtils;
import java.util.Map;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.junit.jupiter.api.Test;

class SchemaEvolutionJsonIT extends ConnectClusterBaseIT {

  @Test()
  void testSchemaEvolutionWithMultipleTopics() throws Exception {
    // two topics write to the same table. Each topic sends unique set of columns. Test that after
    // ingestion all exepcted columns are present in the database
    // given
    final Map<String, String> config = createConnectorConfig();
    config.put(ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG, JsonConverter.class.getName());

    connectCluster.configureConnector(connectorName, config);
    waitForConnectorRunning(connectorName);

    // when
    sendRecordsToTopic0();
    sendRecordsToTopic1();
    sendTombstoneRecords(topic1);
    sendTombstoneRecords(topic0);

    // then
    final int expectedTotalRecords = TOPIC_COUNT * RECORD_COUNT + 2; // + 2 tombstone records
    makeCommonAssertions(expectedTotalRecords);
  }

  @Test
  void testSchemaEvolutionIgnoreTombstone() throws Exception {
    // given
    final Map<String, String> config = createConnectorConfig();
    config.put("behavior.on.null.values", "IGNORE");
    config.put(ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG, JsonConverter.class.getName());

    connectCluster.configureConnector(connectorName, config);
    waitForConnectorRunning(connectorName);

    // when
    sendRecordsToTopic0();
    sendRecordsToTopic1();
    sendTombstoneRecords(topic1);
    sendTombstoneRecords(topic0);

    // then
    final int expectedTotalRecords = TOPIC_COUNT * RECORD_COUNT;
    makeCommonAssertions(expectedTotalRecords);
  }

  @Test
  void removeNotNullConstraint() throws Exception {
    // test that schema evolution is able to remove NON NULL constraint from the column
    // given
    final Map<String, String> config = createConnectorConfig();
    config.put(ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG, JsonConverter.class.getName());

    // COL1 has not null constraint
    snowflake.executeQueryWithParameters(
        "CREATE OR REPLACE TABLE "
            + tableName
            + " (COL1 VARCHAR NOT NULL, COL2 VARCHAR) ENABLE_SCHEMA_EVOLUTION = true");

    connectCluster.configureConnector(connectorName, config);
    waitForConnectorRunning(connectorName);

    final ObjectNode fullRow = objectMapper.createObjectNode();
    fullRow.put("col1", "col1value");
    fullRow.put("col2", "col2value");
    // inserting normal non null columns
    connectCluster.kafka().produce(topic0, objectMapper.writeValueAsString(fullRow));

    // then
    assertWithRetry(() -> TestUtils.getNumberOfRows(tableName) == 1);
    assertTableColumnCount(tableName, 3);
    TestUtils.checkTableSchema(
        tableName,
        Map.of(
            "COL1", "VARCHAR",
            "COL2", "VARCHAR",
            "RECORD_METADATA", "VARIANT"));
    assertColumnNullable(tableName, "COL1", false);

    // col1 not initialized
    final ObjectNode rowWithNullValue = objectMapper.createObjectNode();
    rowWithNullValue.put("col2", "col2value");

    // now insert row with col1 == null
    connectCluster.kafka().produce(topic0, objectMapper.writeValueAsString(rowWithNullValue));

    assertWithRetry(() -> TestUtils.getNumberOfRows(tableName) == 2);
    // constraint has been removed
    assertColumnNullable(tableName, "COL1", true);
  }

  @Test
  void testSchemaEvolutionIgnoreTombstoneAfterSmt() throws Exception {
    // given
    final Map<String, String> config = createConnectorConfig();
    config.put("behavior.on.null.values", "IGNORE");
    config.put("errors.tolerance", "all");
    config.put(
        SNOWFLAKE_TOPICS2TABLE_MAP,
        topic0 + ":" + tableName); // reading only from one topic for this test
    config.put("transforms", "extractField");
    config.put(
        "transforms.extractField.type", "org.apache.kafka.connect.transforms.ExtractField$Value");
    config.put("transforms.extractField.field", "optionalField");
    config.put(ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG, JsonConverter.class.getName());

    connectCluster.configureConnector(connectorName, config);
    waitForConnectorRunning(connectorName);

    // produce records that should result in null value after SMT transformation
    for (int i = 0; i < RECORD_COUNT; i++) {
      final ObjectNode record = objectMapper.createObjectNode();
      record.put("PERFORMANCE_STRING", "Excellent");
      record.put("APPROVAL", true);
      connectCluster.kafka().produce(topic0, objectMapper.writeValueAsString(record));
    }

    // produce records that should result in non-null value after SMT transformation
    for (int i = 0; i < RECORD_COUNT; i++) {
      final ObjectNode record = objectMapper.createObjectNode();
      final ObjectNode optionalFieldValue = objectMapper.createObjectNode();
      optionalFieldValue.put("hasSomething", true);
      record.set("optionalField", optionalFieldValue);
      connectCluster.kafka().produce(topic0, objectMapper.writeValueAsString(record));
    }

    // then
    final int expectedTotalRecords =
        RECORD_COUNT; // not 2x, just half of the records produced should get into destination table
    assertWithRetry(() -> snowflake.tableExist(tableName));
    assertWithRetry(() -> TestUtils.getNumberOfRows(tableName) == expectedTotalRecords);
    assertTableColumnCount(tableName, 2);
    TestUtils.checkTableSchema(
        tableName,
        Map.of(
            "HASSOMETHING", "BOOLEAN",
            "RECORD_METADATA", "VARIANT"));
  }

  @Test
  void testSchemaEvolutionDropTable() throws Exception {
    // given
    final Map<String, String> config = createConnectorConfig();
    config.put(ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG, JsonConverter.class.getName());

    connectCluster.configureConnector(connectorName, config);
    waitForConnectorRunning(connectorName);

    sendRecordsToTopic0();
    sendRecordsToTopic1();
    sendTombstoneRecords(topic1);
    sendTombstoneRecords(topic0);

    // then
    final int expectedTotalRecords = TOPIC_COUNT * RECORD_COUNT + 2; // +2 tombstone records
    makeCommonAssertions(expectedTotalRecords);
    // wait 10 secs to make sure precommit advances consumer group offset and
    // the connector does not reingest the same records after the restart
    // precommit frequency is decided by offset.flush.interval.ms parameter
    Thread.sleep(10000);

    TestUtils.dropTable(tableName);
    connectCluster.restartConnectorAndTasks(connectorName, false, true, false);
    waitForConnectorRunning(connectorName);

    sendRecordsToTopic0();
    sendRecordsToTopic1();
    sendTombstoneRecords(topic1);
    sendTombstoneRecords(topic0);

    makeCommonAssertions(expectedTotalRecords);
  }

  private void sendRecordsToTopic0() throws JsonProcessingException {
    // Record schema for topic 0: PERFORMANCE_STRING, RATING_INT
    for (int i = 0; i < RECORD_COUNT; i++) {
      connectCluster.kafka().produce(topic0, createTopic0Record());
    }
  }

  private void sendRecordsToTopic1() throws JsonProcessingException {
    // Record schema for topic 1: PERFORMANCE_STRING, RATING_DOUBLE, APPROVAL
    for (int i = 0; i < RECORD_COUNT; i++) {
      connectCluster.kafka().produce(topic1, createTopic1Record());
    }
  }

  private String createTopic0Record() throws JsonProcessingException {
    final ObjectNode record = objectMapper.createObjectNode();
    record.put("PERFORMANCE_STRING", "Excellent");
    record.put("RATING_INT", 100);
    return objectMapper.writeValueAsString(record);
  }

  private String createTopic1Record() throws JsonProcessingException {
    final ObjectNode record = objectMapper.createObjectNode();
    record.put("PERFORMANCE_STRING", "Excellent");
    record.put("RATING_DOUBLE", 0.99);
    record.put("APPROVAL", true);
    return objectMapper.writeValueAsString(record);
  }

  private void makeCommonAssertions(final int expectedTotalRecords) throws Exception {
    assertWithRetry(() -> snowflake.tableExist(tableName));
    assertWithRetry(() -> TestUtils.getNumberOfRows(tableName) == expectedTotalRecords);
    assertTableColumnCount(tableName, 5);
    TestUtils.checkTableSchema(
        tableName,
        Map.of(
            "PERFORMANCE_STRING", "VARCHAR",
            "RECORD_METADATA", "VARIANT",
            "RATING_INT", "NUMBER",
            "APPROVAL", "BOOLEAN",
            "RATING_DOUBLE", "NUMBER"));
  }
}
