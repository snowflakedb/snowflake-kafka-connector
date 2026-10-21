package com.snowflake.kafka.connector;

import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.SNOWFLAKE_AUTOCREATE_TABLE_TYPE;
import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.SNOWFLAKE_TOPICS2TABLE_MAP;
import static com.snowflake.kafka.connector.internal.TestUtils.assertWithRetry;
import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.snowflake.kafka.connector.internal.TestUtils;
import java.util.Map;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.junit.jupiter.api.Test;

/**
 * Verifies that with {@code snowflake.autocreate.table.type=interactive} and NO pre-created table,
 * the connector auto-creates an interactive table, ingests into it through the default streaming
 * pipe, and server-side schema evolution adds the record columns.
 *
 * <p>Like {@code SchemaEvolutionJsonIT#testSnowpipeStreamingSchemaEvolution}, this assumes the test
 * account has interactive table streaming enabled.
 */
class InteractiveAutoCreateIT extends SchemaEvolutionBase {

  @Test
  void autoCreatesInteractiveTable_whenTableTypeIsInteractive() throws Exception {
    final int partitionCount = 2;
    final int recordsPerPartition = 50;
    final String topic = tableName + "_interactive";
    connectCluster.kafka().createTopic(topic, partitionCount);

    // Intentionally no CREATE TABLE: the connector must create it.
    final Map<String, String> config = defaultProperties(topic, connectorName);
    config.put(
        ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG,
        org.apache.kafka.connect.storage.StringConverter.class.getName());
    config.put(ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG, JsonConverter.class.getName());
    config.put("value.converter.schemas.enable", "false");
    config.put(SNOWFLAKE_TOPICS2TABLE_MAP, topic + ":" + tableName);
    config.put(SNOWFLAKE_AUTOCREATE_TABLE_TYPE, "interactive");

    connectCluster.configureConnector(connectorName, config);
    waitForConnectorRunning(connectorName);

    for (int partition = 0; partition < partitionCount; partition++) {
      for (int i = 0; i < recordsPerPartition; i++) {
        final ObjectNode record = objectMapper.createObjectNode();
        record.put("fieldName", String.valueOf(i));
        connectCluster
            .kafka()
            .produce(topic, partition, "key-" + i, objectMapper.writeValueAsString(record));
      }
    }

    final int expected = partitionCount * recordsPerPartition;
    assertWithRetry(() -> snowflake.tableExist(tableName));
    assertWithRetry(() -> TestUtils.getNumberOfRows(tableName) == expected);

    final String isInteractive =
        TestUtils.executeQueryAndCollectResult(
            "show tables like ?",
            tableName,
            rs -> {
              try {
                return rs.next() ? rs.getString("is_interactive") : null;
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            });
    assertThat(isInteractive).isEqualTo("Y");

    TestUtils.checkTableSchema(
        tableName, Map.of("FIELDNAME", "VARCHAR", "RECORD_METADATA", "VARIANT"));

    connectCluster.kafka().deleteTopic(topic);
  }
}
