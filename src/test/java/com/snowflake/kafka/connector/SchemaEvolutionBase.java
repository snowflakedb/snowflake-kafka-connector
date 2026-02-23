package com.snowflake.kafka.connector;

import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.SNOWFLAKE_TOPICS2TABLE_MAP;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.snowflake.kafka.connector.internal.SnowflakeConnectionService;
import com.snowflake.kafka.connector.internal.SnowflakeConnectionServiceFactory;
import com.snowflake.kafka.connector.internal.TestUtils;
import com.snowflake.kafka.connector.internal.streaming.v2.client.StreamingClientFactory;
import java.util.Map;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.storage.StringConverter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

abstract class SchemaEvolutionBase extends ConnectClusterBaseIT {

  static final int PARTITION_COUNT = 1;
  static final int RECORD_COUNT = 100;
  static final int TOPIC_COUNT = 2;

  final ObjectMapper objectMapper = new ObjectMapper();

  String tableName;
  String connectorName;
  String topic0;
  String topic1;

  SnowflakeConnectionService snowflake;

  @BeforeEach
  void before() {

    tableName = TestUtils.randomTableName();
    connectorName = String.format("%s_connector", tableName);
    topic0 = tableName + "0";
    topic1 = tableName + "1";
    connectCluster.kafka().createTopic(topic0, PARTITION_COUNT);
    connectCluster.kafka().createTopic(topic1, PARTITION_COUNT);
    snowflake =
        SnowflakeConnectionServiceFactory.builder()
            .setProperties(TestUtils.transformProfileFileToConnectorConfiguration(false))
            .noCaching()
            .build();

    StreamingClientFactory.resetStreamingClientSupplier();
  }

  @AfterEach
  void after() {
    connectCluster.kafka().deleteTopic(topic0);
    connectCluster.kafka().deleteTopic(topic1);
    connectCluster.deleteConnector(connectorName);
    StreamingClientFactory.resetStreamingClientSupplier();
    TestUtils.dropTable(tableName);
  }

  Map<String, String> createConnectorConfig() {
    final String topics = topic0 + "," + topic1;
    final String topicsToTableMap = topic0 + ":" + tableName + "," + topic1 + ":" + tableName;

    final Map<String, String> config = defaultProperties(topics, connectorName);
    config.put(SNOWFLAKE_TOPICS2TABLE_MAP, topicsToTableMap);
    config.put(ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
    config.put("value.converter.schemas.enable", "false");
    config.put("errors.tolerance", "none");
    config.put("errors.log.enable", "true");
    config.put("errors.deadletterqueue.topic.name", "DLQ_TOPIC");
    config.put("errors.deadletterqueue.topic.replication.factor", "1");
    config.put("jmx", "true");
    return config;
  }

  void sendTombstoneRecords(final String topic) {
    // Send null tombstone
    connectCluster.kafka().produce(topic, null);
  }
}
