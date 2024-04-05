package com.snowflake.kafka.connector;

import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.BUFFER_COUNT_RECORDS;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.BUFFER_FLUSH_TIME_SEC;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.NAME;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.SNOWFLAKE_DATABASE;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.SNOWFLAKE_PRIVATE_KEY;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.SNOWFLAKE_SCHEMA;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.SNOWFLAKE_URL;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.SNOWFLAKE_USER;
import static org.apache.kafka.connect.runtime.ConnectorConfig.CONNECTOR_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.TASKS_MAX_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.sink.SinkConnector.TOPICS_CONFIG;

import com.snowflake.kafka.connector.fake.SnowflakeFakeSinkConnector;
import com.snowflake.kafka.connector.fake.SnowflakeFakeSinkTask;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.connect.storage.StringConverter;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ConnectClusterBaseIT {

  EmbeddedConnectCluster connectCluster;
  static final Integer TASK_NUMBER = 1;

  @BeforeAll
  public void beforeAll() {
    connectCluster =
        new EmbeddedConnectCluster.Builder()
            .name("kafka-push-connector-connect-cluster")
            .numWorkers(3)
            .build();
    connectCluster.start();
  }

  @BeforeEach
  public void before() {
    SnowflakeFakeSinkTask.resetRecords();
  }

  @AfterAll
  public void afterAll() {
    if (connectCluster != null) {
      connectCluster.stop();
      connectCluster = null;
    }
  }

  @AfterEach
  public void after() {
    SnowflakeFakeSinkTask.resetRecords();
  }

  final Map<String, String> defaultProperties(String topicName, String connectorName) {
    Map<String, String> config = new HashMap<>();

    // kafka connect specific
    // real connector will be specified with SNOW-1055561
    config.put(CONNECTOR_CLASS_CONFIG, SnowflakeFakeSinkConnector.class.getName());
    config.put(TOPICS_CONFIG, topicName);
    config.put(TASKS_MAX_CONFIG, TASK_NUMBER.toString());
    config.put(KEY_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
    config.put(VALUE_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());

    // kafka push specific
    config.put(NAME, connectorName);
    config.put(SNOWFLAKE_URL, "https://test.testregion.snowflakecomputing.com:443");
    config.put(SNOWFLAKE_USER, "testName");
    config.put(SNOWFLAKE_PRIVATE_KEY, "testPrivateKey");
    config.put(SNOWFLAKE_DATABASE, "testDbName");
    config.put(SNOWFLAKE_SCHEMA, "testSchema");
    config.put(BUFFER_COUNT_RECORDS, "1000000");
    config.put(BUFFER_FLUSH_TIME_SEC, "1");

    return config;
  }

  final void waitForConnectorRunning(String connectorName) {
    try {
      connectCluster
          .assertions()
          .assertConnectorAndAtLeastNumTasksAreRunning(
              connectorName, 1, "The connector did not start.");
    } catch (InterruptedException e) {
      throw new IllegalStateException("The connector is not running");
    }
  }
}
