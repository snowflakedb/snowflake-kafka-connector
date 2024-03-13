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
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.snowflake.kafka.connector.fake.SnowflakeFakeSinkConnector;
import com.snowflake.kafka.connector.fake.SnowflakeFakeSinkTask;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.runtime.AbstractStatus;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.storage.StringConverter;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ConnectClusterBaseIT {

  protected EmbeddedConnectCluster connectCluster;

  protected static final String TEST_TOPIC = "kafka-int-test";
  protected static final String TEST_CONNECTOR_NAME = "test-connector";
  protected static final Integer TASK_NUMBER = 1;
  private static final Duration CONNECTOR_MAX_STARTUP_TIME = Duration.ofSeconds(20);

  @BeforeAll
  public void beforeAll() {
    connectCluster =
        new EmbeddedConnectCluster.Builder()
            .name("kafka-push-connector-connect-cluster")
            .numWorkers(3)
            .build();
    connectCluster.start();
    connectCluster.kafka().createTopic(TEST_TOPIC);
    connectCluster.configureConnector(TEST_CONNECTOR_NAME, createProperties());
    await().timeout(CONNECTOR_MAX_STARTUP_TIME).until(this::isConnectorRunning);
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

  @Test
  public void connectorShouldConsumeMessagesFromTopic() {
    connectCluster.kafka().produce(TEST_TOPIC, "test1");
    connectCluster.kafka().produce(TEST_TOPIC, "test2");

    await()
        .untilAsserted(
            () -> {
              List<SinkRecord> records = SnowflakeFakeSinkTask.getRecords();
              assertThat(records).hasSize(2);
              assertThat(records.stream().map(SinkRecord::value)).containsExactly("test1", "test2");
            });
  }

  protected Map<String, String> createProperties() {
    Map<String, String> config = new HashMap<>();

    // kafka connect specific
    // real connector will be specified with SNOW-1055561
    config.put(CONNECTOR_CLASS_CONFIG, SnowflakeFakeSinkConnector.class.getName());
    config.put(TOPICS_CONFIG, TEST_TOPIC);
    config.put(TASKS_MAX_CONFIG, TASK_NUMBER.toString());
    config.put(KEY_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
    config.put(VALUE_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());

    // kafka push specific
    config.put(NAME, TEST_CONNECTOR_NAME);
    config.put(SNOWFLAKE_URL, "https://test.testregion.snowflakecomputing.com:443");
    config.put(SNOWFLAKE_USER, "testName");
    config.put(SNOWFLAKE_PRIVATE_KEY, "testPrivateKey");
    config.put(SNOWFLAKE_DATABASE, "testDbName");
    config.put(SNOWFLAKE_SCHEMA, "testSchema");
    config.put(BUFFER_COUNT_RECORDS, "1000000");
    config.put(BUFFER_FLUSH_TIME_SEC, "1");

    return config;
  }

  private boolean isConnectorRunning() {
    ConnectorStateInfo status = connectCluster.connectorStatus(TEST_CONNECTOR_NAME);
    return status != null
        && status.connector().state().equals(AbstractStatus.State.RUNNING.toString())
        && status.tasks().size() >= TASK_NUMBER
        && status.tasks().stream()
            .allMatch(state -> state.state().equals(AbstractStatus.State.RUNNING.toString()));
  }
}
