package com.snowflake.kafka.connector;

import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.*;
import static org.apache.kafka.connect.runtime.ConnectorConfig.CONNECTOR_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.TASKS_MAX_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.sink.SinkConnector.TOPICS_CONFIG;

import com.snowflake.kafka.connector.internal.TestUtils;
import com.snowflake.kafka.connector.internal.streaming.IngestionMethodConfig;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.connect.storage.StringConverter;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInstance;

/**
 * Base class for integration tests using an embedded Kafka Connect cluster.
 * 
 * <p>NOTE: Tests extending this class currently use real Snowflake connections for SSv2.
 * TODO: Implement a fake/mock infrastructure for SSv2 testing similar to the old SSv1 
 * FakeStreamingClientHandler to enable faster, isolated testing without real Snowflake connections.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class ConnectClusterBaseIT {

  protected EmbeddedConnectCluster connectCluster;

  // TODO: Add SSv2 fake/mock infrastructure here when needed
  // protected FakeStreamingClientV2Handler fakeStreamingClientV2Handler;

  static final Integer TASK_NUMBER = 1;

  @BeforeAll
  public void beforeAll() {
    Map<String, String> workerConfig = new HashMap<>();
    workerConfig.put("plugin.discovery", "hybrid_warn");
    connectCluster =
        new EmbeddedConnectCluster.Builder()
            .name("kafka-push-connector-connect-cluster")
            .numWorkers(3)
            .workerProps(workerConfig)
            .build();
    connectCluster.start();
  }

  @BeforeEach
  public void beforeEach() {
    // TODO: Initialize SSv2 fake/mock infrastructure when implemented
    // For now, tests will use real Snowflake connections
  }

  @AfterEach
  public void afterEach() {
    // TODO: Clean up SSv2 fake/mock infrastructure when implemented
  }

  @AfterAll
  public void afterAll() {
    if (connectCluster != null) {
      connectCluster.stop();
      connectCluster = null;
    }
  }

  protected final Map<String, String> defaultProperties(String topicName, String connectorName) {
    Map<String, String> config = TestUtils.getConf();

    config.put(CONNECTOR_CLASS_CONFIG, SnowflakeStreamingSinkConnector.class.getName());
    config.put(NAME, connectorName);
    config.put(TOPICS_CONFIG, topicName);
    config.put(Utils.SF_ROLE, "testrole_kafka");
    config.put(SNOWPIPE_STREAMING_MAX_CLIENT_LAG, "1");
    config.put(TASKS_MAX_CONFIG, TASK_NUMBER.toString());
    config.put(SNOWPIPE_STREAMING_CLOSE_CHANNELS_IN_PARALLEL, "true");
    config.put(KEY_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
    config.put(VALUE_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());

    return config;
  }

  protected final void waitForConnectorRunning(String connectorName) {
    try {
      connectCluster
          .assertions()
          .assertConnectorAndAtLeastNumTasksAreRunning(
              connectorName, 1, "The connector did not start.");
    } catch (InterruptedException e) {
      throw new IllegalStateException("The connector is not running");
    }
  }

  protected final void waitForConnectorStopped(String connectorName) {
    try {
      connectCluster
          .assertions()
          .assertConnectorDoesNotExist(connectorName, "Failed to stop the connector");
    } catch (InterruptedException e) {
      throw new IllegalStateException("The connector is not running");
    }
  }
}
