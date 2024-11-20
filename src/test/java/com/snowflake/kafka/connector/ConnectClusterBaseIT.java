package com.snowflake.kafka.connector;

import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.*;
import static org.apache.kafka.connect.runtime.ConnectorConfig.CONNECTOR_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.TASKS_MAX_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.sink.SinkConnector.TOPICS_CONFIG;

import com.snowflake.kafka.connector.internal.TestUtils;
import com.snowflake.kafka.connector.internal.streaming.FakeStreamingClientHandler;
import com.snowflake.kafka.connector.internal.streaming.IngestionMethodConfig;
import com.snowflake.kafka.connector.internal.streaming.StreamingClientProvider;
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
public abstract class ConnectClusterBaseIT {

  protected EmbeddedConnectCluster connectCluster;

  protected FakeStreamingClientHandler fakeStreamingClientHandler;

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
    StreamingClientProvider.reset();
    fakeStreamingClientHandler = new FakeStreamingClientHandler();
    StreamingClientProvider.overrideStreamingClientHandler(fakeStreamingClientHandler);
  }

  @AfterEach
  public void afterEach() {
    StreamingClientProvider.reset();
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

    config.put(CONNECTOR_CLASS_CONFIG, SnowflakeSinkConnector.class.getName());
    config.put(NAME, connectorName);
    config.put(TOPICS_CONFIG, topicName);
    config.put(INGESTION_METHOD_OPT, IngestionMethodConfig.SNOWPIPE_STREAMING.toString());
    config.put(Utils.SF_ROLE, "testrole_kafka");
    config.put(SNOWPIPE_STREAMING_MAX_CLIENT_LAG, "1");
    config.put(SNOWPIPE_STREAMING_ENABLE_SINGLE_BUFFER, "true");
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
