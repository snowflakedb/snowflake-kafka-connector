package com.snowflake.kafka.connector;

import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.*;
import static com.snowflake.kafka.connector.Utils.SF_ROLE;
import static org.apache.kafka.connect.runtime.ConnectorConfig.CONNECTOR_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.TASKS_MAX_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.sink.SinkConnector.TOPICS_CONFIG;
import static org.awaitility.Awaitility.await;

import com.snowflake.kafka.connector.internal.TestUtils;
import com.snowflake.kafka.connector.internal.streaming.FakeIngestClientSupplier;
import com.snowflake.kafka.connector.internal.streaming.FakeSnowflakeStreamingIngestClient;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.connect.storage.StringConverter;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;

/** Base class for integration tests using an embedded Kafka Connect cluster. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class ConnectClusterBaseIT {

  protected EmbeddedConnectCluster connectCluster;
  protected final FakeIngestClientSupplier fakeClientSupplier = new FakeIngestClientSupplier();

  static final Integer TASK_NUMBER = 1;

  @BeforeAll
  public void beforeAll() {
    Map<String, String> workerConfig = new HashMap<>();
    workerConfig.put("plugin.discovery", "hybrid_warn");
    connectCluster =
        new EmbeddedConnectCluster.Builder()
            .name("kafka-push-connector-connect-cluster")
            .numWorkers(1)
            .workerProps(workerConfig)
            .build();
    connectCluster.start();
  }

  @AfterAll
  public void afterAll() {
    if (connectCluster != null) {
      connectCluster.stop();
      connectCluster = null;
    }
  }

  protected FakeSnowflakeStreamingIngestClient getOpenedFakeIngestClient(String connectorName) {
    await("channelsCreated")
        .atMost(Duration.ofSeconds(30))
        .ignoreExceptions()
        .until(
            () ->
                !getFakeSnowflakeStreamingIngestClient(connectorName)
                    .getOpenedChannels()
                    .isEmpty());

    return getFakeSnowflakeStreamingIngestClient(connectorName);
  }

  protected void waitForOpenedFakeIngestClient(String connectorName) {
    getOpenedFakeIngestClient(connectorName);
  }

  protected final Map<String, String> defaultProperties(String topicName, String connectorName) {
    Map<String, String> config = TestUtils.getConf();

    config.put(CONNECTOR_CLASS_CONFIG, SnowflakeStreamingSinkConnector.class.getName());
    config.put(NAME, connectorName);
    config.put(TOPICS_CONFIG, topicName);
    config.put(SF_ROLE, config.get(Utils.SF_ROLE));
    config.put(SNOWPIPE_STREAMING_MAX_CLIENT_LAG, "1");
    config.put(TASKS_MAX_CONFIG, TASK_NUMBER.toString());
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

  private FakeSnowflakeStreamingIngestClient getFakeSnowflakeStreamingIngestClient(
      String connectorName) {
    return fakeClientSupplier.getFakeIngestClients().stream()
        .filter((client) -> client.getConnectorName().equals(connectorName))
        .findFirst()
        .orElseThrow();
  }
}
