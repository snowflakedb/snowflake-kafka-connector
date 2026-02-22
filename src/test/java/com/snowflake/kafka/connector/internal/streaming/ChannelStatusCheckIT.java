package com.snowflake.kafka.connector.internal.streaming;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams;
import com.snowflake.kafka.connector.SnowflakeStreamingSinkConnector;
import com.snowflake.kafka.connector.internal.TestUtils;
import com.snowflake.kafka.connector.internal.streaming.v2.client.StreamingClientFactory;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo;
import org.apache.kafka.connect.sink.SinkConnector;
import org.apache.kafka.connect.storage.StringConverter;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

/**
 * Integration tests for channel status error handling using an embedded Kafka Connect cluster with
 * fake streaming ingest clients.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ChannelStatusCheckIT {

  private EmbeddedConnectCluster connectCluster;
  private final FakeIngestClientSupplier fakeClientSupplier = new FakeIngestClientSupplier();

  @BeforeAll
  void beforeAll() {
    Map<String, String> workerConfig = new HashMap<>();
    workerConfig.put("plugin.discovery", "hybrid_warn");
    // Set a short offset flush interval for faster preCommit calls
    workerConfig.put("offset.flush.interval.ms", "1000");
    connectCluster =
        new EmbeddedConnectCluster.Builder()
            .name("channel-status-check-cluster")
            .numWorkers(5)
            .workerProps(workerConfig)
            .build();
    connectCluster.start();
  }

  @AfterAll
  void afterAll() {
    if (connectCluster != null) {
      connectCluster.stop();
      connectCluster = null;
    }
  }

  private static final int PARTITIONS_NUMBER = 10;

  private String topicName;
  private String connectorName;
  private final ObjectMapper mapper = new ObjectMapper();

  @BeforeEach
  void setUp() {
    topicName = TestUtils.randomTableName();
    connectorName = topicName + "_connector";
    connectCluster.kafka().createTopic(topicName, PARTITIONS_NUMBER);
    TestUtils.getConnectionService().createTableWithMetadataColumn(topicName);
    StreamingClientFactory.setStreamingClientSupplier(fakeClientSupplier);
  }

  @AfterEach
  void tearDown() {
    connectCluster.deleteConnector(connectorName);
    waitForConnectorStopped(connectorName);
    connectCluster.kafka().deleteTopic(topicName);
    StreamingClientFactory.resetStreamingClientSupplier();
    TestUtils.dropTable(topicName);
  }

  @Test
  void shouldContinueWorkingWhenNoChannelErrors() throws JsonProcessingException {
    // Given: connector with default config (errors.tolerance=none)
    Map<String, String> config = defaultProperties(topicName, connectorName);
    connectCluster.configureConnector(connectorName, config);
    waitForConnectorRunning(connectorName);
    waitForOpenedFakeIngestClient(connectorName);

    // When: produce messages
    produceMessages(3000);

    // Then: connector should remain running (no errors to cause failure)
    await("Messages processed")
        .atMost(Duration.ofSeconds(30))
        .until(() -> waitForConnectorToOpenChannels(connectorName).getAppendedRowCount() >= 3);

    ConnectorStateInfo connectorState = connectCluster.connectorStatus(connectorName);
    assertTrue(
        connectorState.tasks().stream().allMatch(task -> "RUNNING".equals(task.state())),
        "All tasks should be running when there are no channel errors");
  }

  @Test
  void shouldFailConnectorWhenChannelHasErrorsAndToleranceIsNone() throws JsonProcessingException {
    // Given: connector with errors.tolerance=none (default)
    Map<String, String> config = defaultProperties(topicName, connectorName);
    connectCluster.configureConnector(connectorName, config);
    waitForConnectorRunning(connectorName);

    FakeSnowflakeStreamingIngestClient fakeClient = waitForConnectorToOpenChannels(connectorName);

    // Produce initial message to ensure channel is set up
    produceMessages(3000);
    await("Initial message processed")
        .atMost(Duration.ofSeconds(30))
        .until(() -> fakeClient.getAppendedRowCount() >= 1);

    // When: inject errors on all channels
    for (FakeSnowflakeStreamingIngestChannel channel : fakeClient.getOpenedChannels()) {
      channel.updateErrors(5, "Test error message", "95");
    }

    // Then: connector task should fail due to channel errors
    await("Connector task failed")
        .atMost(Duration.ofMinutes(2))
        .pollInterval(Duration.ofSeconds(4))
        .until(
            () -> {
              ConnectorStateInfo state = connectCluster.connectorStatus(connectorName);
              return state.tasks().stream().anyMatch(task -> "FAILED".equals(task.state()));
            });
  }

  @Test
  void shouldContinueWorkingWhenChannelHasErrorsAndToleranceIsAll() throws JsonProcessingException {
    // Given: connector with errors.tolerance=all
    Map<String, String> config = defaultProperties(topicName, connectorName);
    config.put(KafkaConnectorConfigParams.ERRORS_TOLERANCE_CONFIG, "all");
    connectCluster.configureConnector(connectorName, config);
    waitForConnectorRunning(connectorName);

    FakeSnowflakeStreamingIngestClient fakeClient = waitForConnectorToOpenChannels(connectorName);

    // Produce initial message
    produceMessages(1);
    await("Initial message processed")
        .atMost(Duration.ofSeconds(30))
        .until(() -> fakeClient.getAppendedRowCount() >= 1);

    // When: inject errors on all channels
    for (FakeSnowflakeStreamingIngestChannel channel : fakeClient.getOpenedChannels()) {
      channel.updateErrors(5, "Test error message", "95");
    }

    // Produce more messages
    produceMessages(2);

    // Then: connector should continue running (errors are tolerated)
    await("Messages processed despite errors")
        .atMost(Duration.ofSeconds(30))
        .until(() -> fakeClient.getAppendedRowCount() >= 3);

    ConnectorStateInfo connectorState = connectCluster.connectorStatus(connectorName);
    assertTrue(
        connectorState.tasks().stream().allMatch(task -> "RUNNING".equals(task.state())),
        "All tasks should remain running when errors.tolerance=all");
  }

  @Test
  void shouldContinueWorkingWithPreExistingErrorsAndToleranceIsNone()
      throws JsonProcessingException {
    // Given: Pre-existing errors are set BEFORE the connector starts (simulating channel reopen
    // scenario)
    // This simulates the case where a channel has cumulative errors from a previous connector run
    fakeClientSupplier.setPreExistingErrorCount(5);

    Map<String, String> config = defaultProperties(topicName, connectorName);
    config.put(KafkaConnectorConfigParams.ERRORS_TOLERANCE_CONFIG, "none");
    connectCluster.configureConnector(connectorName, config);
    waitForConnectorRunning(connectorName);

    FakeSnowflakeStreamingIngestClient fakeClient = waitForConnectorToOpenChannels(connectorName);

    // Produce messages
    produceMessages(5);

    // Then: connector should remain running because pre-existing errors don't count as new errors
    await("Messages processed despite pre-existing errors")
        .atMost(Duration.ofSeconds(30))
        .until(() -> fakeClient.getAppendedRowCount() >= 5);

    ConnectorStateInfo connectorState = connectCluster.connectorStatus(connectorName);
    assertTrue(
        connectorState.tasks().stream().allMatch(task -> "RUNNING".equals(task.state())),
        "All tasks should be running when there are only pre-existing errors");
  }

  @Test
  void shouldFailWhenNewErrorsOccurAfterStartupWithPreExistingErrors()
      throws JsonProcessingException {
    // Given: Pre-existing errors are set BEFORE the connector starts
    fakeClientSupplier.setPreExistingErrorCount(5);

    Map<String, String> config = defaultProperties(topicName, connectorName);
    connectCluster.configureConnector(connectorName, config);
    waitForConnectorRunning(connectorName);

    FakeSnowflakeStreamingIngestClient fakeClient = waitForConnectorToOpenChannels(connectorName);

    // Produce initial message
    produceMessages(1);
    await("Initial message processed")
        .atMost(Duration.ofSeconds(30))
        .until(() -> fakeClient.getAppendedRowCount() >= 1);

    // When: NEW errors occur (error count increases from 5 to 10)
    for (FakeSnowflakeStreamingIngestChannel channel : fakeClient.getOpenedChannels()) {
      channel.updateErrors(10, "Test error message", "95");
    }

    // Then: connector task should fail due to NEW channel errors
    await("Connector task failed due to new errors")
        .atMost(Duration.ofMinutes(2))
        .pollInterval(Duration.ofSeconds(4))
        .until(
            () -> {
              ConnectorStateInfo state = connectCluster.connectorStatus(connectorName);
              return state.tasks().stream().anyMatch(task -> "FAILED".equals(task.state()));
            });
  }

  private void produceMessages(int count) throws JsonProcessingException {
    Map<String, String> payload = Map.of("key1", "value1", "key2", "value2");
    for (int i = 0; i < count; i++) {
      connectCluster
          .kafka()
          .produce(
              topicName, i % PARTITIONS_NUMBER, "key-" + i, mapper.writeValueAsString(payload));
    }
  }

  // Helper methods

  private FakeSnowflakeStreamingIngestClient waitForConnectorToOpenChannels(String connectorName) {
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

  private void waitForOpenedFakeIngestClient(String connectorName) {
    waitForConnectorToOpenChannels(connectorName);
  }

  private FakeSnowflakeStreamingIngestClient getFakeSnowflakeStreamingIngestClient(
      String connectorName) {
    return fakeClientSupplier.getFakeIngestClients().stream()
        .filter((client) -> client.getConnectorName().equals(connectorName))
        .findFirst()
        .orElseThrow();
  }

  private Map<String, String> defaultProperties(String topicName, String connectorName) {
    Map<String, String> config = TestUtils.transformProfileFileToConnectorConfiguration(false);
    config.put(SinkConnector.TOPICS_CONFIG, topicName);
    config.put(
        ConnectorConfig.CONNECTOR_CLASS_CONFIG, SnowflakeStreamingSinkConnector.class.getName());
    config.put(ConnectorConfig.TASKS_MAX_CONFIG, "1");
    config.put(ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
    config.put(ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG, JsonConverter.class.getName());
    config.put(KafkaConnectorConfigParams.NAME, connectorName);
    config.put(KafkaConnectorConfigParams.SNOWFLAKE_STREAMING_MAX_CLIENT_LAG, "1");
    config.put(KafkaConnectorConfigParams.VALUE_CONVERTER_SCHEMAS_ENABLE, "false");
    return config;
  }

  private void waitForConnectorRunning(String connectorName) {
    try {
      connectCluster
          .assertions()
          .assertConnectorAndAtLeastNumTasksAreRunning(
              connectorName, 1, "The connector did not start.");
    } catch (InterruptedException e) {
      throw new IllegalStateException("The connector is not running");
    }
  }

  private void waitForConnectorStopped(String connectorName) {
    try {
      connectCluster
          .assertions()
          .assertConnectorDoesNotExist(connectorName, "Failed to stop the connector");
    } catch (InterruptedException e) {
      throw new IllegalStateException("Interrupted while waiting for connector to stop");
    }
  }
}
