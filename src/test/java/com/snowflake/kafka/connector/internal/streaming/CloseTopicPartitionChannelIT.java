package com.snowflake.kafka.connector.internal.streaming;

import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.snowflake.kafka.connector.ConnectClusterBaseIT;
import com.snowflake.kafka.connector.internal.TestUtils;
import com.snowflake.kafka.connector.internal.streaming.v2.StreamingClientManager;
import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CloseTopicPartitionChannelIT extends ConnectClusterBaseIT {

  private static final int PARTITIONS_NUMBER = 16;

  private String topicName;
  private String connectorName;
  private ObjectMapper mapper = new ObjectMapper();

  @BeforeEach
  void setUp() throws JsonProcessingException {
    topicName = TestUtils.randomTableName();
    connectorName = topicName + "_connector";
    connectCluster.kafka().createTopic(topicName, PARTITIONS_NUMBER);
    TestUtils.getConnectionService().createTableWithMetadataColumn(topicName);
    // JVM scoped Ingest client mock
    StreamingClientManager.setIngestClientSupplier(fakeClientSupplier);
    generateKafkaMessages();
  }

  @AfterEach
  void tearDown() {
    connectCluster.kafka().deleteTopic(topicName);
    StreamingClientManager.resetIngestClientSupplier();
    TestUtils.dropTable(topicName);
  }

  private void generateKafkaMessages() throws JsonProcessingException {
    final Map<String, String> payload = Map.of("key1", "value1", "key2", "value2");

    int bound = PARTITIONS_NUMBER;
    for (int partition = 0; partition < bound; partition++) {
      connectCluster
          .kafka()
          .produce(topicName, partition, "key-" + partition, mapper.writeValueAsString(payload));
    }
  }

  @Test
  void closeChannels() {
    // given
    connectCluster.configureConnector(connectorName, defaultProperties(topicName, connectorName));
    waitForConnectorRunning(connectorName);
    waitForOpenedFakeIngestClient(connectorName);
    await("Channels created")
        .atMost(Duration.ofSeconds(30))
        .ignoreExceptions()
        .until(
            () ->
                getOpenedFakeIngestClient(connectorName).getOpenedChannels().size()
                    == PARTITIONS_NUMBER);

    // when
    connectCluster.deleteConnector(connectorName);
    waitForConnectorStopped(connectorName);

    // then
    await("Channels closed")
        .atMost(Duration.ofSeconds(30))
        .until(
            () ->
                getOpenedFakeIngestClient(connectorName).countClosedChannels()
                    == PARTITIONS_NUMBER);
  }
}
