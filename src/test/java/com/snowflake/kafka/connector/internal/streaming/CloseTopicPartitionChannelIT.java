package com.snowflake.kafka.connector.internal.streaming;

import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.SNOWPIPE_STREAMING_CLOSE_CHANNELS_IN_PARALLEL;
import static org.awaitility.Awaitility.await;

import com.snowflake.kafka.connector.ConnectClusterBaseIT;
import com.snowflake.kafka.connector.internal.TestUtils;
import com.snowflake.kafka.connector.internal.streaming.v2.StreamingIngestClientProvider;

import java.time.Duration;
import java.util.Map;
import java.util.stream.IntStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class CloseTopicPartitionChannelIT extends ConnectClusterBaseIT {

  private static final int PARTITIONS_NUMBER = 16;

  private String topicName;
  private String connectorName;
  private FakeIngestClientSupplier fakeClientSupplier = new FakeIngestClientSupplier();

  @BeforeEach
  void setUp() {
    topicName = TestUtils.randomTableName();
    connectorName = topicName + "_connector";
    connectCluster.kafka().createTopic(topicName, PARTITIONS_NUMBER);
    StreamingIngestClientProvider.setIngestClientSupplier(fakeClientSupplier);
    generateKafkaMessages();
  }

    @AfterEach
    void tearDown() {
        connectCluster.kafka().deleteTopic(topicName);
        StreamingIngestClientProvider.resetIngestClientSupplier();
    }


    private void generateKafkaMessages() {
    IntStream.range(0, PARTITIONS_NUMBER)
        .forEach(
            partition ->
                connectCluster
                    .kafka()
                    .produce(topicName, partition, "key-" + partition, "message-" + partition));
  }


  @ParameterizedTest(name = "closeInParallel: {0}")
  @ValueSource(booleans = { true, false})
  void closeChannels(boolean closeInParallel) throws InterruptedException {
    // given
    connectCluster.configureConnector(connectorName, connectorProperties(closeInParallel));
    waitForConnectorRunning(connectorName);

    await("channelsCreated")
        .atMost(Duration.ofSeconds(30))
        .ignoreExceptions()
        .until( () -> getFakeIngestClient(connectorName).getOpenedChannels().size() == PARTITIONS_NUMBER);

    // when
    connectCluster.deleteConnector(connectorName);
    waitForConnectorStopped(connectorName);

    // then
    await("channelsClosed").atMost(Duration.ofSeconds(30))
        .until(() -> getFakeIngestClient(connectorName).countClosedChannels() == PARTITIONS_NUMBER);
  }

  private Map<String, String> connectorProperties(boolean closeInParallel) {
    Map<String, String> config = defaultProperties(topicName, connectorName);
    config.put(SNOWPIPE_STREAMING_CLOSE_CHANNELS_IN_PARALLEL, Boolean.toString(closeInParallel));
    return config;
  }

  private FakeSnowflakeStreamingIngestClient getFakeIngestClient(String connectorName) {
      return fakeClientSupplier.getFakeIngestClients().stream()
          .filter((client) -> client.getConnectorName().equals(connectorName))
          .findFirst()
          .orElseThrow();
  }
}
