package com.snowflake.kafka.connector.internal.streaming;

import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.SNOWPIPE_STREAMING_CLOSE_CHANNELS_IN_PARALLEL;
import static org.awaitility.Awaitility.await;

import com.snowflake.kafka.connector.ConnectClusterBaseIT;
import com.snowflake.kafka.connector.internal.TestUtils;
import java.time.Duration;
import java.util.Map;
import java.util.stream.IntStream;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class CloseTopicPartitionChannelIT extends ConnectClusterBaseIT {

  private static final int PARTITIONS_NUMBER = 16;

  private String topicName;
  private String connectorName;

  @BeforeEach
  void setUp() {
    topicName = TestUtils.randomTableName();
    connectorName = topicName + "_connector";
    connectCluster.kafka().createTopic(topicName, PARTITIONS_NUMBER);

    generateKafkaMessages();
  }

  private void generateKafkaMessages() {
    IntStream.range(0, PARTITIONS_NUMBER)
        .forEach(
            partition ->
                connectCluster
                    .kafka()
                    .produce(topicName, partition, "key-" + partition, "message-" + partition));
  }

  @AfterEach
  void tearDown() {
    connectCluster.kafka().deleteTopic(topicName);
  }

  @ParameterizedTest(name = "closeInParallel: {0}")
  @ValueSource(booleans = {true, false})
  void closeChannels(boolean closeInParallel) {
    // given
    connectCluster.configureConnector(connectorName, connectorProperties(closeInParallel));
    waitForConnectorRunning(connectorName);

    await("channelsCreated").atMost(Duration.ofSeconds(30)).until(this::channelsCreated);

    // when
    connectCluster.deleteConnector(connectorName);
    waitForConnectorStopped(connectorName);

    // then
    // Cluster considers the connector stopped even when it's still in the teardown phase.
    // Therefore, some of the channels might still be opened instantly.
    await("channelsClosed").atMost(Duration.ofSeconds(30)).until(this::channelsClosed);
  }

  private boolean channelsCreated() {
    long channelsCount =
        fakeStreamingClientHandler.countChannels(SnowflakeStreamingIngestChannel::isValid);
    return PARTITIONS_NUMBER == channelsCount;
  }

  private boolean channelsClosed() {
    long channelsCount =
        fakeStreamingClientHandler.countChannels(SnowflakeStreamingIngestChannel::isClosed);
    return PARTITIONS_NUMBER == channelsCount;
  }

  private Map<String, String> connectorProperties(boolean closeInParallel) {
    Map<String, String> config = defaultProperties(topicName, connectorName);
    config.put(SNOWPIPE_STREAMING_CLOSE_CHANNELS_IN_PARALLEL, Boolean.toString(closeInParallel));
    return config;
  }
}
