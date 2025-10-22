package com.snowflake.kafka.connector.internal.streaming;

import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.SNOWPIPE_STREAMING_CLOSE_CHANNELS_IN_PARALLEL;
import static org.awaitility.Awaitility.await;

import com.snowflake.kafka.connector.ConnectClusterBaseIT;
import com.snowflake.kafka.connector.internal.TestUtils;
import java.util.Map;
import java.util.stream.IntStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Integration test for closing topic partition channels in parallel vs sequentially.
 * 
 * <p>CURRENTLY DISABLED: This test requires SSv2 fake/mock infrastructure to verify
 * channel closing behavior without making actual Snowflake calls.
 * TODO: Re-enable after implementing SSv2 fake infrastructure.
 */
@Disabled("Requires SSv2 fake/mock infrastructure - to be implemented")
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

    // TODO: Wait for channels to be created using SSv2 fake infrastructure
    // await("channelsCreated").atMost(Duration.ofSeconds(30)).until(this::channelsCreated);

    // when
    connectCluster.deleteConnector(connectorName);
    waitForConnectorStopped(connectorName);

    // then
    // TODO: Verify channels are closed using SSv2 fake infrastructure
    // await("channelsClosed").atMost(Duration.ofSeconds(30)).until(this::channelsClosed);
  }

  // TODO: Implement using SSv2 fake infrastructure
  // private boolean channelsCreated() {
  //   long channelsCount =
  //       fakeStreamingClientV2Handler.countChannels(channel -> channel.isValid());
  //   return PARTITIONS_NUMBER == channelsCount;
  // }

  // TODO: Implement using SSv2 fake infrastructure
  // private boolean channelsClosed() {
  //   long channelsCount =
  //       fakeStreamingClientV2Handler.countChannels(channel -> channel.isClosed());
  //   return PARTITIONS_NUMBER == channelsCount;
  // }

  private Map<String, String> connectorProperties(boolean closeInParallel) {
    Map<String, String> config = defaultProperties(topicName, connectorName);
    config.put(SNOWPIPE_STREAMING_CLOSE_CHANNELS_IN_PARALLEL, Boolean.toString(closeInParallel));
    return config;
  }
}
