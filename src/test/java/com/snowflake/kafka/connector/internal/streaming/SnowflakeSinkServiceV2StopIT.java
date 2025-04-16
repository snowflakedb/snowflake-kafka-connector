package com.snowflake.kafka.connector.internal.streaming;

import com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig;
import com.snowflake.kafka.connector.dlq.InMemoryKafkaRecordErrorReporter;
import com.snowflake.kafka.connector.internal.SnowflakeConnectionService;
import com.snowflake.kafka.connector.internal.SnowflakeSinkServiceFactory;
import com.snowflake.kafka.connector.internal.TestUtils;
import java.util.Collections;
import java.util.Map;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class SnowflakeSinkServiceV2StopIT {

  private String topicAndTableName;

  @BeforeEach
  public void beforeEach() {
    topicAndTableName = TestUtils.randomTableName();
  }

  @AfterEach
  public void afterEach() {
    TestUtils.dropTable(topicAndTableName);
  }

  @ParameterizedTest(name = "optimizationEnabled: {0}")
  @ValueSource(booleans = {true, false})
  public void stop_forSingleService_closesClientDependingOnOptimization(
      boolean optimizationEnabled) {
    final boolean clientClosed = !optimizationEnabled;
    // given
    SnowflakeConnectionService conn = TestUtils.getConnectionServiceForStreaming();
    Map<String, String> config = getConfig(optimizationEnabled);
    SnowflakeSinkConnectorConfig.setDefaultValues(config);

    conn.createTable(topicAndTableName);
    int partition = 0;
    TopicPartition topicPartition = new TopicPartition(topicAndTableName, partition);

    // opens a channel for partition 0, table and topic
    SnowflakeSinkServiceV2 service =
        (SnowflakeSinkServiceV2)
            SnowflakeSinkServiceFactory.builder(
                    conn, IngestionMethodConfig.SNOWPIPE_STREAMING, config)
                .setErrorReporter(new InMemoryKafkaRecordErrorReporter())
                .setSinkTaskContext(
                    new InMemorySinkTaskContext(Collections.singleton(topicPartition)))
                .addTask(topicAndTableName, topicPartition)
                .build();
    SnowflakeStreamingIngestClient client = service.getStreamingIngestClient();

    // when
    service.stop();

    // then
    Assertions.assertEquals(clientClosed, client.isClosed());
  }

  private Map<String, String> getConfig(boolean optimizationEnabled) {
    Map<String, String> config = TestUtils.getConfForStreaming();
    config.put(
        SnowflakeSinkConnectorConfig.ENABLE_STREAMING_CLIENT_OPTIMIZATION_CONFIG,
        String.valueOf(optimizationEnabled));
    return config;
  }
}
