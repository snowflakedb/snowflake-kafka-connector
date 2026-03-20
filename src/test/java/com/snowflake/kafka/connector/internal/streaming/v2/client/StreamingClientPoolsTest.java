package com.snowflake.kafka.connector.internal.streaming.v2.client;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.snowflake.kafka.connector.config.SinkTaskConfig;
import com.snowflake.kafka.connector.internal.SnowflakeKafkaConnectorException;
import com.snowflake.kafka.connector.internal.TestUtils;
import com.snowflake.kafka.connector.internal.metrics.TaskMetrics;
import com.snowflake.kafka.connector.internal.streaming.StreamingClientProperties;
import com.snowflake.kafka.connector.internal.streaming.v2.service.ThreadPools;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class StreamingClientPoolsTest {

  private Map<String, String> connectorConfig;
  private StreamingClientProperties streamingClientProperties;
  private String connectorName;

  @BeforeEach
  void setUp() {
    connectorConfig = TestUtils.getConnectorConfigurationForStreaming(false);
    streamingClientProperties =
        StreamingClientProperties.from(SinkTaskConfig.from(connectorConfig));
    connectorName = "test-connector-pools-" + UUID.randomUUID().toString().substring(0, 8);
    ThreadPools.registerTask(connectorName, SinkTaskConfig.from(connectorConfig));
  }

  @AfterEach
  void tearDown() {
    StreamingClientFactory.resetStreamingClientSupplier();
    StreamingClientPools.closeTaskClients(connectorName, "test-task");
    ThreadPools.closeForTask(connectorName);
  }

  @Test
  void getClient_unwraps_CompletionException_and_throws_original_RuntimeException() {
    SnowflakeKafkaConnectorException originalException =
        new SnowflakeKafkaConnectorException("creation failed", "TEST_ERROR");
    StreamingClientFactory.setStreamingClientSupplier(
        (clientName, dbName, schemaName, pipeName, config, props) -> {
          throw originalException;
        });

    assertThatThrownBy(
            () ->
                StreamingClientPools.getClient(
                    connectorName,
                    "test-task",
                    "pipe-A",
                    SinkTaskConfig.from(connectorConfig),
                    streamingClientProperties,
                    TaskMetrics.noop()))
        .isSameAs(originalException);
  }
}
