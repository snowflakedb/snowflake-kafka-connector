package com.snowflake.kafka.connector.internal.streaming.v2.client;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
    streamingClientProperties = new StreamingClientProperties(connectorConfig);
    connectorName = "test-connector-pools-" + UUID.randomUUID().toString().substring(0, 8);
    ThreadPools.registerTask(connectorName, "test-task");
  }

  @AfterEach
  void tearDown() {
    StreamingClientFactory.resetStreamingClientSupplier();
    StreamingClientPools.closeTaskClients(connectorName, "test-task");
    ThreadPools.closeForTask(connectorName, "test-task");
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
                    connectorConfig,
                    streamingClientProperties,
                    TaskMetrics.noop()))
        .isSameAs(originalException);
  }
}
