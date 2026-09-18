package com.snowflake.kafka.connector.internal.streaming.v2.client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.snowflake.ingest.streaming.SFException;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import com.snowflake.kafka.connector.config.SinkTaskConfig;
import com.snowflake.kafka.connector.config.SinkTaskConfigTestBuilder;
import com.snowflake.kafka.connector.internal.SnowflakeKafkaConnectorException;
import com.snowflake.kafka.connector.internal.metrics.TaskMetrics;
import com.snowflake.kafka.connector.internal.streaming.StreamingClientProperties;
import com.snowflake.kafka.connector.internal.streaming.v2.service.ThreadPools;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.mockito.Mockito;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class StreamingClientPoolsTest {

  private static final String TASK_ID = "test-task";

  private SinkTaskConfig sinkTaskConfig;
  private StreamingClientProperties streamingClientProperties;
  private String connectorName;

  @BeforeEach
  void setUp() {
    connectorName = "test-connector-pools-" + UUID.randomUUID().toString().substring(0, 8);
    sinkTaskConfig =
        SinkTaskConfigTestBuilder.builder().connectorName(connectorName).taskId(TASK_ID).build();
    streamingClientProperties = StreamingClientProperties.from(sinkTaskConfig);
    ThreadPools.registerTask(connectorName, sinkTaskConfig);
  }

  @AfterEach
  void tearDown() {
    StreamingClientFactory.resetStreamingClientSupplier();
    StreamingClientPools.closeTaskClients(connectorName, TASK_ID);
    ThreadPools.closeForTask(connectorName);
  }

  private SnowflakeStreamingIngestClient getClient(String pipeName) {
    return StreamingClientPools.getClient(
        connectorName,
        TASK_ID,
        pipeName,
        sinkTaskConfig,
        streamingClientProperties,
        TaskMetrics.noop());
  }

  @Test
  void getClient_unwraps_CompletionException_and_throws_original_RuntimeException() {
    SnowflakeKafkaConnectorException originalException =
        new SnowflakeKafkaConnectorException("creation failed", "TEST_ERROR");
    StreamingClientFactory.setStreamingClientSupplier(
        (clientName, dbName, schemaName, pipeName, props) -> {
          throw originalException;
        });

    assertThatThrownBy(() -> getClient("pipe-A")).isSameAs(originalException);
  }

  @Test
  void getClient_retries_on_bodyless_404() {
    SnowflakeStreamingIngestClient mockClient = Mockito.mock(SnowflakeStreamingIngestClient.class);
    AtomicInteger callCount = new AtomicInteger();

    StreamingClientFactory.setStreamingClientSupplier(
        (clientName, dbName, schemaName, pipeName, props) -> {
          if (callCount.incrementAndGet() == 1) {
            throw new SFException("SfApiUserError", "", 404, "");
          }
          return mockClient;
        });

    SnowflakeStreamingIngestClient result = getClient("pipe-A");

    assertThat(result).isSameAs(mockClient);
    assertThat(callCount.get()).isEqualTo(2);
  }

  @Test
  void getClient_does_not_retry_404_with_error_message() {
    AtomicInteger callCount = new AtomicInteger();
    SFException notFound = new SFException("SfApiUserError", "pipe not found", 404, "Not Found");

    StreamingClientFactory.setStreamingClientSupplier(
        (clientName, dbName, schemaName, pipeName, props) -> {
          callCount.incrementAndGet();
          throw notFound;
        });

    assertThatThrownBy(() -> getClient("pipe-A")).isSameAs(notFound);
    assertThat(callCount.get()).isEqualTo(1);
  }
}
