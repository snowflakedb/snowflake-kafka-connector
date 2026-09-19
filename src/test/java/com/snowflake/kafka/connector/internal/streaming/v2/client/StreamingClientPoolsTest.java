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
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

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
    StreamingClientPools.resetRetryDurations();
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

  private SnowflakeStreamingIngestClient recreateClient(
      String pipeName, SnowflakeStreamingIngestClient invalidClient) {
    return StreamingClientPools.recreateClient(
        connectorName,
        TASK_ID,
        pipeName,
        invalidClient,
        sinkTaskConfig,
        streamingClientProperties,
        TaskMetrics.noop());
  }

  /** One burst attempt, then a short outer delay so hourly-retry tests finish quickly. */
  private static void enableHourlyRetryForTest() {
    StreamingClientPools.setRetryDurationsForTest(
        Duration.ofMinutes(6), Duration.ofMinutes(30), Duration.ofMillis(50));
    StreamingClientPools.setBurstMaxAttemptsForTest(1);
  }

  private static SFException bodyless404() {
    return new SFException("SfApiUserError", "", 404, "");
  }

  private static SFException notFound404() {
    return new SFException("SfApiUserError", "pipe not found", 404, "Not Found");
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
            throw bodyless404();
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
    SFException notFound = notFound404();

    StreamingClientFactory.setStreamingClientSupplier(
        (clientName, dbName, schemaName, pipeName, props) -> {
          callCount.incrementAndGet();
          throw notFound;
        });

    assertThatThrownBy(() -> getClient("pipe-A")).isSameAs(notFound);
    assertThat(callCount.get()).isEqualTo(1);
  }

  @Test
  void recreateClient_retries_on_bodyless_404() {
    SnowflakeStreamingIngestClient oldClient = Mockito.mock(SnowflakeStreamingIngestClient.class);
    SnowflakeStreamingIngestClient newClient = Mockito.mock(SnowflakeStreamingIngestClient.class);
    AtomicInteger callCount = new AtomicInteger();

    StreamingClientFactory.setStreamingClientSupplier(
        (clientName, dbName, schemaName, pipeName, props) -> {
          int count = callCount.incrementAndGet();
          if (count == 1) {
            return oldClient;
          }
          if (count == 2) {
            throw bodyless404();
          }
          return newClient;
        });

    getClient("pipe-A");

    SnowflakeStreamingIngestClient result = recreateClient("pipe-A", oldClient);

    assertThat(result).isSameAs(newClient);
    assertThat(callCount.get()).isEqualTo(3);
  }

  @Test
  void recreateClient_does_not_retry_404_with_error_message() {
    SnowflakeStreamingIngestClient oldClient = Mockito.mock(SnowflakeStreamingIngestClient.class);
    AtomicInteger callCount = new AtomicInteger();
    SFException notFound = notFound404();

    StreamingClientFactory.setStreamingClientSupplier(
        (clientName, dbName, schemaName, pipeName, props) -> {
          int count = callCount.incrementAndGet();
          if (count == 1) {
            return oldClient;
          }
          throw notFound;
        });

    getClient("pipe-A");

    assertThatThrownBy(() -> recreateClient("pipe-A", oldClient)).isSameAs(notFound);
    assertThat(callCount.get()).isEqualTo(2);
  }

  @Test
  void getClient_retries_hourly_after_create_burst_budget() {
    SnowflakeStreamingIngestClient mockClient = Mockito.mock(SnowflakeStreamingIngestClient.class);
    AtomicInteger callCount = new AtomicInteger();
    enableHourlyRetryForTest();

    StreamingClientFactory.setStreamingClientSupplier(
        (clientName, dbName, schemaName, pipeName, props) -> {
          if (callCount.incrementAndGet() == 1) {
            throw bodyless404();
          }
          return mockClient;
        });

    SnowflakeStreamingIngestClient result = getClient("pipe-A");

    assertThat(result).isSameAs(mockClient);
    assertThat(callCount.get()).isEqualTo(2);
  }

  @Test
  void recreateClient_retries_hourly_after_recreate_burst_budget() {
    SnowflakeStreamingIngestClient oldClient = Mockito.mock(SnowflakeStreamingIngestClient.class);
    SnowflakeStreamingIngestClient newClient = Mockito.mock(SnowflakeStreamingIngestClient.class);
    AtomicInteger callCount = new AtomicInteger();
    enableHourlyRetryForTest();

    StreamingClientFactory.setStreamingClientSupplier(
        (clientName, dbName, schemaName, pipeName, props) -> {
          int count = callCount.incrementAndGet();
          if (count == 1) {
            return oldClient;
          }
          if (count == 2) {
            throw bodyless404();
          }
          return newClient;
        });

    getClient("pipe-A");

    SnowflakeStreamingIngestClient result = recreateClient("pipe-A", oldClient);

    assertThat(result).isSameAs(newClient);
    assertThat(callCount.get()).isEqualTo(3);
  }
}
