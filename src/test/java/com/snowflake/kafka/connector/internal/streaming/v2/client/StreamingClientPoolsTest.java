package com.snowflake.kafka.connector.internal.streaming.v2.client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

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
import java.util.concurrent.CompletionException;
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
  void getClient_retries_on_sf_api_not_found() {
    SnowflakeStreamingIngestClient mockClient = Mockito.mock(SnowflakeStreamingIngestClient.class);
    AtomicInteger callCount = new AtomicInteger();

    StreamingClientFactory.setStreamingClientSupplier(
        (clientName, dbName, schemaName, pipeName, props) -> {
          if (callCount.incrementAndGet() == 1) {
            throw new SFException("SfApiNotFoundError", "not found", 404, "Not Found");
          }
          return mockClient;
        });

    SnowflakeStreamingIngestClient result = getClient("pipe-A");

    assertThat(result).isSameAs(mockClient);
    assertThat(callCount.get()).isEqualTo(2);
  }

  @Test
  void getClientAsync_gives_up_on_sf_api_not_found_after_max_duration() {
    Duration maxDuration = Duration.ofSeconds(10);
    AtomicInteger callCount = new AtomicInteger();
    SFException notFound = new SFException("SfApiNotFoundError", "not found", 404, "Not Found");

    StreamingClientFactory.setStreamingClientSupplier(
        (clientName, dbName, schemaName, pipeName, props) -> {
          callCount.incrementAndGet();
          throw notFound;
        });

    long startNanos = System.nanoTime();
    Throwable thrown =
        catchThrowable(
            () ->
                StreamingClientPools.getClientAsync(
                        connectorName,
                        TASK_ID,
                        "pipe-A",
                        sinkTaskConfig,
                        streamingClientProperties,
                        TaskMetrics.noop(),
                        maxDuration)
                    .join());
    Duration elapsed = Duration.ofNanos(System.nanoTime() - startNanos);

    assertThat(thrown).isInstanceOf(CompletionException.class);
    assertThat(thrown.getCause()).isSameAs(notFound);
    assertThat(callCount.get() - 1).as("retries").isGreaterThan(2);
    assertThat(elapsed).isGreaterThanOrEqualTo(maxDuration.minusSeconds(1));
  }

  @Test
  void getClient_does_not_retry_client_invalid_error() {
    AtomicInteger callCount = new AtomicInteger();
    SFException invalid = new SFException("InvalidClientError", "client invalid", 409, "Conflict");

    StreamingClientFactory.setStreamingClientSupplier(
        (clientName, dbName, schemaName, pipeName, props) -> {
          callCount.incrementAndGet();
          throw invalid;
        });

    assertThatThrownBy(() -> getClient("pipe-A")).isSameAs(invalid);
    assertThat(callCount.get()).isEqualTo(1);
  }

  @Test
  void recreateClient_retries_on_sf_api_not_found() {
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
            throw new SFException("SfApiNotFoundError", "not found", 404, "Not Found");
          }
          return newClient;
        });

    getClient("pipe-A");

    SnowflakeStreamingIngestClient result = recreateClient("pipe-A", oldClient);

    assertThat(result).isSameAs(newClient);
    assertThat(callCount.get()).isEqualTo(3);
  }

  @Test
  void recreateClient_retries_mixed_client_invalid_and_sf_api_not_found() {
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
            throw new SFException("InvalidClientError", "client invalid", 409, "Conflict");
          }
          if (count == 3) {
            throw new SFException("SfApiNotFoundError", "not found", 404, "Not Found");
          }
          return newClient;
        });

    getClient("pipe-A");

    SnowflakeStreamingIngestClient result = recreateClient("pipe-A", oldClient);

    assertThat(result).isSameAs(newClient);
    assertThat(callCount.get()).isEqualTo(4);
  }

  @Test
  void recreateClient_retries_on_client_invalid_error() {
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
            throw new SFException("InvalidClientError", "client invalid", 409, "Conflict");
          }
          return newClient;
        });

    getClient("pipe-A");

    SnowflakeStreamingIngestClient result = recreateClient("pipe-A", oldClient);

    assertThat(result).isSameAs(newClient);
    assertThat(callCount.get()).isEqualTo(3);
  }
}
