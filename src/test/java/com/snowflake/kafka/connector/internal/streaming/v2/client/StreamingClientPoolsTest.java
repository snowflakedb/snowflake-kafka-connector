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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class StreamingClientPoolsTest {

  private static final String TASK_ID = "test-task";

  private static final String T1_ENVOY_NR_404 =
      "HTTP request failed with a non-retryable error for API get_subdomain_name."
          + " HTTP 404, error_code=, message=,"
          + " url=https://example.snowflakecomputing.com/v2/streaming/hostname?requestId=abc";

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
  void getClient_retries_on_unenveloped_nr_404() {
    SnowflakeStreamingIngestClient mockClient = Mockito.mock(SnowflakeStreamingIngestClient.class);
    AtomicInteger callCount = new AtomicInteger();

    StreamingClientFactory.setStreamingClientSupplier(
        (clientName, dbName, schemaName, pipeName, props) -> {
          if (callCount.incrementAndGet() == 1) {
            throw new SFException("SfApiUserError", T1_ENVOY_NR_404, 400, "Bad Request");
          }
          return mockClient;
        });

    SnowflakeStreamingIngestClient result = getClient("pipe-A");

    assertThat(result).isSameAs(mockClient);
    assertThat(callCount.get()).isEqualTo(2);
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
  void recreateClient_does_not_retry_unenveloped_nr_404() {
    SnowflakeStreamingIngestClient oldClient = Mockito.mock(SnowflakeStreamingIngestClient.class);
    AtomicInteger callCount = new AtomicInteger();
    SFException nr404 = new SFException("SfApiUserError", T1_ENVOY_NR_404, 400, "Bad Request");

    StreamingClientFactory.setStreamingClientSupplier(
        (clientName, dbName, schemaName, pipeName, props) -> {
          int count = callCount.incrementAndGet();
          if (count == 1) {
            return oldClient;
          }
          throw nr404;
        });

    getClient("pipe-A");

    assertThatThrownBy(() -> recreateClient("pipe-A", oldClient)).isSameAs(nr404);
    assertThat(callCount.get()).isEqualTo(2);
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
