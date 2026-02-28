package com.snowflake.kafka.connector.internal.streaming.v2.client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

import com.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import com.snowflake.kafka.connector.internal.SnowflakeKafkaConnectorException;
import com.snowflake.kafka.connector.internal.TestUtils;
import com.snowflake.kafka.connector.internal.metrics.TaskMetrics;
import com.snowflake.kafka.connector.internal.streaming.StreamingClientProperties;
import java.io.IOException;
import java.net.URLClassLoader;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class StreamingClientPoolTest {

  private Map<String, String> connectorConfig;
  private StreamingClientProperties streamingClientProperties;

  @BeforeEach
  void setUp() {
    connectorConfig = TestUtils.getConnectorConfigurationForStreaming(false);
    streamingClientProperties = new StreamingClientProperties(connectorConfig);
  }

  @AfterEach
  void tearDown() {
    StreamingClientFactory.resetStreamingClientSupplier();
  }

  @Nested
  class RefCountedClientTest {

    @Test
    void taskTracking() {
      RefCountedClientTestHarness harness = new RefCountedClientTestHarness();

      // empty initially
      assertThat(harness.refCountedClient.taskCount()).isEqualTo(0);
      assertThat(harness.refCountedClient.hasTask("task-0")).isFalse();

      // add two tasks (duplicate add is idempotent)
      harness.refCountedClient.addTask("task-0");
      harness.refCountedClient.addTask("task-1");
      harness.refCountedClient.addTask("task-0");
      assertThat(harness.refCountedClient.taskCount()).isEqualTo(2);

      // removing unknown task is a no-op
      assertThat(harness.refCountedClient.removeTask("task-unknown")).isFalse();

      // removing one of two is not the last
      assertThat(harness.refCountedClient.removeTask("task-0")).isFalse();
      assertThat(harness.refCountedClient.hasTask("task-0")).isFalse();
      assertThat(harness.refCountedClient.hasTask("task-1")).isTrue();

      // removing the final task signals empty
      assertThat(harness.refCountedClient.removeTask("task-1")).isTrue();
      assertThat(harness.refCountedClient.taskCount()).isEqualTo(0);
    }

    @Test
    void awaitClient_returns_client_on_success() {
      SnowflakeStreamingIngestClient mockClient = mock(SnowflakeStreamingIngestClient.class);
      setSupplierReturning(mockClient);

      RefCountedClientTestHarness harness = new RefCountedClientTestHarness();

      SnowflakeStreamingIngestClient result = harness.refCountedClient.awaitClient("test-pipe");
      assertThat(result).isSameAs(mockClient);
    }

    @Test
    void awaitClient_unwraps_RuntimeException_and_subsequent_attempt_succeeds() {
      SnowflakeKafkaConnectorException originalException =
          new SnowflakeKafkaConnectorException("boom", "TEST_ERROR");
      setSupplierThrowing(originalException);

      RefCountedClientTestHarness failedHarness = new RefCountedClientTestHarness();
      assertThatThrownBy(() -> failedHarness.refCountedClient.awaitClient("test-pipe"))
          .isSameAs(originalException);

      // a new RefCountedClient with a working supplier succeeds
      SnowflakeStreamingIngestClient mockClient = mock(SnowflakeStreamingIngestClient.class);
      setSupplierReturning(mockClient);

      RefCountedClientTestHarness successHarness = new RefCountedClientTestHarness();
      assertThat(successHarness.refCountedClient.awaitClient("test-pipe")).isSameAs(mockClient);
    }

    @Test
    void awaitClient_wraps_checked_exception_in_ConnectException() {
      IOException checkedException = new IOException("disk error");
      setSupplierThrowingChecked(checkedException);

      RefCountedClientTestHarness harness = new RefCountedClientTestHarness();

      assertThatThrownBy(() -> harness.refCountedClient.awaitClient("test-pipe"))
          .isInstanceOf(ConnectException.class)
          .hasCause(checkedException)
          .hasMessageContaining("test-pipe");
    }

    @Test
    void close_calls_close_on_client() {
      SnowflakeStreamingIngestClient mockClient = mock(SnowflakeStreamingIngestClient.class);
      setSupplierReturning(mockClient);

      RefCountedClientTestHarness harness = new RefCountedClientTestHarness();
      harness.refCountedClient.awaitClient("test-pipe");

      harness.refCountedClient.close("test-pipe", "test-connector");

      verify(mockClient).close();
    }

    /**
     * Helper that creates a RefCountedClient with the currently-installed supplier. Must be called
     * after configuring the supplier via {@code setSupplier*} methods.
     */
    class RefCountedClientTestHarness {
      final StreamingClientPool.RefCountedClient refCountedClient;

      RefCountedClientTestHarness() {
        this.refCountedClient =
            new StreamingClientPool.RefCountedClient(
                "test-pipe",
                "test-connector",
                connectorConfig,
                streamingClientProperties,
                TaskMetrics.noop(),
                Executors.newSingleThreadExecutor());
      }
    }
  }

  @Nested
  class PoolTest {

    private StreamingClientPool pool;
    private String connectorName;

    @BeforeEach
    void setUp() {
      connectorName = "test-connector-" + UUID.randomUUID().toString().substring(0, 8);
      pool = new StreamingClientPool(connectorName);
    }

    @AfterEach
    void tearDownPool() {
      pool.shutdown();
    }

    @Test
    void getClient_creates_client_for_new_pipe() {
      SnowflakeStreamingIngestClient mockClient = mock(SnowflakeStreamingIngestClient.class);
      setSupplierReturning(mockClient);

      SnowflakeStreamingIngestClient result =
          pool.getClient(
              "task-0", "pipe-A", connectorConfig, streamingClientProperties, TaskMetrics.noop());

      assertThat(result).isSameAs(mockClient);
    }

    @Test
    void getClient_reuses_client_for_same_pipe() {
      AtomicInteger callCount = new AtomicInteger();
      StreamingClientFactory.setStreamingClientSupplier(
          (clientName, dbName, schemaName, pipeName, config, props) -> {
            callCount.incrementAndGet();
            return mock(SnowflakeStreamingIngestClient.class);
          });

      pool.getClient(
          "task-0", "pipe-A", connectorConfig, streamingClientProperties, TaskMetrics.noop());
      pool.getClient(
          "task-1", "pipe-A", connectorConfig, streamingClientProperties, TaskMetrics.noop());

      assertThat(callCount.get())
          .as("supplier should only be called once for the same pipe")
          .isEqualTo(1);
    }

    @Test
    void getClient_returns_different_clients_for_different_pipes() {
      AtomicInteger callCount = new AtomicInteger();
      StreamingClientFactory.setStreamingClientSupplier(
          (clientName, dbName, schemaName, pipeName, config, props) -> {
            callCount.incrementAndGet();
            return mock(SnowflakeStreamingIngestClient.class);
          });

      SnowflakeStreamingIngestClient clientA =
          pool.getClient(
              "task-0", "pipe-A", connectorConfig, streamingClientProperties, TaskMetrics.noop());
      SnowflakeStreamingIngestClient clientB =
          pool.getClient(
              "task-0", "pipe-B", connectorConfig, streamingClientProperties, TaskMetrics.noop());

      assertThat(clientA).isNotSameAs(clientB);
      assertThat(callCount.get()).isEqualTo(2);
    }

    @Test
    void getClientCountForTask_counts_only_that_tasks_pipes() {
      setSupplierReturning(mock(SnowflakeStreamingIngestClient.class));

      // initially zero
      assertThat(pool.getClientCountForTask("task-0")).isEqualTo(0);

      // task-0 on two pipes, task-1 on one — counts are independent
      pool.getClient(
          "task-0", "pipe-A", connectorConfig, streamingClientProperties, TaskMetrics.noop());
      pool.getClient(
          "task-0", "pipe-B", connectorConfig, streamingClientProperties, TaskMetrics.noop());
      pool.getClient(
          "task-1", "pipe-B", connectorConfig, streamingClientProperties, TaskMetrics.noop());
      assertThat(pool.getClientCountForTask("task-0")).isEqualTo(2);
      assertThat(pool.getClientCountForTask("task-1")).isEqualTo(1);
    }

    @Test
    void closeTaskClients_removes_entry_when_last_task_released() {
      SnowflakeStreamingIngestClient mockClient = mock(SnowflakeStreamingIngestClient.class);
      setSupplierReturning(mockClient);

      pool.getClient(
          "task-0", "pipe-A", connectorConfig, streamingClientProperties, TaskMetrics.noop());
      pool.closeTaskClients("task-0");

      assertThat(pool.getClientCountForTask("task-0")).isEqualTo(0);
      verify(mockClient, timeout(5000)).close();
    }

    @Test
    void closeTaskClients_keeps_client_when_other_tasks_remain() {
      SnowflakeStreamingIngestClient mockClient = mock(SnowflakeStreamingIngestClient.class);
      setSupplierReturning(mockClient);

      pool.getClient(
          "task-0", "pipe-A", connectorConfig, streamingClientProperties, TaskMetrics.noop());
      pool.getClient(
          "task-1", "pipe-A", connectorConfig, streamingClientProperties, TaskMetrics.noop());

      pool.closeTaskClients("task-0");

      assertThat(pool.getClientCountForTask("task-1")).isEqualTo(1);
      verify(mockClient, never()).close();
    }

    @Test
    void closeTaskClients_then_getClient_creates_new_client() {
      AtomicInteger callCount = new AtomicInteger();
      StreamingClientFactory.setStreamingClientSupplier(
          (clientName, dbName, schemaName, pipeName, config, props) -> {
            callCount.incrementAndGet();
            return mock(SnowflakeStreamingIngestClient.class);
          });

      SnowflakeStreamingIngestClient first =
          pool.getClient(
              "task-0", "pipe-A", connectorConfig, streamingClientProperties, TaskMetrics.noop());
      pool.closeTaskClients("task-0");

      SnowflakeStreamingIngestClient second =
          pool.getClient(
              "task-0", "pipe-A", connectorConfig, streamingClientProperties, TaskMetrics.noop());

      assertThat(second).isNotSameAs(first);
      assertThat(callCount.get()).isEqualTo(2);
    }

    @Test
    void closeTaskClients_for_unknown_task_does_not_throw() {
      pool.closeTaskClients("nonexistent-task");
    }

    @Test
    void getClient_removes_entry_on_failure_and_rethrows() {
      SnowflakeKafkaConnectorException originalException =
          new SnowflakeKafkaConnectorException("creation failed", "TEST_ERROR");
      setSupplierThrowing(originalException);

      assertThatThrownBy(
              () ->
                  pool.getClient(
                      "task-0",
                      "pipe-A",
                      connectorConfig,
                      streamingClientProperties,
                      TaskMetrics.noop()))
          .isSameAs(originalException);

      assertThat(pool.getClientCountForTask("task-0")).isEqualTo(0);
    }

    @Test
    void getClient_after_failure_retries_creation() {
      AtomicInteger callCount = new AtomicInteger();
      SnowflakeStreamingIngestClient mockClient = mock(SnowflakeStreamingIngestClient.class);

      StreamingClientFactory.setStreamingClientSupplier(
          (clientName, dbName, schemaName, pipeName, config, props) -> {
            if (callCount.incrementAndGet() == 1) {
              throw new SnowflakeKafkaConnectorException("transient", "TEST_ERROR");
            }
            return mockClient;
          });

      assertThatThrownBy(
              () ->
                  pool.getClient(
                      "task-0",
                      "pipe-A",
                      connectorConfig,
                      streamingClientProperties,
                      TaskMetrics.noop()))
          .isInstanceOf(SnowflakeKafkaConnectorException.class);

      SnowflakeStreamingIngestClient result =
          pool.getClient(
              "task-0", "pipe-A", connectorConfig, streamingClientProperties, TaskMetrics.noop());

      assertThat(result).isSameAs(mockClient);
      assertThat(callCount.get()).isEqualTo(2);
    }

    @Test
    void pool_threads_inherit_context_classloader_from_pool_creator() {
      AtomicReference<ClassLoader> capturedClassLoader = new AtomicReference<>();
      StreamingClientFactory.setStreamingClientSupplier(
          (clientName, dbName, schemaName, pipeName, config, props) -> {
            capturedClassLoader.set(Thread.currentThread().getContextClassLoader());
            return mock(SnowflakeStreamingIngestClient.class);
          });

      // Simulate Kafka Connect's PluginClassLoader by setting a custom context classloader
      // before creating the pool — the factory captures it at construction time.
      URLClassLoader fakePluginCL = new URLClassLoader(new java.net.URL[0], null);
      ClassLoader originalCL = Thread.currentThread().getContextClassLoader();
      Thread.currentThread().setContextClassLoader(fakePluginCL);
      StreamingClientPool poolWithCustomCL;
      try {
        poolWithCustomCL =
            new StreamingClientPool(
                "test-connector-cl-" + UUID.randomUUID().toString().substring(0, 8));
      } finally {
        Thread.currentThread().setContextClassLoader(originalCL);
      }

      try {
        poolWithCustomCL.getClient(
            "task-0", "pipe-A", connectorConfig, streamingClientProperties, TaskMetrics.noop());

        assertThat(capturedClassLoader.get())
            .as("Pool thread should have the classloader from the pool creator")
            .isSameAs(fakePluginCL);
      } finally {
        poolWithCustomCL.shutdown();
      }
    }

    @Test
    void getClient_parallel_for_different_pipes_creates_concurrently() throws Exception {
      CountDownLatch bothStarted = new CountDownLatch(2);
      CountDownLatch proceed = new CountDownLatch(1);

      StreamingClientFactory.setStreamingClientSupplier(
          (clientName, dbName, schemaName, pipeName, config, props) -> {
            bothStarted.countDown();
            try {
              proceed.await();
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              throw new RuntimeException(e);
            }
            return mock(SnowflakeStreamingIngestClient.class);
          });

      CompletableFuture<SnowflakeStreamingIngestClient> futureA =
          CompletableFuture.supplyAsync(
              () ->
                  pool.getClient(
                      "task-0",
                      "pipe-A",
                      connectorConfig,
                      streamingClientProperties,
                      TaskMetrics.noop()));
      CompletableFuture<SnowflakeStreamingIngestClient> futureB =
          CompletableFuture.supplyAsync(
              () ->
                  pool.getClient(
                      "task-1",
                      "pipe-B",
                      connectorConfig,
                      streamingClientProperties,
                      TaskMetrics.noop()));

      // Both suppliers should have started before either completes
      bothStarted.await();
      proceed.countDown();

      SnowflakeStreamingIngestClient clientA = futureA.join();
      SnowflakeStreamingIngestClient clientB = futureB.join();

      assertThat(clientA).isNotSameAs(clientB);
    }
  }

  private void setSupplierReturning(SnowflakeStreamingIngestClient client) {
    StreamingClientFactory.setStreamingClientSupplier(
        (clientName, dbName, schemaName, pipeName, config, props) -> client);
  }

  private void setSupplierThrowing(RuntimeException exception) {
    StreamingClientFactory.setStreamingClientSupplier(
        (clientName, dbName, schemaName, pipeName, config, props) -> {
          throw exception;
        });
  }

  @SuppressWarnings("unchecked")
  private void setSupplierThrowingChecked(Exception checkedException) {
    StreamingClientFactory.setStreamingClientSupplier(
        (clientName, dbName, schemaName, pipeName, config, props) -> {
          sneakyThrow(checkedException);
          return null; // unreachable
        });
  }

  /**
   * Throws a checked exception without declaring it, for testing CompletionException unwrapping.
   */
  @SuppressWarnings("unchecked")
  private static <E extends Throwable> void sneakyThrow(Exception exception) throws E {
    throw (E) exception;
  }
}
