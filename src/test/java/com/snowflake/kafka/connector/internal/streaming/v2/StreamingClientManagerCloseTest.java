package com.snowflake.kafka.connector.internal.streaming.v2;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import com.snowflake.ingest.streaming.ChannelStatus;
import com.snowflake.ingest.streaming.ChannelStatusBatch;
import com.snowflake.ingest.streaming.OpenChannelResult;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import com.snowflake.kafka.connector.internal.streaming.StreamingClientProperties;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for StreamingClientManager resource cleanup, specifically verifying that client close
 * failures don't prevent other clients from being closed, and that empty connector entries are
 * pruned from the static map.
 */
class StreamingClientManagerCloseTest {

  private String connectorName;
  private String task1;
  private String pipe1;
  private String pipe2;
  private CloseTrackingClientSupplier clientSupplier;

  @BeforeEach
  void setUp() {
    String uniqueId = UUID.randomUUID().toString().substring(0, 8);
    connectorName = "test_connector_" + uniqueId;
    task1 = "task_" + uniqueId;
    pipe1 = "pipe1_" + uniqueId;
    pipe2 = "pipe2_" + uniqueId;

    clientSupplier = new CloseTrackingClientSupplier();
    StreamingClientManager.setIngestClientSupplier(clientSupplier);
  }

  @AfterEach
  void tearDown() {
    StreamingClientManager.resetIngestClientSupplier();
    // Clean up any remaining clients
    try {
      StreamingClientManager.closeTaskClients(connectorName, task1);
    } catch (Exception ignored) {
    }
  }

  @Test
  void closeTaskClients_continuesClosingWhenOneClientThrows() {
    // Given: task uses two pipes, first client will throw on close
    Map<String, String> config = minimalConfig();
    StreamingClientProperties props = new StreamingClientProperties(config);

    StreamingClientManager.getClient(connectorName, task1, pipe1, config, props);
    StreamingClientManager.getClient(connectorName, task1, pipe2, config, props);
    assertThat(StreamingClientManager.getClientCountForTask(connectorName, task1)).isEqualTo(2);

    // Configure first pipe's client to throw on close
    clientSupplier.setThrowOnCloseForPipe(pipe1);

    // When: close all clients for the task
    assertThatCode(() -> StreamingClientManager.closeTaskClients(connectorName, task1))
        .doesNotThrowAnyException();

    // Then: both clients should have been attempted for close
    assertThat(clientSupplier.getCloseAttemptCount()).isEqualTo(2);
    // And the task should have no more clients
    assertThat(StreamingClientManager.getClientCountForTask(connectorName, task1)).isEqualTo(0);
  }

  @Test
  void closeTaskClients_removesConnectorEntryWhenAllClientsAndTasksGone() {
    // Given: register a client
    Map<String, String> config = minimalConfig();
    StreamingClientProperties props = new StreamingClientProperties(config);

    StreamingClientManager.getClient(connectorName, task1, pipe1, config, props);

    // When: close the only task
    StreamingClientManager.closeTaskClients(connectorName, task1);

    // Then: the connector entry should be pruned (getClientCountForTask returns 0 for unknown
    // connector)
    assertThat(StreamingClientManager.getClientCountForTask(connectorName, task1)).isEqualTo(0);

    // And: re-registering should work (proves old entry was cleaned up)
    SnowflakeStreamingIngestClient newClient =
        StreamingClientManager.getClient(connectorName, task1, pipe1, config, props);
    assertThat(newClient).isNotNull();

    // Cleanup
    StreamingClientManager.closeTaskClients(connectorName, task1);
  }

  @Test
  void closeTaskClients_removesConnectorEntryEvenWhenClientCloseThrows() {
    // Given: register a client that will throw on close
    Map<String, String> config = minimalConfig();
    StreamingClientProperties props = new StreamingClientProperties(config);

    StreamingClientManager.getClient(connectorName, task1, pipe1, config, props);
    clientSupplier.setThrowOnCloseForPipe(pipe1);

    // When: close the task (client throws)
    assertThatCode(() -> StreamingClientManager.closeTaskClients(connectorName, task1))
        .doesNotThrowAnyException();

    // Then: connector entry should still be pruned
    assertThat(StreamingClientManager.getClientCountForTask(connectorName, task1)).isEqualTo(0);
  }

  private Map<String, String> minimalConfig() {
    Map<String, String> config = new HashMap<>();
    config.put("name", connectorName);
    return config;
  }

  /** Client supplier that tracks close attempts and can simulate close failures. */
  static class CloseTrackingClientSupplier implements IngestClientSupplier {

    private final AtomicInteger closeAttemptCount = new AtomicInteger(0);
    private volatile String throwOnCloseForPipe = null;

    @Override
    public SnowflakeStreamingIngestClient get(
        String clientName,
        String dbName,
        String schemaName,
        String pipeName,
        Map<String, String> connectorConfig,
        StreamingClientProperties streamingClientProperties) {
      return new CloseTrackingClient(pipeName, this);
    }

    void setThrowOnCloseForPipe(String pipeName) {
      this.throwOnCloseForPipe = pipeName;
    }

    int getCloseAttemptCount() {
      return closeAttemptCount.get();
    }

    void recordCloseAttempt() {
      closeAttemptCount.incrementAndGet();
    }

    boolean shouldThrowOnClose(String pipeName) {
      return pipeName.equals(throwOnCloseForPipe);
    }
  }

  static class CloseTrackingClient implements SnowflakeStreamingIngestClient {

    private final String pipeName;
    private final CloseTrackingClientSupplier supplier;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    CloseTrackingClient(String pipeName, CloseTrackingClientSupplier supplier) {
      this.pipeName = pipeName;
      this.supplier = supplier;
    }

    @Override
    public void close() {
      supplier.recordCloseAttempt();
      if (supplier.shouldThrowOnClose(pipeName)) {
        throw new RuntimeException("Simulated close failure for pipe: " + pipeName);
      }
      closed.set(true);
    }

    @Override
    public CompletableFuture<Void> close(boolean waitForFlush, Duration timeoutDuration) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void initiateFlush() {}

    @Override
    public OpenChannelResult openChannel(String channelName) {
      throw new UnsupportedOperationException();
    }

    @Override
    public OpenChannelResult openChannel(String channelName, String offsetToken) {
      ChannelStatus status =
          new ChannelStatus(
              "db",
              "schema",
              pipeName,
              channelName,
              "SUCCESS",
              offsetToken,
              Instant.now(),
              0,
              0,
              0,
              null,
              null,
              null,
              null,
              Instant.now());
      return new OpenChannelResult(new StubChannel(channelName), status);
    }

    @Override
    public void dropChannel(String channelName) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, String> getLatestCommittedOffsetTokens(List<String> channelNames) {
      throw new UnsupportedOperationException();
    }

    @Override
    public ChannelStatusBatch getChannelStatus(List<String> channelNames) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean isClosed() {
      return closed.get();
    }

    @Override
    public CompletableFuture<Void> waitForFlush(Duration timeoutDuration) {
      throw new UnsupportedOperationException();
    }

    @Override
    public String getDBName() {
      return "db";
    }

    @Override
    public String getSchemaName() {
      return "schema";
    }

    @Override
    public String getPipeName() {
      return pipeName;
    }

    @Override
    public String getClientName() {
      return "test_client";
    }
  }

  static class StubChannel implements SnowflakeStreamingIngestChannel {

    private final String channelName;

    StubChannel(String channelName) {
      this.channelName = channelName;
    }

    @Override
    public String getDBName() {
      return "db";
    }

    @Override
    public String getSchemaName() {
      return "schema";
    }

    @Override
    public String getPipeName() {
      return "pipe";
    }

    @Override
    public String getFullyQualifiedPipeName() {
      return "pipe";
    }

    @Override
    public String getFullyQualifiedChannelName() {
      return channelName;
    }

    @Override
    public boolean isClosed() {
      return false;
    }

    @Override
    public String getChannelName() {
      return channelName;
    }

    @Override
    public void close() {}

    @Override
    public void close(boolean waitForFlush, Duration timeoutDuration) {}

    @Override
    public void appendRow(Map<String, Object> row, String offsetToken) {}

    @Override
    public void appendRows(
        Iterable<Map<String, Object>> rows, String startOffsetToken, String endOffsetToken) {}

    @Override
    public String getLatestCommittedOffsetToken() {
      return null;
    }

    @Override
    public ChannelStatus getChannelStatus() {
      throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Void> waitForCommit(
        Predicate<String> tokenChecker, Duration timeoutDuration) {
      throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Void> waitForFlush(Duration timeoutDuration) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void initiateFlush() {}
  }
}
