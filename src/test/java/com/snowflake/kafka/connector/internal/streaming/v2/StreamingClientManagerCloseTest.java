package com.snowflake.kafka.connector.internal.streaming.v2;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import com.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import com.snowflake.kafka.connector.internal.streaming.FakeIngestClientSupplier;
import com.snowflake.kafka.connector.internal.streaming.StreamingClientProperties;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
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
  private FakeIngestClientSupplier clientSupplier;

  @BeforeEach
  void setUp() {
    String uniqueId = UUID.randomUUID().toString().substring(0, 8);
    connectorName = "test_connector_" + uniqueId;
    task1 = "task_" + uniqueId;
    pipe1 = "pipe1_" + uniqueId;
    pipe2 = "pipe2_" + uniqueId;

    clientSupplier = new FakeIngestClientSupplier();
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
    assertThat(clientSupplier.getTotalCloseAttemptCount()).isEqualTo(2);
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
}
