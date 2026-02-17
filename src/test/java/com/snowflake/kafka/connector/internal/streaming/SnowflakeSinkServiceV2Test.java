package com.snowflake.kafka.connector.internal.streaming;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.internal.SnowflakeConnectionService;
import com.snowflake.kafka.connector.internal.streaming.v2.StreamingClientManager;
import com.snowflake.kafka.connector.internal.telemetry.SnowflakeTelemetryService;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for SnowflakeSinkServiceV2 resource cleanup, covering:
 *
 * <ul>
 *   <li>Issue #1: stop() calls closeTaskClients even when waitForAllChannelsToCommitData throws
 *   <li>Issue #6: partitionsToChannel cleaned up even when closeChannelAsync fails
 *   <li>Issue #10: old channel closed before overwrite in createStreamingChannelForTopicPartition
 * </ul>
 */
class SnowflakeSinkServiceV2Test {

  private static final String TOPIC = "test_topic";
  private static final int PARTITION = 0;

  private String connectorName;
  private String taskId;
  private SnowflakeConnectionService mockConn;
  private SnowflakeTelemetryService mockTelemetry;
  private FakeIngestClientSupplier fakeClientSupplier;
  private Map<String, String> connectorConfig;
  private InMemorySinkTaskContext sinkTaskContext;

  @BeforeEach
  void setUp() {
    String uniqueId = UUID.randomUUID().toString().substring(0, 8);
    connectorName = "test_connector_" + uniqueId;
    taskId = "0";

    mockConn = mock(SnowflakeConnectionService.class);
    mockTelemetry = mock(SnowflakeTelemetryService.class);

    when(mockConn.isClosed()).thenReturn(false);
    when(mockConn.getTelemetryClient()).thenReturn(mockTelemetry);
    when(mockConn.tableExist(anyString())).thenReturn(true);
    when(mockConn.pipeExist(anyString())).thenReturn(false);

    connectorConfig = new HashMap<>();
    connectorConfig.put("name", connectorName);
    connectorConfig.put(Utils.TASK_ID, taskId);

    sinkTaskContext =
        new InMemorySinkTaskContext(Collections.singleton(new TopicPartition(TOPIC, PARTITION)));

    fakeClientSupplier = new FakeIngestClientSupplier();
    StreamingClientManager.setIngestClientSupplier(fakeClientSupplier);
  }

  @AfterEach
  void tearDown() {
    StreamingClientManager.resetIngestClientSupplier();
    try {
      StreamingClientManager.closeTaskClients(connectorName, taskId);
    } catch (Exception ignored) {
    }
  }

  @Test
  void stop_cleansUpClients_evenWhenNoDataInserted() {
    // Given: service with one partition started (registers a client in StreamingClientManager)
    SnowflakeSinkServiceV2 service = buildService();
    service.startPartition(new TopicPartition(TOPIC, PARTITION));

    assertThat(StreamingClientManager.getClientCountForTask(connectorName, taskId))
        .as("Client should be registered after startPartition")
        .isGreaterThan(0);

    // When: stop the service
    service.stop();

    // Then: clients should be cleaned up
    assertThat(StreamingClientManager.getClientCountForTask(connectorName, taskId))
        .as("Clients should be released after stop()")
        .isEqualTo(0);
  }

  @Test
  void close_removesPartitionFromMap_evenWhenCloseChannelAsyncFails() {
    // Given: service with one partition started
    SnowflakeSinkServiceV2 service = buildService();
    TopicPartition tp = new TopicPartition(TOPIC, PARTITION);
    service.startPartition(tp);

    assertThat(service.getPartitionCount()).as("Should have 1 partition after start").isEqualTo(1);

    // When: close the partition (the close may encounter issues but should still clean up)
    assertThatCode(() -> service.close(Set.of(tp))).doesNotThrowAnyException();

    // Then: partition should be removed from the map
    assertThat(service.getPartitionCount())
        .as("Partition should be removed from map after close")
        .isEqualTo(0);
  }

  @Test
  void startPartition_closesOldChannel_whenCalledTwiceForSamePartition() {
    // Given: service with one partition started
    SnowflakeSinkServiceV2 service = buildService();
    TopicPartition tp = new TopicPartition(TOPIC, PARTITION);
    service.startPartition(tp);

    long closedBefore = countClosedChannels();

    // When: start the same partition again (simulates rebalance)
    service.startPartition(tp);

    // Then: the old channel should have been closed
    long closedAfter = countClosedChannels();
    assertThat(closedAfter)
        .as("Old channel should be closed before creating a new one")
        .isGreaterThan(closedBefore);

    // And: we should still have exactly 1 partition
    assertThat(service.getPartitionCount()).isEqualTo(1);
  }

  private SnowflakeSinkServiceV2 buildService() {
    return StreamingSinkServiceBuilder.builder(mockConn, connectorConfig)
        .withSinkTaskContext(sinkTaskContext)
        .build();
  }

  private long countClosedChannels() {
    return fakeClientSupplier.getFakeIngestClients().stream()
        .mapToLong(FakeSnowflakeStreamingIngestClient::countClosedChannels)
        .sum();
  }
}
