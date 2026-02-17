package com.snowflake.kafka.connector.internal.streaming;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.internal.SnowflakeConnectionService;
import com.snowflake.kafka.connector.internal.streaming.v2.StreamingClientManager;
import com.snowflake.kafka.connector.internal.telemetry.SnowflakeTelemetryService;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
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
  void stop_cleansUpClients_evenWhenFlushFails() {
    // Given: service with one partition started and a record inserted
    SnowflakeSinkServiceV2 service = buildService();
    service.startPartition(new TopicPartition(TOPIC, PARTITION));

    // Insert a valid JSON record so lastAppendRowsOffset is set, which triggers the flush path
    SinkRecord record = createJsonRecord(0);
    service.insert(record);

    assertThat(StreamingClientManager.getClientCountForTask(connectorName, taskId))
        .as("Client should be registered after startPartition")
        .isGreaterThan(0);

    // When: stop the service — waitForAllChannelsToCommitData will throw because
    // FakeSnowflakeStreamingIngestClient.initiateFlush() throws UnsupportedOperationException,
    // but the try-finally in stop() ensures closeTaskClients still runs
    service.stop();

    // Then: clients should be cleaned up despite the flush failure
    assertThat(StreamingClientManager.getClientCountForTask(connectorName, taskId))
        .as("Clients should be released after stop() even when flush fails")
        .isEqualTo(0);
  }

  @Test
  void close_removesPartitionFromMap_evenWhenCloseChannelAsyncFails() {
    // Given: service with one partition started, channel configured to throw on close
    SnowflakeSinkServiceV2 service = buildService();
    TopicPartition tp = new TopicPartition(TOPIC, PARTITION);
    service.startPartition(tp);

    assertThat(service.getPartitionCount()).as("Should have 1 partition after start").isEqualTo(1);

    // Configure channel to throw RuntimeException on close — this propagates through
    // tryRecoverFromCloseChannelError (non-SFException) so the future fails, but
    // whenComplete still runs partitionsToChannel.remove(key)
    fakeClientSupplier.setThrowOnCloseForAllChannels();

    // When: close the partition — exception propagates through CompletableFuture.allOf().join()
    try {
      service.close(Set.of(tp));
    } catch (Exception expected) {
      // Expected: RuntimeException from channel close propagates
    }

    // Then: partition should still be removed from the map via whenComplete
    assertThat(service.getPartitionCount())
        .as("Partition should be removed from map after close even when channel close fails")
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

  private SinkRecord createJsonRecord(long offset) {
    JsonConverter jsonConverter = new JsonConverter();
    jsonConverter.configure(Map.of("schemas.enable", "false"), false);
    SchemaAndValue schemaAndValue =
        jsonConverter.toConnectData(TOPIC, "{\"name\":\"test\"}".getBytes(StandardCharsets.UTF_8));
    return new SinkRecord(
        TOPIC, PARTITION, null, null, schemaAndValue.schema(), schemaAndValue.value(), offset);
  }
}
