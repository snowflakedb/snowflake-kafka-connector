package com.snowflake.kafka.connector.internal.streaming;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.snowflake.kafka.connector.config.SinkTaskConfig;
import com.snowflake.kafka.connector.config.SinkTaskConfigTestBuilder;
import com.snowflake.kafka.connector.internal.SnowflakeConnectionService;
import com.snowflake.kafka.connector.internal.metrics.TaskMetrics;
import com.snowflake.kafka.connector.internal.streaming.v2.service.BatchOffsetFetcher;
import com.snowflake.kafka.connector.internal.streaming.v2.service.PartitionChannelManager;
import com.snowflake.kafka.connector.internal.streaming.v2.service.ThreadPools;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/**
 * Reproduces the SSV2-side contribution to the SNOW-3647384 fix.
 *
 * <p>When an in-flight channel open completes after a partition has been revoked, it re-enqueues a
 * pending offset reset for that revoked partition. If {@link
 * SnowflakeSinkServiceV2#insert(java.util.Collection)} then applied that reset via {@code
 * sinkTaskContext.offset(...)}, {@code WorkerSinkTask.rewind()} would seek a partition the consumer
 * no longer owns and throw {@code IllegalStateException: No current assignment for partition},
 * killing the task.
 *
 * <p>The fix drops resets for revoked partitions inside {@link
 * PartitionChannelManager#drainPendingOffsetResets(Set)} (see {@code
 * PartitionChannelManagerTest#drainPendingOffsetResetsDropsResetsForUnassignedPartitions}). This
 * test pins {@code insert()}'s half of the contract: it must forward the task's <b>current</b>
 * assignment to the drain so the manager can do that filtering.
 */
class SnowflakeSinkServiceV2RebalanceRewindTest {

  private static final String TOPIC = "test_topic";
  private static final String CONNECTOR_NAME = "test_connector";

  @AfterEach
  void tearDown() {
    ThreadPools.closeForTask(CONNECTOR_NAME);
  }

  @Test
  void insertForwardsCurrentAssignmentToDrain() {
    TopicPartition assignedTp = new TopicPartition(TOPIC, 28);
    Set<TopicPartition> assignment = Set.of(assignedTp);

    InMemorySinkTaskContext sinkTaskContext = new InMemorySinkTaskContext(assignment);

    SinkTaskConfig taskConfig =
        SinkTaskConfigTestBuilder.builder().connectorName(CONNECTOR_NAME).taskId("0").build();

    PartitionChannelManager mockChannelManager = mock(PartitionChannelManager.class);
    when(mockChannelManager.drainPendingOffsetResets(any())).thenReturn(Map.of());

    SnowflakeConnectionService mockConn = mock(SnowflakeConnectionService.class);
    when(mockConn.isClosed()).thenReturn(false);

    SnowflakeSinkServiceV2 service =
        new SnowflakeSinkServiceV2(
            mockConn,
            taskConfig,
            sinkTaskContext,
            Optional.empty(),
            () -> mock(BatchOffsetFetcher.class),
            () -> mockChannelManager,
            TaskMetrics.noop());

    service.insert(Collections.emptyList());

    @SuppressWarnings("unchecked")
    ArgumentCaptor<Set<TopicPartition>> assignmentCaptor = ArgumentCaptor.forClass(Set.class);
    verify(mockChannelManager).drainPendingOffsetResets(assignmentCaptor.capture());
    assertEquals(
        assignment,
        assignmentCaptor.getValue(),
        "insert() must forward the task's current assignment so PartitionChannelManager drops"
            + " pending offset resets for partitions revoked by a rebalance, rather than rewinding"
            + " them and triggering IllegalStateException: No current assignment for partition"
            + " (SNOW-3647384).");
  }
}
