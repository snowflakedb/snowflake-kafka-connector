package com.snowflake.kafka.connector.internal.streaming;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.snowflake.ingest.streaming.SFException;
import com.snowflake.kafka.connector.builder.SinkRecordBuilder;
import com.snowflake.kafka.connector.config.SinkTaskConfig;
import com.snowflake.kafka.connector.config.SinkTaskConfigTestBuilder;
import com.snowflake.kafka.connector.config.SnowflakeValidation;
import com.snowflake.kafka.connector.config.TableType;
import com.snowflake.kafka.connector.internal.SnowflakeConnectionService;
import com.snowflake.kafka.connector.internal.SnowflakeKafkaConnectorException;
import com.snowflake.kafka.connector.internal.metrics.TaskMetrics;
import com.snowflake.kafka.connector.internal.streaming.channel.TopicPartitionChannel;
import com.snowflake.kafka.connector.internal.streaming.v2.BackpressureException;
import com.snowflake.kafka.connector.internal.streaming.v2.service.BatchOffsetFetcher;
import com.snowflake.kafka.connector.internal.streaming.v2.service.PartitionChannelManager;
import com.snowflake.kafka.connector.internal.streaming.v2.service.ThreadPools;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class SnowflakeSinkServiceV2Test {

  private static final String TOPIC = "test_topic";
  private static final String CONNECTOR_NAME = "test_connector";

  private PartitionChannelManager mockChannelManager;
  private BatchOffsetFetcher mockBatchOffsetFetcher;
  private SinkTaskContext mockSinkTaskContext;
  private SnowflakeSinkServiceV2 service;

  @BeforeEach
  void setUp() {
    mockChannelManager = mock(PartitionChannelManager.class);
    when(mockChannelManager.drainPendingOffsetResets(any())).thenReturn(Map.of());
    mockBatchOffsetFetcher = mock(BatchOffsetFetcher.class);
    mockSinkTaskContext = mock(SinkTaskContext.class);
    // insert() asserts that record partitions and rewinds are within the current assignment; the
    // insert() tests below use partitions 0 and 1 of TOPIC.
    when(mockSinkTaskContext.assignment())
        .thenReturn(Set.of(new TopicPartition(TOPIC, 0), new TopicPartition(TOPIC, 1)));

    SnowflakeConnectionService mockConn = mock(SnowflakeConnectionService.class);
    when(mockConn.isClosed()).thenReturn(false);

    service =
        new SnowflakeSinkServiceV2(
            mockConn,
            SinkTaskConfigTestBuilder.builder().connectorName(CONNECTOR_NAME).taskId("0").build(),
            mockSinkTaskContext,
            Optional.empty(),
            () -> mockBatchOffsetFetcher,
            () -> mockChannelManager,
            TaskMetrics.noop());
  }

  @AfterEach
  void tearDown() {
    ThreadPools.closeForTask(CONNECTOR_NAME);
  }

  // --- insert() skip logic ---

  @Test
  void insertSkipsRecordsForInitializingPartitions() {
    TopicPartition tp = new TopicPartition(TOPIC, 0);
    TopicPartitionChannel channel = mockChannel("ch_0", true);
    when(mockChannelManager.getChannel(tp)).thenReturn(Optional.of(channel));

    SinkRecord record = recordFor(TOPIC, 0, 10);
    service.insert(Collections.singletonList(record));

    verify(channel, never()).insertRecord(any());
    verify(mockSinkTaskContext).offset(Map.of(tp, 10L));
  }

  @Test
  @SuppressWarnings("unchecked")
  void insertProcessesRecordsForReadyPartitions() {
    TopicPartition tp = new TopicPartition(TOPIC, 0);
    TopicPartitionChannel channel = mockChannel("ch_0", false);
    when(mockChannelManager.getChannel(tp)).thenReturn(Optional.of(channel));

    SinkRecord record = recordFor(TOPIC, 0, 5);
    service.insert(Collections.singletonList(record));

    verify(channel).insertRecord(record);
    verify(mockSinkTaskContext, never()).offset(any(Map.class));
  }

  @Test
  void insertHandlesMixOfInitializingAndReadyPartitions() {
    TopicPartition tpInit = new TopicPartition(TOPIC, 0);
    TopicPartition tpReady = new TopicPartition(TOPIC, 1);

    TopicPartitionChannel initChannel = mockChannel("ch_0", true);
    TopicPartitionChannel readyChannel = mockChannel("ch_1", false);

    when(mockChannelManager.getChannel(tpInit)).thenReturn(Optional.of(initChannel));
    when(mockChannelManager.getChannel(tpReady)).thenReturn(Optional.of(readyChannel));

    List<SinkRecord> records = Arrays.asList(recordFor(TOPIC, 0, 100), recordFor(TOPIC, 1, 200));

    service.insert(records);

    verify(initChannel, never()).insertRecord(any());
    verify(readyChannel).insertRecord(records.get(1));
    verify(mockSinkTaskContext).offset(Map.of(tpInit, 100L));
  }

  @Test
  void insertResetsToFirstSkippedOffset() {
    TopicPartition tp = new TopicPartition(TOPIC, 0);
    TopicPartitionChannel channel = mockChannel("ch_0", true);
    when(mockChannelManager.getChannel(tp)).thenReturn(Optional.of(channel));

    List<SinkRecord> records =
        Arrays.asList(recordFor(TOPIC, 0, 5), recordFor(TOPIC, 0, 6), recordFor(TOPIC, 0, 7));

    service.insert(records);

    verify(mockSinkTaskContext).offset(Map.of(tp, 5L));
  }

  // --- getCommittedOffsets() skip logic ---

  @Test
  @SuppressWarnings("unchecked")
  void getCommittedOffsetsExcludesInitializingPartitions() {
    TopicPartition tpInit = new TopicPartition(TOPIC, 0);
    TopicPartition tpReady = new TopicPartition(TOPIC, 1);

    TopicPartitionChannel initChannel = mockChannel("ch_0", true);
    TopicPartitionChannel readyChannel = mockChannel("ch_1", false);

    when(mockChannelManager.getChannel(tpInit)).thenReturn(Optional.of(initChannel));
    when(mockChannelManager.getChannel(tpReady)).thenReturn(Optional.of(readyChannel));

    Map<TopicPartition, Long> expectedOffsets = new HashMap<>();
    expectedOffsets.put(tpReady, 42L);
    when(mockBatchOffsetFetcher.getCommittedOffsets(any(), any(Function.class)))
        .thenReturn(expectedOffsets);

    Set<TopicPartition> allPartitions = new HashSet<>(Arrays.asList(tpInit, tpReady));
    Map<TopicPartition, Long> result = service.getCommittedOffsets(allPartitions);

    assertEquals(expectedOffsets, result);

    ArgumentCaptor<Set<TopicPartition>> captor = ArgumentCaptor.forClass(Set.class);
    verify(mockBatchOffsetFetcher).getCommittedOffsets(captor.capture(), any(Function.class));
    Set<TopicPartition> passedPartitions = captor.getValue();
    assertEquals(1, passedPartitions.size());
    assertTrue(passedPartitions.contains(tpReady));
  }

  @Test
  @SuppressWarnings("unchecked")
  void getCommittedOffsetsReturnsEmptyWhenAllInitializing() {
    TopicPartition tp0 = new TopicPartition(TOPIC, 0);
    TopicPartition tp1 = new TopicPartition(TOPIC, 1);

    TopicPartitionChannel ch0 = mockChannel("ch_0", true);
    TopicPartitionChannel ch1 = mockChannel("ch_1", true);
    when(mockChannelManager.getChannel(tp0)).thenReturn(Optional.of(ch0));
    when(mockChannelManager.getChannel(tp1)).thenReturn(Optional.of(ch1));

    when(mockBatchOffsetFetcher.getCommittedOffsets(any(), any(Function.class)))
        .thenReturn(Collections.emptyMap());

    Set<TopicPartition> allPartitions = new HashSet<>(Arrays.asList(tp0, tp1));
    Map<TopicPartition, Long> result = service.getCommittedOffsets(allPartitions);

    assertTrue(result.isEmpty());

    ArgumentCaptor<Set<TopicPartition>> captor = ArgumentCaptor.forClass(Set.class);
    verify(mockBatchOffsetFetcher).getCommittedOffsets(captor.capture(), any(Function.class));
    assertTrue(captor.getValue().isEmpty());
  }

  // --- transition from initializing to ready ---

  @Test
  void insertProcessesRecordsAfterChannelTransitionsFromInitializingToReady() {
    TopicPartition tp = new TopicPartition(TOPIC, 0);
    TopicPartitionChannel channel = mockChannel("ch_0", true);
    when(mockChannelManager.getChannel(tp)).thenReturn(Optional.of(channel));

    SinkRecord record1 = recordFor(TOPIC, 0, 10);
    service.insert(Collections.singletonList(record1));

    verify(channel, never()).insertRecord(any());
    verify(mockSinkTaskContext).offset(Map.of(tp, 10L));

    // Channel finishes initializing — Kafka re-delivers from the rewound offset
    when(channel.isInitializing()).thenReturn(false);

    SinkRecord record2 = recordFor(TOPIC, 0, 11);
    service.insert(List.of(record1, record2));

    verify(channel).insertRecord(record1);
    verify(channel).insertRecord(record2);
  }

  // --- startPartitions() pipe resolution (FR5) ---

  @Test
  void startPartitionsThrowsWhenValidationEnabledAndNonDefaultPipeExists() {
    SnowflakeConnectionService mockConn = mock(SnowflakeConnectionService.class);
    when(mockConn.isClosed()).thenReturn(false);
    when(mockConn.tableExist(TOPIC)).thenReturn(true);
    when(mockConn.pipeExist(TOPIC)).thenReturn(true);

    SnowflakeSinkServiceV2 svc = buildService(mockConn, /* clientValidationEnabled= */ true);

    TopicPartition tp = new TopicPartition(TOPIC, 0);
    SnowflakeKafkaConnectorException exception =
        assertThrows(SnowflakeKafkaConnectorException.class, () -> svc.startPartitions(Set.of(tp)));

    assertTrue(exception.getMessage().contains("0032"));
  }

  @Test
  @SuppressWarnings("unchecked")
  void startPartitionsUsesDefaultPipeWhenValidationEnabledAndNoNonDefaultPipe() {
    SnowflakeConnectionService mockConn = mock(SnowflakeConnectionService.class);
    when(mockConn.isClosed()).thenReturn(false);
    when(mockConn.tableExist(TOPIC)).thenReturn(true);
    when(mockConn.pipeExist(TOPIC)).thenReturn(false);

    PartitionChannelManager channelMgr = mock(PartitionChannelManager.class);
    SnowflakeSinkServiceV2 svc =
        buildService(mockConn, /* clientValidationEnabled= */ true, channelMgr);

    TopicPartition tp = new TopicPartition(TOPIC, 0);
    svc.startPartitions(Set.of(tp));

    ArgumentCaptor<Map<String, String>> captor = ArgumentCaptor.forClass(Map.class);
    verify(channelMgr).startPartitions(any(), captor.capture());
    assertEquals(TOPIC + "-STREAMING", captor.getValue().get(TOPIC));
  }

  @Test
  @SuppressWarnings("unchecked")
  void startPartitionsUsesNonDefaultPipeWhenValidationDisabled() {
    SnowflakeConnectionService mockConn = mock(SnowflakeConnectionService.class);
    when(mockConn.isClosed()).thenReturn(false);
    when(mockConn.tableExist(TOPIC)).thenReturn(true);
    when(mockConn.pipeExist(TOPIC)).thenReturn(true);

    PartitionChannelManager channelMgr = mock(PartitionChannelManager.class);
    SnowflakeSinkServiceV2 svc =
        buildService(mockConn, /* clientValidationEnabled= */ false, channelMgr);

    TopicPartition tp = new TopicPartition(TOPIC, 0);
    svc.startPartitions(Set.of(tp));

    ArgumentCaptor<Map<String, String>> captor = ArgumentCaptor.forClass(Map.class);
    verify(channelMgr).startPartitions(any(), captor.capture());
    assertEquals(TOPIC, captor.getValue().get(TOPIC));
  }

  @Test
  @SuppressWarnings("unchecked")
  void startPartitions_noneType_usesDefaultPipe_noPipeExistenceCheck() {
    // table.type=none no longer requires the pipe to pre-exist at startup; the default pipe is
    // connector-managed (created lazily at first ingest). This test verifies that startPartitions
    // does NOT call pipeExist for a pre-existence guard when the table already exists.
    SnowflakeConnectionService mockConn = mock(SnowflakeConnectionService.class);
    when(mockConn.isClosed()).thenReturn(false);
    when(mockConn.tableExist(TOPIC)).thenReturn(true);
    // Named pipe does NOT exist; default pipe does NOT exist either — neither should cause a
    // failure.
    when(mockConn.pipeExist(TOPIC)).thenReturn(false);
    when(mockConn.hasErrorLoggingEnabled(TOPIC)).thenReturn(true);

    PartitionChannelManager channelMgr = mock(PartitionChannelManager.class);
    SinkTaskConfig config =
        SinkTaskConfigTestBuilder.builder()
            .connectorName(CONNECTOR_NAME)
            .taskId("0")
            .tableType(TableType.NONE)
            .validation(SnowflakeValidation.SERVER_SIDE)
            .enableSanitization(false)
            .build();
    SnowflakeSinkServiceV2 svc =
        new SnowflakeSinkServiceV2(
            mockConn,
            config,
            mockSinkTaskContext,
            Optional.empty(),
            () -> mock(BatchOffsetFetcher.class),
            () -> channelMgr,
            TaskMetrics.noop());

    TopicPartition tp = new TopicPartition(TOPIC, 0);
    svc.startPartitions(Set.of(tp)); // must not throw

    // The default pipe (tableName + STREAMING suffix) must be chosen.
    ArgumentCaptor<Map<String, String>> captor = ArgumentCaptor.forClass(Map.class);
    verify(channelMgr).startPartitions(any(), captor.capture());
    assertEquals(TOPIC + "-STREAMING", captor.getValue().get(TOPIC));
  }

  // --- backpressure handling ---

  @Test
  void insertSkipsAllPartitionsAfterBackpressure() {
    TopicPartition tp0 = new TopicPartition(TOPIC, 0);
    TopicPartition tp1 = new TopicPartition(TOPIC, 1);

    TopicPartitionChannel channel0 = mockChannel("ch_0", false);
    TopicPartitionChannel channel1 = mockChannel("ch_1", false);

    when(mockChannelManager.getChannel(tp0)).thenReturn(Optional.of(channel0));
    when(mockChannelManager.getChannel(tp1)).thenReturn(Optional.of(channel1));

    // channel0 throws BackpressureException
    doThrow(
            new BackpressureException(
                new SFException("MemoryThresholdExceeded", "backpressure", 0, "")))
        .when(channel0)
        .insertRecord(any());

    List<SinkRecord> records = Arrays.asList(recordFor(TOPIC, 0, 100), recordFor(TOPIC, 1, 200));
    service.insert(records);

    // channel0 threw; channel1 is skipped because backpressure stops all partitions
    verify(channel0).insertRecord(records.get(0));
    verify(channel1, never()).insertRecord(any());

    // Both partitions rewound in one call
    verify(mockSinkTaskContext).offset(Map.of(tp0, 100L, tp1, 200L));
  }

  @Test
  void insertSkipsRemainingRecordsForAllPartitionsAfterBackpressure() {
    TopicPartition tp0 = new TopicPartition(TOPIC, 0);
    TopicPartition tp1 = new TopicPartition(TOPIC, 1);

    TopicPartitionChannel channel0 = mockChannel("ch_0", false);
    TopicPartitionChannel channel1 = mockChannel("ch_1", false);

    when(mockChannelManager.getChannel(tp0)).thenReturn(Optional.of(channel0));
    when(mockChannelManager.getChannel(tp1)).thenReturn(Optional.of(channel1));

    // channel1 throws BackpressureException
    doThrow(new BackpressureException(new SFException("ReceiverSaturated", "backpressure", 0, "")))
        .when(channel1)
        .insertRecord(any());

    // p0's first record succeeds, p1 throws, p0's second record is skipped
    List<SinkRecord> records =
        Arrays.asList(recordFor(TOPIC, 0, 100), recordFor(TOPIC, 1, 200), recordFor(TOPIC, 0, 101));
    service.insert(records);

    // channel0 first record processed, channel1 threw, channel0 second record skipped
    verify(channel0).insertRecord(records.get(0));
    verify(channel1).insertRecord(records.get(1));
    verify(channel0, never()).insertRecord(records.get(2));

    // p1 rewound to the backpressured record; p0 rewound to the first skipped record
    verify(mockSinkTaskContext).offset(Map.of(tp0, 101L, tp1, 200L));
  }

  @Test
  void insertRewindsOnBackpressureWithInitializingPartitions() {
    TopicPartition tpInit = new TopicPartition(TOPIC, 0);
    TopicPartition tpReady = new TopicPartition(TOPIC, 1);

    TopicPartitionChannel initChannel = mockChannel("ch_0", true);
    TopicPartitionChannel readyChannel = mockChannel("ch_1", false);

    when(mockChannelManager.getChannel(tpInit)).thenReturn(Optional.of(initChannel));
    when(mockChannelManager.getChannel(tpReady)).thenReturn(Optional.of(readyChannel));

    // Ready channel hits backpressure
    doThrow(
            new BackpressureException(
                new SFException("MemoryThresholdExceeded", "backpressure", 0, "")))
        .when(readyChannel)
        .insertRecord(any());

    List<SinkRecord> records = Arrays.asList(recordFor(TOPIC, 0, 100), recordFor(TOPIC, 1, 200));
    service.insert(records);

    // initChannel skipped (initializing), readyChannel attempted and threw
    verify(initChannel, never()).insertRecord(any());
    verify(readyChannel).insertRecord(records.get(1));

    // Both partitions rewound via offsetsOfFirstSkippedRecord
    verify(mockSinkTaskContext).offset(Map.of(tpInit, 100L, tpReady, 200L));
  }

  @Test
  void insertSetsCooldownAfterBackpressure() {
    TopicPartition tp0 = new TopicPartition(TOPIC, 0);
    TopicPartitionChannel channel0 = mockChannel("ch_0", false);
    when(mockChannelManager.getChannel(tp0)).thenReturn(Optional.of(channel0));

    doThrow(
            new BackpressureException(
                new SFException("MemoryThresholdExceeded", "backpressure", 0, "")))
        .when(channel0)
        .insertRecord(any());

    service.insert(Collections.singletonList(recordFor(TOPIC, 0, 100)));

    // Cooldown should be set to a future time
    assertTrue(
        service.backpressureUntil.isAfter(
            Instant.now().minus(SnowflakeSinkServiceV2.BACKPRESSURE_COOLDOWN)));
  }

  @Test
  void insertSkipsEntireBatchDuringCooldown() {
    TopicPartition tp0 = new TopicPartition(TOPIC, 0);
    TopicPartition tp1 = new TopicPartition(TOPIC, 1);

    TopicPartitionChannel channel0 = mockChannel("ch_0", false);
    TopicPartitionChannel channel1 = mockChannel("ch_1", false);

    when(mockChannelManager.getChannel(tp0)).thenReturn(Optional.of(channel0));
    when(mockChannelManager.getChannel(tp1)).thenReturn(Optional.of(channel1));

    // Set cooldown to a future time
    service.backpressureUntil = Instant.now().plusSeconds(30);

    List<SinkRecord> records = Arrays.asList(recordFor(TOPIC, 0, 100), recordFor(TOPIC, 1, 200));
    service.insert(records);

    // No inserts attempted during cooldown
    verify(channel0, never()).insertRecord(any());
    verify(channel1, never()).insertRecord(any());

    // All partitions rewound
    verify(mockSinkTaskContext).offset(Map.of(tp0, 100L, tp1, 200L));
  }

  @Test
  @SuppressWarnings("unchecked")
  void insertResumesNormallyAfterCooldownExpires() {
    TopicPartition tp0 = new TopicPartition(TOPIC, 0);
    TopicPartitionChannel channel0 = mockChannel("ch_0", false);
    when(mockChannelManager.getChannel(tp0)).thenReturn(Optional.of(channel0));

    // Set cooldown to the past (expired)
    service.backpressureUntil = Instant.now().minusSeconds(1);

    service.insert(Collections.singletonList(recordFor(TOPIC, 0, 100)));

    // Normal processing resumes
    verify(channel0).insertRecord(any());
    verify(mockSinkTaskContext, never()).offset(any(Map.class));
  }

  // --- recovery skip logic ---

  @Test
  void insertSkipsRemainingRecordsForPartitionAfterRecovery() {
    TopicPartition tp0 = new TopicPartition(TOPIC, 0);
    TopicPartition tp1 = new TopicPartition(TOPIC, 1);

    TopicPartitionChannel channel0 = mockChannel("ch_0", false);
    TopicPartitionChannel channel1 = mockChannel("ch_1", false);

    when(mockChannelManager.getChannel(tp0)).thenReturn(Optional.of(channel0));
    when(mockChannelManager.getChannel(tp1)).thenReturn(Optional.of(channel1));

    // channel0 signals recovery on its first record
    when(channel0.insertRecord(any())).thenReturn(false);

    List<SinkRecord> records =
        Arrays.asList(
            recordFor(TOPIC, 0, 100),
            recordFor(TOPIC, 1, 200),
            recordFor(TOPIC, 0, 101),
            recordFor(TOPIC, 0, 102));
    service.insert(records);

    // channel0: only the first record was attempted; 101 and 102 were skipped
    verify(channel0).insertRecord(records.get(0));
    verify(channel0, never()).insertRecord(records.get(2));
    verify(channel0, never()).insertRecord(records.get(3));

    // channel1: processed normally
    verify(channel1).insertRecord(records.get(1));

    // Only the recovering partition is rewound, to the triggering record's offset
    verify(mockSinkTaskContext).offset(Map.of(tp0, 100L));
  }

  @Test
  void insertRewindsToFirstSkippedOffsetAfterRecoveryMidPartition() {
    TopicPartition tp = new TopicPartition(TOPIC, 0);
    TopicPartitionChannel channel = mockChannel("ch_0", false);
    when(mockChannelManager.getChannel(tp)).thenReturn(Optional.of(channel));

    // First record succeeds, second triggers recovery
    when(channel.insertRecord(any())).thenReturn(true).thenReturn(false);

    List<SinkRecord> records =
        Arrays.asList(recordFor(TOPIC, 0, 100), recordFor(TOPIC, 0, 101), recordFor(TOPIC, 0, 102));
    service.insert(records);

    // First two records attempted, third skipped
    verify(channel).insertRecord(records.get(0));
    verify(channel).insertRecord(records.get(1));
    verify(channel, never()).insertRecord(records.get(2));

    // Rewind to the record that triggered recovery
    verify(mockSinkTaskContext).offset(Map.of(tp, 101L));
  }

  // --- helpers ---

  private SnowflakeSinkServiceV2 newService(SnowflakeConnectionService conn, SinkTaskConfig cfg) {
    return new SnowflakeSinkServiceV2(
        conn,
        cfg,
        mockSinkTaskContext,
        Optional.empty(),
        () -> mock(BatchOffsetFetcher.class),
        () -> mock(PartitionChannelManager.class),
        TaskMetrics.noop());
  }

  private SinkTaskConfig cfg(TableType type, String createTableOptions) {
    return SinkTaskConfigTestBuilder.builder()
        .connectorName(CONNECTOR_NAME)
        .taskId("0")
        .tableType(type)
        .icebergCreateTableOptions(Optional.ofNullable(createTableOptions))
        .build();
  }

  @Test
  void createTableIfNotExists_icebergType_callsIcebergCreate() {
    SnowflakeConnectionService conn = mock(SnowflakeConnectionService.class);
    when(conn.tableExist("t1")).thenReturn(false);
    SnowflakeSinkServiceV2 svc = newService(conn, cfg(TableType.ICEBERG, "ICEBERG_VERSION=3"));

    svc.createTableIfNotExists("t1");

    verify(conn).createIcebergTableWithOnlyMetadataColumn("t1", "ICEBERG_VERSION=3");
    verify(conn, never()).createTableWithOnlyMetadataColumn(anyString());
  }

  @Test
  void createTableIfNotExists_snowflakeType_callsFdnCreate() {
    SnowflakeConnectionService conn = mock(SnowflakeConnectionService.class);
    when(conn.tableExist("t1")).thenReturn(false);
    SnowflakeSinkServiceV2 svc = newService(conn, cfg(TableType.SNOWFLAKE, ""));

    svc.createTableIfNotExists("t1");

    verify(conn).createTableWithOnlyMetadataColumn("t1");
    verify(conn, never()).createIcebergTableWithOnlyMetadataColumn(anyString(), anyString());
  }

  @Test
  void createTableIfNotExists_snowflakeType_createdTableIsIceberg_warnsButProceeds() {
    // A bare CREATE TABLE can come out Iceberg if the schema default is
    // DEFAULT_METADATA_WRITE_FORMAT=ICEBERG. That is a benign surprise (the connector ingests into
    // the Iceberg table), so we create, then check and warn -- we do NOT fail.
    SnowflakeConnectionService conn = mock(SnowflakeConnectionService.class);
    when(conn.tableExist("t1")).thenReturn(false);
    when(conn.isIcebergTable("t1")).thenReturn(true); // created table came out Iceberg
    SnowflakeSinkServiceV2 svc = newService(conn, cfg(TableType.SNOWFLAKE, ""));

    svc.createTableIfNotExists("t1"); // must not throw

    verify(conn).createTableWithOnlyMetadataColumn("t1");
    verify(conn).isIcebergTable("t1");
    verify(conn, never()).createIcebergTableWithOnlyMetadataColumn(anyString(), anyString());
  }

  @Test
  void createTableIfNotExists_noneType_missingTable_throws() {
    SnowflakeConnectionService conn = mock(SnowflakeConnectionService.class);
    when(conn.tableExist("t1")).thenReturn(false);
    SnowflakeSinkServiceV2 svc = newService(conn, cfg(TableType.NONE, ""));

    assertThatThrownBy(() -> svc.createTableIfNotExists("t1"))
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("t1");
    verify(conn, never()).createTableWithOnlyMetadataColumn(anyString());
    verify(conn, never()).createIcebergTableWithOnlyMetadataColumn(anyString(), anyString());
  }

  @Test
  void createTableIfNotExists_icebergType_existingIcebergTable_noCreate() {
    SnowflakeConnectionService conn = mock(SnowflakeConnectionService.class);
    when(conn.tableExist("t1")).thenReturn(true);
    when(conn.isIcebergTable("t1")).thenReturn(true);
    SnowflakeSinkServiceV2 svc = newService(conn, cfg(TableType.ICEBERG, ""));

    svc.createTableIfNotExists("t1");

    verify(conn, never()).createIcebergTableWithOnlyMetadataColumn(anyString(), anyString());
    verify(conn, never()).createTableWithOnlyMetadataColumn(anyString());
  }

  @Test
  void createTableIfNotExists_icebergType_existingFdnTable_usedAsIs_noCreate() {
    // table.type only governs auto-creation; an existing table is used as-is regardless of type.
    SnowflakeConnectionService conn = mock(SnowflakeConnectionService.class);
    when(conn.tableExist("t1")).thenReturn(true);
    when(conn.isIcebergTable("t1")).thenReturn(false);
    SnowflakeSinkServiceV2 svc = newService(conn, cfg(TableType.ICEBERG, ""));

    svc.createTableIfNotExists("t1");

    verify(conn, never()).createIcebergTableWithOnlyMetadataColumn(anyString(), anyString());
    verify(conn, never()).createTableWithOnlyMetadataColumn(anyString());
  }

  @Test
  void createTableIfNotExists_snowflakeType_existingIcebergTable_usedAsIs_noCreate() {
    SnowflakeConnectionService conn = mock(SnowflakeConnectionService.class);
    when(conn.tableExist("t1")).thenReturn(true);
    when(conn.isIcebergTable("t1")).thenReturn(true);
    SnowflakeSinkServiceV2 svc = newService(conn, cfg(TableType.SNOWFLAKE, ""));

    svc.createTableIfNotExists("t1");

    verify(conn, never()).createTableWithOnlyMetadataColumn(anyString());
    verify(conn, never()).createIcebergTableWithOnlyMetadataColumn(anyString(), anyString());
  }

  @Test
  void createTableIfNotExists_icebergType_nonSchematized_withoutV3_throws() {
    SnowflakeConnectionService conn = mock(SnowflakeConnectionService.class);
    when(conn.tableExist("t1")).thenReturn(false);
    SinkTaskConfig config =
        SinkTaskConfigTestBuilder.builder()
            .connectorName(CONNECTOR_NAME)
            .taskId("0")
            .tableType(TableType.ICEBERG)
            .enableSchematization(false)
            .icebergCreateTableOptions(Optional.of("EXTERNAL_VOLUME='v'"))
            .build();
    SnowflakeSinkServiceV2 svc = newService(conn, config);

    assertThatThrownBy(() -> svc.createTableIfNotExists("t1"))
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("ICEBERG_VERSION=3");
    verify(conn, never()).createIcebergTableWithOnlyMetadataColumn(anyString(), anyString());
  }

  @Test
  void createTableIfNotExists_icebergType_nonSchematized_withV3_creates() {
    SnowflakeConnectionService conn = mock(SnowflakeConnectionService.class);
    when(conn.tableExist("t1")).thenReturn(false);
    SinkTaskConfig config =
        SinkTaskConfigTestBuilder.builder()
            .connectorName(CONNECTOR_NAME)
            .taskId("0")
            .tableType(TableType.ICEBERG)
            .enableSchematization(false)
            .icebergCreateTableOptions(Optional.of("ICEBERG_VERSION=3"))
            .build();
    SnowflakeSinkServiceV2 svc = newService(conn, config);

    svc.createTableIfNotExists("t1");

    verify(conn).createIcebergTableWithOnlyMetadataColumn("t1", "ICEBERG_VERSION=3");
  }

  private SnowflakeSinkServiceV2 buildService(
      SnowflakeConnectionService conn, boolean clientValidationEnabled) {
    return buildService(conn, clientValidationEnabled, mock(PartitionChannelManager.class));
  }

  private SnowflakeSinkServiceV2 buildService(
      SnowflakeConnectionService conn,
      boolean clientValidationEnabled,
      PartitionChannelManager channelManager) {
    SinkTaskConfig config =
        SinkTaskConfigTestBuilder.builder()
            .connectorName(CONNECTOR_NAME)
            .taskId("0")
            .validation(
                clientValidationEnabled
                    ? SnowflakeValidation.CLIENT_SIDE
                    : SnowflakeValidation.SERVER_SIDE)
            .enableSanitization(false)
            .build();
    return new SnowflakeSinkServiceV2(
        conn,
        config,
        mockSinkTaskContext,
        Optional.empty(),
        () -> mock(BatchOffsetFetcher.class),
        () -> channelManager,
        TaskMetrics.noop());
  }

  private static TopicPartitionChannel mockChannel(String channelName, boolean initializing) {
    TopicPartitionChannel channel = mock(TopicPartitionChannel.class);
    when(channel.getChannelName()).thenReturn(channelName);
    when(channel.isInitializing()).thenReturn(initializing);
    when(channel.isChannelClosed()).thenReturn(false);
    when(channel.insertRecord(any())).thenReturn(true);
    return channel;
  }

  private static SinkRecord recordFor(String topic, int partition, long offset) {
    return SinkRecordBuilder.forTopicPartition(topic, partition).withOffset(offset).build();
  }
}
