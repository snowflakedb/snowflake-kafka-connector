package com.snowflake.kafka.connector.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyList;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.anyMap;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig;
import com.snowflake.kafka.connector.internal.streaming.IngestionMethodConfig;
import com.snowflake.kafka.connector.internal.telemetry.SnowflakeTelemetryBasicInfo;
import com.snowflake.kafka.connector.internal.telemetry.SnowflakeTelemetryPipeCreation;
import com.snowflake.kafka.connector.internal.telemetry.SnowflakeTelemetryPipeStatus;
import com.snowflake.kafka.connector.internal.telemetry.SnowflakeTelemetryService;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.node.ObjectNode;
import net.snowflake.client.jdbc.internal.joda.time.DateTime;
import net.snowflake.client.jdbc.internal.joda.time.DateTimeZone;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

class StageFilesProcessorTest {

  private static final String PIPE_NAME = "testPipe";
  private static final String TABLE_NAME = "testTable";
  private static final String STAGE_NAME = "testStage";
  private static final String PREFIX = "prefix";

  private SnowflakeConnectionService conn;
  private SnowflakeIngestionService ingestionService;
  private AtomicLong currentTime;
  private StageFilesProcessor victim;
  private StageFilesProcessor.ProgressRegisterImpl register;
  private SnowflakeTelemetryPipeCreation pipeCreation;
  private SnowflakeTelemetryPipeStatus pipeTelemetry;
  private FakeSnowflakeTelemetryService telemetryService;
  private PipeProgressRegistryTelemetry telemetry;
  private AtomicReference<BiConsumer<Integer, Long>> nextTickCallback;
  private AtomicReference<ScheduledFuture<?>> scheduledFuture;

  @BeforeEach
  void setUp() {
    currentTime = new AtomicLong(new DateTime(2000, 1, 10, 12, 0, DateTimeZone.UTC).getMillis());
    nextTickCallback = new AtomicReference<>();
    scheduledFuture = new AtomicReference<>();

    conn = Mockito.mock(SnowflakeConnectionService.class);
    ingestionService = Mockito.mock(SnowflakeIngestionService.class);
    pipeCreation = new SnowflakeTelemetryPipeCreation(TABLE_NAME, STAGE_NAME, PIPE_NAME);
    pipeCreation = new SnowflakeTelemetryPipeCreation(TABLE_NAME, STAGE_NAME, PIPE_NAME);
    pipeTelemetry =
        new SnowflakeTelemetryPipeStatus(TABLE_NAME, STAGE_NAME, PIPE_NAME, 0, false, null);
    telemetryService = new FakeSnowflakeTelemetryService();
    telemetry = new PipeProgressRegistryTelemetry(pipeCreation, pipeTelemetry, telemetryService);
  }

  private void createFileProcessor(int ticks) {
    victim =
        new StageFilesProcessor(
            PIPE_NAME,
            TABLE_NAME,
            STAGE_NAME,
            PREFIX,
            "topic",
            0,
            conn,
            ingestionService,
            pipeTelemetry,
            telemetryService,
            createTestScheduler(ticks, currentTime, nextTickCallback, scheduledFuture),
            currentTime::get,
            SnowflakeSinkConnectorConfig.SNOWPIPE_FILE_CLEANER_INTERVAL_SECONDS_DEFAULT);
    register = new StageFilesProcessor.ProgressRegisterImpl(victim);
  }

  @Test
  void fileProcessor_WillTerminateOnStopSignal() {
    createFileProcessor(100);
    StageFilesProcessor.ProgressRegister client = victim.trackFilesAsync();
    client.close();
    verify(scheduledFuture.get(), times(1)).cancel(true);
  }

  @Test
  void fileProcessor_WillTryToGetInitialStateFromStage_ExactlyOnce() {
    // lets simulate 1 hour (60 cleanup cycles)
    createFileProcessor(60);
    configureInitialState();

    when(conn.listStage(STAGE_NAME, PREFIX)).thenReturn(new ArrayList<>());

    victim.trackFiles(register, telemetry);

    assertThat(telemetryService.records).hasSize(61);
    List<SnowflakeTelemetryPipeStatus> statuses =
        telemetryService.records.stream()
            .filter(r -> r instanceof SnowflakeTelemetryPipeStatus)
            .map(SnowflakeTelemetryPipeStatus.class::cast)
            .collect(Collectors.toList());
    assertThat(statuses).hasSize(60);

    verify(conn, times(1)).listStage(STAGE_NAME, PREFIX);
    assertInitialStateWasConfigured();
  }

  @Test
  void fileProcessor_WillTryGettingStateFromStage_OnError() {
    createFileProcessor(10);
    configureInitialState();

    String ingestFile = String.format("connector/topic/0/1_9_%d.json.gz", currentTime.get());
    register.registerNewStageFile(ingestFile);

    when(conn.listStage(STAGE_NAME, PREFIX)).thenReturn(new ArrayList<>());
    when(ingestionService.readIngestHistoryForward(anyMap(), any(), any(), anyInt()))
        .thenReturn(0)
        .thenThrow(new SnowflakeKafkaConnectorException("expected", "error"))
        .thenReturn(0);

    victim.trackFiles(register, telemetry);

    verify(conn, times(2)).listStage(STAGE_NAME, PREFIX);
    verify(ingestionService, times(10)).readIngestHistoryForward(anyMap(), any(), any(), anyInt());
    assertInitialStateWasConfigured();
  }

  @Test
  void fileProcessor_WillPurgeLoadedFiles_WhenHistoryIsAvailable_AfterFilesHaveBeenSubmitted() {
    createFileProcessor(10);
    configureInitialState();

    String file1 = String.format("connector/topic/0/1_9_%d.json.gz", currentTime.get());
    String file2 = String.format("connector/topic/0/10_19_%d.json.gz", currentTime.get());
    String file3 = String.format("connector/topic/0/20_29_%d.json.gz", currentTime.get());
    register.registerNewStageFile(file1);
    register.registerNewStageFile(file2);
    register.registerNewStageFile(file3);

    when(conn.listStage(STAGE_NAME, PREFIX)).thenReturn(new ArrayList<>());
    when(ingestionService.readIngestHistoryForward(anyMap(), any(), any(), anyInt()))
        .thenAnswer(
            a -> {
              Map<String, InternalUtils.IngestedFileStatus> report = a.getArgument(0);
              report.put(file1, InternalUtils.IngestedFileStatus.LOADED);
              return 1;
            })
        .thenAnswer(
            a -> {
              Map<String, InternalUtils.IngestedFileStatus> report = a.getArgument(0);
              report.put(file2, InternalUtils.IngestedFileStatus.LOADED);
              return 1;
            })
        .thenAnswer(
            a -> {
              Map<String, InternalUtils.IngestedFileStatus> report = a.getArgument(0);
              report.put(file3, InternalUtils.IngestedFileStatus.LOADED);
              return 1;
            })
        .thenThrow(new IllegalStateException("ups!"));

    List<String> purgedFiles = new ArrayList<>();
    doAnswer(
            a -> {
              purgedFiles.addAll(a.getArgument(1));
              return null;
            })
        .when(conn)
        .purgeStage(anyString(), anyList());

    victim.trackFiles(register, telemetry);

    verify(conn, times(1)).listStage(STAGE_NAME, PREFIX);
    verify(ingestionService, times(3)).readIngestHistoryForward(anyMap(), any(), any(), anyInt());
    verify(conn, times(3)).purgeStage(anyString(), anyList());
    assertThat(purgedFiles).containsOnly(file1, file2, file3);
    assertInitialStateWasConfigured();
  }

  @Test
  void fileProcessor_WillPurgeLoadedFiles_WhenHistoryIsFetchedSooner_ThanFilesAreRegistered() {
    createFileProcessor(10);
    configureInitialState();

    String file1 = String.format("connector/topic/0/1_9_%d.json.gz", currentTime.get());
    String file2 = String.format("connector/topic/0/10_19_%d.json.gz", currentTime.get());
    String file3 = String.format("connector/topic/0/20_29_%d.json.gz", currentTime.get());

    nextTickCallback.set(
        (run, timer) -> {
          if (run == 1) {
            register.registerNewStageFile(file1);
          }
          if (run == 2) {
            register.registerNewStageFile(file2);
          }
          if (run == 3) {
            register.registerNewStageFile(file3);
          }
        });

    when(conn.listStage(STAGE_NAME, PREFIX)).thenReturn(new ArrayList<>());
    when(ingestionService.readIngestHistoryForward(anyMap(), any(), any(), anyInt()))
        .thenAnswer(
            a -> {
              Map<String, InternalUtils.IngestedFileStatus> report = a.getArgument(0);
              report.put(file1, InternalUtils.IngestedFileStatus.LOADED);
              report.put(file2, InternalUtils.IngestedFileStatus.LOADED);
              report.put(file3, InternalUtils.IngestedFileStatus.LOADED);
              return 3;
            })
        .thenAnswer(a -> 0)
        .thenAnswer(a -> 0)
        .thenThrow(new IllegalStateException("ups!"));

    List<String> purgedFiles = new ArrayList<>();
    doAnswer(
            a -> {
              purgedFiles.addAll(a.getArgument(1));
              return null;
            })
        .when(conn)
        .purgeStage(anyString(), anyList());

    victim.trackFiles(register, telemetry);

    verify(conn, times(1)).listStage(STAGE_NAME, PREFIX);
    verify(ingestionService, times(3)).readIngestHistoryForward(anyMap(), any(), any(), anyInt());
    verify(conn, times(3)).purgeStage(anyString(), anyList());
    assertThat(purgedFiles).containsOnly(file1, file2, file3);
    assertInitialStateWasConfigured();
  }

  @Test
  void fileProcessor_WillMoveOldFilesToTableStage() {
    createFileProcessor(60);
    configureInitialState();

    String file1 = String.format("connector/topic/0/1_9_%d.json.gz", currentTime.get());
    String file2 = String.format("connector/topic/0/10_19_%d.json.gz", currentTime.get());
    String file3 = String.format("connector/topic/0/20_29_%d.json.gz", currentTime.get());
    register.registerNewStageFile(file1);
    register.registerNewStageFile(file2);
    register.registerNewStageFile(file3);

    when(conn.listStage(STAGE_NAME, PREFIX)).thenReturn(new ArrayList<>());
    // no report for request files
    when(ingestionService.readIngestHistoryForward(anyMap(), any(), any(), anyInt())).thenReturn(0);
    // ... nor any history entry in the past one hour
    when(ingestionService.readOneHourHistory(anyList(), anyLong())).thenReturn(new HashMap<>());
    // we should not purge stage when file is not LOADED
    doNothing().when(conn).purgeStage(anyString(), anyList());
    ArgumentCaptor<List<String>> failedFiles = ArgumentCaptor.forClass(List.class);
    doNothing().when(conn).moveToTableStage(anyString(), anyString(), failedFiles.capture());

    victim.trackFiles(register, telemetry);

    verify(conn, times(1)).listStage(STAGE_NAME, PREFIX);
    verify(ingestionService, times(59)).readIngestHistoryForward(anyMap(), any(), any(), anyInt());
    verify(conn, times(0)).purgeStage(anyString(), anyList());
    verify(ingestionService, times(50)).readOneHourHistory(anyList(), anyLong());
    // when files get to the 1 hour age, they will be automatically marked as failed and moved to
    // table stage
    verify(conn, times(3)).moveToTableStage(anyString(), anyString(), failedFiles.capture());
    assertThat(failedFiles.getAllValues().stream().flatMap(Collection::stream))
        .containsOnly(file1, file2, file3);
    assertInitialStateWasConfigured();
  }

  @Test
  void fileProcessor_WillMoveFailedFilesToStageEvenWhenOneFails() {
    createFileProcessor(61);
    configureInitialState();

    String file1 = String.format("connector/topic/0/1_9_%d.json.gz", currentTime.get());
    String file2 = String.format("connector/topic/0/10_19_%d.json.gz", currentTime.get());
    String file3 = String.format("connector/topic/0/20_29_%d.json.gz", currentTime.get());
    register.registerNewStageFile(file1);
    register.registerNewStageFile(file2);
    register.registerNewStageFile(file3);

    when(conn.listStage(STAGE_NAME, PREFIX)).thenReturn(new ArrayList<>());
    // no report for request files
    when(ingestionService.readIngestHistoryForward(anyMap(), any(), any(), anyInt()))
        .thenAnswer(
            a -> {
              Map<String, InternalUtils.IngestedFileStatus> report = a.getArgument(0);
              report.put(file1, InternalUtils.IngestedFileStatus.FAILED);
              report.put(file2, InternalUtils.IngestedFileStatus.FAILED);
              report.put(file3, InternalUtils.IngestedFileStatus.FAILED);
              return 3;
            });
    // ... nor any history entry in the past one hour
    when(ingestionService.readOneHourHistory(anyList(), anyLong())).thenReturn(new HashMap<>());
    // we should not purge stage when file is not LOADED
    doNothing().when(conn).purgeStage(anyString(), anyList());
    ArgumentCaptor<List<String>> failedFiles = ArgumentCaptor.forClass(List.class);
    doThrow(new SnowflakeKafkaConnectorException("ups", "123"))
        .doNothing()
        .doNothing()
        .when(conn)
        .moveToTableStage(anyString(), anyString(), failedFiles.capture());

    victim.trackFiles(register, telemetry);

    verify(conn, times(1)).listStage(STAGE_NAME, PREFIX);
    verify(ingestionService, times(1)).readIngestHistoryForward(anyMap(), any(), any(), anyInt());
    verify(conn, times(0)).purgeStage(anyString(), anyList());
    verify(ingestionService, times(0)).readOneHourHistory(anyList(), anyLong());
    verify(conn, times(3)).moveToTableStage(anyString(), anyString(), failedFiles.capture());
    assertThat(failedFiles.getAllValues().stream().flatMap(Collection::stream))
        .containsOnly(file1, file2, file3);

    assertInitialStateWasConfigured();
  }

  @Test
  void fileProcessor_WillTreatFreshStaleFileAsStageOne() {
    createFileProcessor(61);
    configureInitialState();

    String file1 =
        String.format(
            "connector/topic/0/1_9_%d.json.gz",
            currentTime.get() + Duration.ofSeconds(65L).toMillis());
    register.registerNewStageFile(file1);
    register.newOffset(Long.MIN_VALUE);
    nextTickCallback.set(
        (run, time) -> {
          if (run == 1) {
            register.newOffset(Long.MAX_VALUE);
          }
        });

    when(conn.listStage(STAGE_NAME, PREFIX)).thenReturn(new ArrayList<>());
    // no report for request files
    when(ingestionService.readIngestHistoryForward(anyMap(), any(), any(), anyInt())).thenReturn(0);
    // ... nor any history entry in the past one hour
    when(ingestionService.readOneHourHistory(anyList(), anyLong())).thenReturn(new HashMap<>());
    // we should not purge stage when file is not LOADED
    doNothing().when(conn).purgeStage(anyString(), anyList());
    ArgumentCaptor<List<String>> failedFiles = ArgumentCaptor.forClass(List.class);
    doNothing().when(conn).moveToTableStage(anyString(), anyString(), failedFiles.capture());

    victim.trackFiles(register, telemetry);

    verify(conn, times(1)).listStage(STAGE_NAME, PREFIX);
    verify(ingestionService, times(60)).readIngestHistoryForward(anyMap(), any(), any(), anyInt());
    verify(conn, times(0)).purgeStage(anyString(), anyList());
    verify(ingestionService, times(50)).readOneHourHistory(anyList(), anyLong());
    // when files get to the 1 hour age, they will be automatically marked as failed and moved to
    // table stage
    verify(conn, times(1)).moveToTableStage(anyString(), anyString(), failedFiles.capture());
    assertThat(failedFiles.getValue()).containsOnly(file1);
    assertInitialStateWasConfigured();
  }

  @Test
  void fileProcessor_WillProperlyHandleStaleFiles() {
    createFileProcessor(13);
    configureInitialState();

    String fileOk = String.format("connector/topic/0/10_19_%d.json.gz", currentTime.get());
    String fileFailed = String.format("connector/topic/0/20_29_%d.json.gz", currentTime.get());
    currentTime.addAndGet(Duration.ofMinutes(1).toMillis());
    register.registerNewStageFile(fileOk);
    register.registerNewStageFile(fileFailed);

    when(conn.listStage(STAGE_NAME, PREFIX)).thenReturn(new ArrayList<>());
    // no report for request files
    when(ingestionService.readIngestHistoryForward(anyMap(), any(), any(), anyInt())).thenReturn(0);
    // ... nor any history entry in the past one hour
    when(ingestionService.readOneHourHistory(anyList(), anyLong()))
        // no report in first cycle
        .thenReturn(new HashMap<>())
        // first file was sucessfully processed, but the second one - still in progress
        .thenReturn(Collections.singletonMap(fileOk, InternalUtils.IngestedFileStatus.LOADED))
        // second file eventually failed
        .thenReturn(Collections.singletonMap(fileFailed, InternalUtils.IngestedFileStatus.FAILED));

    // we should purge 1 file and move other to table stage
    ArgumentCaptor<List<String>> loadedFiles = ArgumentCaptor.forClass(List.class);
    doNothing().when(conn).purgeStage(anyString(), loadedFiles.capture());
    ArgumentCaptor<List<String>> failedFiles = ArgumentCaptor.forClass(List.class);
    doNothing().when(conn).moveToTableStage(anyString(), anyString(), failedFiles.capture());

    victim.trackFiles(register, telemetry);

    verify(conn, times(1)).listStage(STAGE_NAME, PREFIX);
    verify(ingestionService, times(10)).readIngestHistoryForward(anyMap(), any(), any(), anyInt());
    verify(conn, times(1)).purgeStage(anyString(), loadedFiles.capture());
    verify(ingestionService, times(3)).readOneHourHistory(failedFiles.capture(), anyLong());
    // when files get to the 1 hour age, they will be automatically marked as failed and moved to
    // table stage
    verify(conn, times(1)).moveToTableStage(anyString(), anyString(), failedFiles.capture());

    assertThat(failedFiles.getValue()).containsOnly(fileFailed);
    assertThat(loadedFiles.getValue()).containsOnly(fileOk);
    assertInitialStateWasConfigured();
  }

  @Test
  void fileProcessor_WillDeleteDirtyFiles() {
    createFileProcessor(10);
    configureInitialState();

    String file1 = String.format("connector/topic/0/100_199_%d.json.gz", currentTime.get());
    String file2 = String.format("connector/topic/0/200_299_%d.json.gz", currentTime.get());
    String file3 = String.format("connector/topic/0/300_399_%d.json.gz", currentTime.get());

    // reset offset before first file
    register.newOffset(99L);
    register.registerNewStageFile(file1);
    register.registerNewStageFile(file2);
    register.registerNewStageFile(file3);

    when(conn.listStage(STAGE_NAME, PREFIX)).thenReturn(new ArrayList<>());
    ArgumentCaptor<List<String>> purgedFiles = ArgumentCaptor.forClass(List.class);
    doNothing().when(conn).purgeStage(anyString(), purgedFiles.capture());

    victim.trackFiles(register, telemetry);

    verify(conn, times(1)).listStage(STAGE_NAME, PREFIX);
    verify(conn, times(1)).purgeStage(anyString(), purgedFiles.capture());
    assertThat(purgedFiles.getValue()).containsOnly(file1, file2, file3);

    assertInitialStateWasConfigured();
  }

  @Test
  void fileProcessor_WillCleanHistoryEntries_OlderThanOneHour() {
    createFileProcessor(61);
    configureInitialState();

    String file1 = String.format("connector/topic/0/1_9_%d.json.gz", currentTime.get());
    String file2 = String.format("connector/topic/0/10_19_%d.json.gz", currentTime.get());
    String file3 = String.format("connector/topic/0/20_29_%d.json.gz", currentTime.get());
    register.registerNewStageFile(file1);

    when(conn.listStage(STAGE_NAME, PREFIX)).thenReturn(new ArrayList<>());
    when(ingestionService.readIngestHistoryForward(anyMap(), any(), any(), anyInt()))
        .thenAnswer(
            a -> {
              // load history for 3 files, but 2 were available in previous run and have been moved
              // from stage already
              Map<String, InternalUtils.IngestedFileStatus> report = a.getArgument(0);
              report.put(file1, InternalUtils.IngestedFileStatus.LOADED);
              report.put(file2, InternalUtils.IngestedFileStatus.LOADED);
              report.put(file3, InternalUtils.IngestedFileStatus.LOADED);
              return 3;
            })
        .thenAnswer(a -> 0);

    List<String> purgedFiles = new ArrayList<>();
    doAnswer(
            a -> {
              purgedFiles.addAll(a.getArgument(1));
              return null;
            })
        .when(conn)
        .purgeStage(anyString(), anyList());

    victim.trackFiles(register, telemetry);

    verify(conn, times(1)).listStage(STAGE_NAME, PREFIX);
    verify(ingestionService, times(1)).readIngestHistoryForward(anyMap(), any(), any(), anyInt());
    verify(conn, times(1)).purgeStage(anyString(), anyList());
    assertThat(purgedFiles).containsOnly(file1);
    // check if these old not matched files have been removed from history and do not result in
    // memory leak
    assertThat(register.currentProcessorContext.ingestHistory).isEmpty();
    assertInitialStateWasConfigured();
  }

  private void configureInitialState() {
    when(conn.tableExist(TABLE_NAME)).thenReturn(true);
    when(conn.isTableCompatible(TABLE_NAME)).thenReturn(true);
    when(conn.stageExist(STAGE_NAME)).thenReturn(true);
    when(conn.isStageCompatible(STAGE_NAME)).thenReturn(true);
    when(conn.pipeExist(PIPE_NAME)).thenReturn(true);
    when(conn.isPipeCompatible(TABLE_NAME, STAGE_NAME, PIPE_NAME)).thenReturn(true);
  }

  private void assertInitialStateWasConfigured() {
    verify(conn, times(1)).tableExist(TABLE_NAME);
    verify(conn, times(1)).isTableCompatible(TABLE_NAME);
    verify(conn, times(1)).stageExist(STAGE_NAME);
    verify(conn, times(1)).isStageCompatible(STAGE_NAME);
    verify(conn, times(1)).pipeExist(PIPE_NAME);
    verify(conn, times(1)).isPipeCompatible(TABLE_NAME, STAGE_NAME, PIPE_NAME);
  }

  static ScheduledExecutorService createTestScheduler(
      int ticks,
      AtomicLong currentTime,
      AtomicReference<BiConsumer<Integer, Long>> nextTickCallback,
      AtomicReference<ScheduledFuture<?>> scheduledTaskReference) {
    ScheduledExecutorService service = mock(ScheduledExecutorService.class);
    AtomicInteger maxLoops = new AtomicInteger(ticks);
    when(service.scheduleWithFixedDelay(any(Runnable.class), anyLong(), anyLong(), any()))
        .thenAnswer(
            a -> {
              Runnable task = a.getArgument(0);
              Long initialDelay = a.getArgument(1);
              Long period = a.getArgument(2);
              TimeUnit unit = a.getArgument(3);
              currentTime.addAndGet(unit.toMillis(initialDelay));
              int run = 0;
              while (maxLoops.getAndDecrement() > 0) {
                currentTime.addAndGet(unit.toMillis(period));
                BiConsumer<Integer, Long> runCallback = nextTickCallback.get();
                if (null != runCallback) {
                  runCallback.accept(run++, currentTime.get());
                }
                task.run();
              }
              ScheduledFuture<?> result = mock(ScheduledFuture.class);

              if (null != scheduledTaskReference) {
                scheduledTaskReference.set(result);
              }

              return result;
            });
    return service;
  }

  class FakeSnowflakeTelemetryService extends SnowflakeTelemetryService {

    List<Object> records = new ArrayList<Object>();

    @Override
    public ObjectNode getObjectNode() {
      return getDefaultObjectNode(IngestionMethodConfig.SNOWPIPE);
    }

    @Override
    public void reportKafkaConnectFatalError(String errorDetail) {
      records.add(errorDetail);
    }

    @Override
    public void reportKafkaPartitionUsage(
        SnowflakeTelemetryBasicInfo partitionStatus, boolean isClosing) {
      records.add(partitionStatus);
    }

    @Override
    public void reportKafkaPartitionStart(SnowflakeTelemetryBasicInfo partitionCreation) {
      records.add(partitionCreation);
    }
  }
}
