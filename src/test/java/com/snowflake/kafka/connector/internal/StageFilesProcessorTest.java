package com.snowflake.kafka.connector.internal;

import com.snowflake.kafka.connector.internal.telemetry.SnowflakeTelemetryPipeCreation;
import com.snowflake.kafka.connector.internal.telemetry.SnowflakeTelemetryPipeStatus;
import com.snowflake.kafka.connector.internal.telemetry.SnowflakeTelemetryService;
import net.snowflake.client.jdbc.internal.joda.time.DateTime;
import net.snowflake.client.jdbc.internal.joda.time.DateTimeZone;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.concurrent.ForkJoinPool.commonPool;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyList;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.anyMap;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
  private SnowflakeTelemetryService telemetryService;

  private ProgressRegistryTelemetry telemetry;

  @BeforeEach
  void setUp() {
    currentTime = new AtomicLong(new DateTime(2000, 1, 10, 12, 0, DateTimeZone.UTC).getMillis());

    conn = Mockito.mock(SnowflakeConnectionService.class);
    ingestionService = Mockito.mock(SnowflakeIngestionService.class);
    pipeCreation = new SnowflakeTelemetryPipeCreation(TABLE_NAME, STAGE_NAME, PIPE_NAME);
    pipeTelemetry = Mockito.mock(SnowflakeTelemetryPipeStatus.class);
    telemetryService = Mockito.mock(SnowflakeTelemetryService.class);
    telemetry = new ProgressRegistryTelemetry(pipeCreation, pipeTelemetry, telemetryService);

    victim =
        new StageFilesProcessor(
            PIPE_NAME,
            TABLE_NAME,
            STAGE_NAME,
            PREFIX,
            conn,
            ingestionService,
            pipeTelemetry,
            telemetryService,
            currentTime::get);
    register = new StageFilesProcessor.ProgressRegisterImpl(victim);
  }

  @Test
  void filesProcessWillTerminateOnStopSignal() throws InterruptedException {
    AtomicBoolean isStopped = new AtomicBoolean(false);

    commonPool()
        .submit(
            () -> {
              StageFilesProcessor.ProgressRegister client = victim.trackFilesAsync();
              try {
                Thread.sleep(1000L);
              } catch (InterruptedException e) {
                throw new RuntimeException(e);
              }
              client.close();
              isStopped.set(true);
            });

    await().atMost(Duration.ofSeconds(3L)).until(isStopped::get);
  }

  @Test
  void fileProcessorWillTryToGetInitialStateFromStageExactlyOnce() {
    // lets simulate 10 cleanup cycles
    AtomicInteger canRun = new AtomicInteger(10);

    when(conn.listStage(STAGE_NAME, PREFIX)).thenReturn(new ArrayList<>());

    victim.trackFiles(register, telemetry, () -> canRun.getAndDecrement() > 0, d -> {});

    verify(conn, times(1)).listStage(STAGE_NAME, PREFIX);
  }

  @Test
  void fileProcessorWillTryGettingStateFromStageOnError() {
    String ingestFile = String.format("connector/topic/0/1_9_%d.json.gz", currentTime.get());
    register.registerNewStageFile(ingestFile);
    List<String> reportRequest = new ArrayList<>();
    reportRequest.add(ingestFile);

    // lets simulate 3 cleanup cycles
    AtomicInteger canRun = new AtomicInteger(3);

    when(conn.listStage(STAGE_NAME, PREFIX)).thenReturn(new ArrayList<>());
    when(ingestionService.readIngestHistoryForward(anyMap(), any(), any(), anyInt()))
        .thenReturn(0)
        .thenThrow(new SnowflakeKafkaConnectorException("expected", "error"))
        .thenReturn(0);

    victim.trackFiles(register, telemetry, () -> canRun.getAndDecrement() > 0, d -> {});

    verify(conn, times(2)).listStage(STAGE_NAME, PREFIX);
    verify(ingestionService, times(3)).readIngestHistoryForward(anyMap(), any(), any(), anyInt());
  }

  @Test
  void fileProcessorWillPurgeLoadedFiles() {
    String file1 = String.format("connector/topic/0/1_9_%d.json.gz", currentTime.get());
    String file2 = String.format("connector/topic/0/10_19_%d.json.gz", currentTime.get());
    String file3 = String.format("connector/topic/0/20_29_%d.json.gz", currentTime.get());
    register.registerNewStageFile(file1);
    register.registerNewStageFile(file2);
    register.registerNewStageFile(file3);

    // lets simulate 4 cleanup cycles
    AtomicInteger canRun = new AtomicInteger(4);

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

    victim.trackFiles(register, telemetry, () -> canRun.getAndDecrement() > 0, d -> {});

    verify(conn, times(1)).listStage(STAGE_NAME, PREFIX);
    verify(ingestionService, times(3)).readIngestHistoryForward(anyMap(), any(), any(), anyInt());
    verify(conn, times(3)).purgeStage(anyString(), anyList());
    assertThat(purgedFiles).containsOnly(file1, file2, file3);
  }

  @Test
  void fileProcessorWillMoveOldFilesToTableStage() {
    String file1 = String.format("connector/topic/0/1_9_%d.json.gz", currentTime.get());
    String file2 = String.format("connector/topic/0/10_19_%d.json.gz", currentTime.get());
    String file3 = String.format("connector/topic/0/20_29_%d.json.gz", currentTime.get());
    currentTime.addAndGet(Duration.ofMinutes(1).toMillis());
    register.registerNewStageFile(file1);
    register.registerNewStageFile(file2);
    register.registerNewStageFile(file3);

    // lets simulate 4 cleanup cycles
    AtomicInteger canRun = new AtomicInteger(4);

    when(conn.listStage(STAGE_NAME, PREFIX)).thenReturn(new ArrayList<>());
    // no report for request files
    when(ingestionService.readIngestHistoryForward(anyMap(), any(), any(), anyInt())).thenReturn(0);
    // ... nor any history entry in the past one hour
    when(ingestionService.readOneHourHistory(anyList(), anyLong())).thenReturn(new HashMap<>());
    // we should not purge stage when file is not LOADED
    doNothing().when(conn).purgeStage(anyString(), anyList());
    ArgumentCaptor<List<String>> failedFiles = ArgumentCaptor.forClass(List.class);
    doNothing().when(conn).moveToTableStage(anyString(), anyString(), failedFiles.capture());

    victim.trackFiles(
        register,
        telemetry,
        () -> canRun.getAndDecrement() > 0,
        d -> {
          // advance time by 20 minutes - the last check will be 61 minutes after submitting files,
          // so they should be
          // moved to table stage
          currentTime.addAndGet(Duration.ofMinutes(20L).toMillis());
        });

    verify(conn, times(1)).listStage(STAGE_NAME, PREFIX);
    verify(ingestionService, times(3)).readIngestHistoryForward(anyMap(), any(), any(), anyInt());
    verify(conn, times(0)).purgeStage(anyString(), anyList());
    verify(ingestionService, times(2)).readOneHourHistory(anyList(), anyLong());
    // when files get to the 1 hour age, they will be automatically marked as failed and moved to
    // table stage
    verify(conn, times(1)).moveToTableStage(anyString(), anyString(), failedFiles.capture());
    assertThat(failedFiles.getValue()).containsOnly(file1, file2, file3);
  }

  @Test
  void fileProcessorWillProperlyHandleStaleFiles() {
    String fileOk = String.format("connector/topic/0/10_19_%d.json.gz", currentTime.get());
    String fileFailed = String.format("connector/topic/0/20_29_%d.json.gz", currentTime.get());
    currentTime.addAndGet(Duration.ofMinutes(1).toMillis());
    register.registerNewStageFile(fileOk);
    register.registerNewStageFile(fileFailed);

    // lets simulate 4 cleanup cycles
    AtomicInteger canRun = new AtomicInteger(3);

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

    victim.trackFiles(
        register,
        telemetry,
        () -> canRun.getAndDecrement() > 0,
        d -> {
          // advance time by 10 minutes - we will be within 1 hour window
          currentTime.addAndGet(Duration.ofMinutes(10L).toMillis());
        });

    verify(conn, times(1)).listStage(STAGE_NAME, PREFIX);
    verify(ingestionService, times(3)).readIngestHistoryForward(anyMap(), any(), any(), anyInt());
    verify(conn, times(1)).purgeStage(anyString(), loadedFiles.capture());
    verify(ingestionService, times(3)).readOneHourHistory(failedFiles.capture(), anyLong());
    // when files get to the 1 hour age, they will be automatically marked as failed and moved to
    // table stage
    verify(conn, times(1)).moveToTableStage(anyString(), anyString(), failedFiles.capture());

    assertThat(failedFiles.getValue()).containsOnly(fileFailed);
    assertThat(loadedFiles.getValue()).containsOnly(fileOk);
  }

  @Test
  void fileProcessorWillDeleteDirtyFiles() {
    String file1 = String.format("connector/topic/0/100_199_%d.json.gz", currentTime.get());
    String file2 = String.format("connector/topic/0/200_299_%d.json.gz", currentTime.get());
    String file3 = String.format("connector/topic/0/300_399_%d.json.gz", currentTime.get());

    // lets simulate 1 cleanup cycle
    AtomicInteger canRun = new AtomicInteger(1);
    // reset offset before first file
    register.newOffset(100L);
    register.registerNewStageFile(file1);
    register.registerNewStageFile(file2);
    register.registerNewStageFile(file3);

    when(conn.listStage(STAGE_NAME, PREFIX)).thenReturn(new ArrayList<>());
    ArgumentCaptor<List<String>> purgedFiles = ArgumentCaptor.forClass(List.class);
    doNothing().when(conn).purgeStage(anyString(), purgedFiles.capture());

    victim.trackFiles(register, telemetry, () -> canRun.getAndDecrement() > 0, d -> {});

    verify(conn, times(1)).listStage(STAGE_NAME, PREFIX);
    verify(conn, times(1)).purgeStage(anyString(), purgedFiles.capture());
    assertThat(purgedFiles.getValue()).containsOnly(file1, file2, file3);
  }
}
