package com.snowflake.kafka.connector.internal;

import com.snowflake.kafka.connector.internal.telemetry.SnowflakeTelemetryPipeCreation;
import com.snowflake.kafka.connector.internal.telemetry.SnowflakeTelemetryPipeStatus;
import com.snowflake.kafka.connector.internal.telemetry.SnowflakeTelemetryService;

/** Container class for pipe specific metrics. Wraps reporting behind simple method calls. */
class PipeProgressRegistryTelemetry {
  private final SnowflakeTelemetryPipeCreation pipeCreation;
  private final SnowflakeTelemetryPipeStatus pipeTelemetry;
  private final SnowflakeTelemetryService telemetryService;

  public PipeProgressRegistryTelemetry(
      SnowflakeTelemetryPipeCreation pipeCreation,
      SnowflakeTelemetryPipeStatus pipeTelemetry,
      SnowflakeTelemetryService telemetryService) {
    this.pipeCreation = pipeCreation;
    this.pipeTelemetry = pipeTelemetry;
    this.telemetryService = telemetryService;
  }

  public void reportKafkaPartitionStart() {
    telemetryService.reportKafkaPartitionUsage(pipeCreation, false);
  }

  public void reportKafkaPartitionUsage() {
    telemetryService.reportKafkaPartitionUsage(pipeTelemetry, false);
  }

  public void reportKafkaConnectFatalError(String message) {
    telemetryService.reportKafkaConnectFatalError(message);
  }

  public void setupInitialState(long remoteFileCount) {
    pipeTelemetry.addAndGetFileCountOnStage(remoteFileCount);
    pipeTelemetry.addAndGetFileCountOnIngestion(remoteFileCount);
  }

  public void updateStatsAfterError(int dirtyFilesCount, int stageFilesCount) {
    pipeCreation.setFileCountReprocessPurge(dirtyFilesCount);
    pipeCreation.setFileCountRestart(stageFilesCount);
  }

  public void notifyFilesPurged(long maxOffset, int purgedFilesCount) {
    pipeTelemetry.setPurgedOffsetAtomically(value -> Math.max(value, maxOffset));
    pipeTelemetry.addAndGetFileCountOnStage(-purgedFilesCount);
    pipeTelemetry.addAndGetFileCountOnIngestion(-purgedFilesCount);
    pipeTelemetry.addAndGetFileCountPurged(purgedFilesCount);
  }

  public void notifyFileIngestLag(String fileName, long lag) {
    pipeTelemetry.updateIngestionLag(System.currentTimeMillis() - lag);
  }

  public void notifyFilesDeleted(int deletedFilesCount) {
    pipeTelemetry.addAndGetFileCountOnStage(-deletedFilesCount);
    pipeTelemetry.addAndGetFileCountOnIngestion(-deletedFilesCount);
    pipeTelemetry.updateFailedIngestionMetrics(deletedFilesCount);
  }
}
