package com.snowflake.kafka.connector.internal.telemetry;

import static com.snowflake.kafka.connector.internal.telemetry.TelemetryConstants.FILE_COUNT_REPROCESS_PURGE;
import static com.snowflake.kafka.connector.internal.telemetry.TelemetryConstants.FILE_COUNT_RESTART;
import static com.snowflake.kafka.connector.internal.telemetry.TelemetryConstants.IS_REUSE_PIPE;
import static com.snowflake.kafka.connector.internal.telemetry.TelemetryConstants.IS_REUSE_STAGE;
import static com.snowflake.kafka.connector.internal.telemetry.TelemetryConstants.IS_REUSE_TABLE;
import static com.snowflake.kafka.connector.internal.telemetry.TelemetryConstants.PIPE_NAME;
import static com.snowflake.kafka.connector.internal.telemetry.TelemetryConstants.STAGE_NAME;
import static com.snowflake.kafka.connector.internal.telemetry.TelemetryConstants.START_TIME;
import static com.snowflake.kafka.connector.internal.telemetry.TelemetryConstants.TABLE_NAME;

import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.node.ObjectNode;

/**
 * This object is send only once when pipe starts No concurrent modification is made on this object,
 * thus no lock is required.
 */
public class SnowflakeTelemetryPipeCreation extends SnowflakeTelemetryBasicInfo {
  boolean isReuseTable = false; // is the create reusing existing table
  boolean isReuseStage = false; // is the create reusing existing stage
  boolean isReusePipe = false; // is the create reusing existing pipe
  int fileCountRestart = 0; // files on stage when cleaner starts
  int fileCountReprocessPurge =
      0; // files on stage that are purged due to reprocessing when cleaner starts
  long startTime; // start time of the pipe
  private final String stageName;
  private final String pipeName;

  public SnowflakeTelemetryPipeCreation(
      final String tableName, final String stageName, final String pipeName) {
    super(tableName, SnowflakeTelemetryService.TelemetryType.KAFKA_PIPE_START);
    this.stageName = stageName;
    this.pipeName = pipeName;
    this.startTime = System.currentTimeMillis();
  }

  @Override
  public void dumpTo(ObjectNode msg) {
    msg.put(TABLE_NAME, tableName);
    msg.put(STAGE_NAME, stageName);
    msg.put(PIPE_NAME, pipeName);

    msg.put(IS_REUSE_TABLE, isReuseTable);
    msg.put(IS_REUSE_STAGE, isReuseStage);
    msg.put(IS_REUSE_PIPE, isReusePipe);
    msg.put(FILE_COUNT_RESTART, fileCountRestart);
    msg.put(FILE_COUNT_REPROCESS_PURGE, fileCountReprocessPurge);
    msg.put(START_TIME, startTime);
  }

  @Override
  public boolean isEmpty() {
    throw new IllegalStateException(
        "Empty function doesnt apply to:" + this.getClass().getSimpleName());
  }

  public void setReuseTable(boolean reuseTable) {
    isReuseTable = reuseTable;
  }

  public void setReuseStage(boolean reuseStage) {
    isReuseStage = reuseStage;
  }

  public void setReusePipe(boolean reusePipe) {
    isReusePipe = reusePipe;
  }

  public void setFileCountRestart(int fileCountRestart) {
    this.fileCountRestart = fileCountRestart;
  }

  public void setFileCountReprocessPurge(int fileCountReprocessPurge) {
    this.fileCountReprocessPurge = fileCountReprocessPurge;
  }

  public void setStartTime(long startTime) {
    this.startTime = startTime;
  }
}
