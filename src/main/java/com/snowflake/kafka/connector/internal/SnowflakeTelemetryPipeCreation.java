package com.snowflake.kafka.connector.internal;

import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.node.ObjectNode;

import java.util.concurrent.atomic.AtomicLong;

// This object is send only once when pipe starts
// No concurrent modification is made on this object, thus no lock is required.
public class SnowflakeTelemetryPipeCreation extends SnowflakeTelemetryBasicInfo {
  boolean isReuseTable = false;                  // is the create reusing existing table
  boolean isReuseStage = false;                  // is the create reusing existing stage
  boolean isReusePipe = false;                   // is the create reusing existing pipe
  int fileCountRestart = 0;                      // files on stage when cleaner starts
  int fileCountReprocessPurge = 0;               // files on stage that are purged due to reprocessing when cleaner starts
  long startTime;                                // start time of the pipe
  static final String START_TIME                 = "start_time";
  static final String IS_REUSE_TABLE             = "is_reuse_table";
  static final String IS_REUSE_STAGE             = "is_reuse_stage";
  static final String IS_REUSE_PIPE              = "is_reuse_pipe";
  static final String FILE_COUNT_RESTART         = "file_count_restart";
  static final String FILE_COUNT_REPROCESS_PURGE = "file_count_reprocess_purge";

  SnowflakeTelemetryPipeCreation(final String tableName, final String stageName, final String pipeName)
  {
    super(tableName, stageName, pipeName);
    this.startTime = System.currentTimeMillis();
  }

  @Override
  void dumpTo(ObjectNode msg)
  {
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
}
