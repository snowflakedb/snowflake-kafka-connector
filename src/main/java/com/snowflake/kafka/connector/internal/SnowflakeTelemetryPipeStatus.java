package com.snowflake.kafka.connector.internal;

import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.node.ObjectNode;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class SnowflakeTelemetryPipeStatus extends SnowflakeTelemetryBasicInfo {
  // Offset info
  AtomicLong processedOffset;           // processed offset (offset that is most recent in buffer)
  AtomicLong flushedOffset;             // flushed offset (files on stage)
  AtomicLong committedOffset;           // committed offset + 1 (files being ingested)
  AtomicLong purgedOffset;              // purged offset (files purged or moved to table stage)
  static final String PROCESSED_OFFSET  = "processed_offset";
  static final String FLUSHED_OFFSET    = "flushed_offset";
  static final String COMMITTED_OFFSET  = "committed_offset";
  static final String PURGED_OFFSET     = "purged_offset";

  // File count info
  int fileCountOnStage;               // files that are currently on stage
  int fileCountOnIngestion;           // files that are being ingested
  int fileCountPurged;                // files that are purged
  int fileCountTableStage;            // files that are moved to table stage
  static final String FILE_COUNT_ON_STAGE       = "file_count_on_stage";
  static final String FILE_COUNT_ON_INGESTION   = "file_count_on_ingestion";
  static final String FILE_COUNT_PURGED         = "file_count_purged";
  static final String FILE_COUNT_TABLE_STAGE    = "file_count_table_stage";

  // Cleaner restart count
  int cleanerRestartCount;          // how many times the cleaner restarted
  static final String CLEANER_RESTART_COUNT   = "cleaner_restart_count";

  // Average lag of Kafka
  long averageKafkaLag;                                // average lag on Kafka side
  int averageKafkaLagRecordCount;                      // record count
  static final String AVERAGE_KAFKA_LAG                = "average_kafka_lag";
  static final String AVERAGE_KAFKA_LAG_RECORD_COUNT   = "average_kafka_lag_record_count";

  // Average lag of ingestion
  long averageIngestionLag;                             // average lag on Ingestion side
  int averageIngestionLagFileCount;                     // file count
  static final String AVERAGE_INGESTION_LAG             = "average_ingestion_lag";
  static final String AVERAGE_INGESTION_LAG_FILE_COUNT  = "average_ingestion_lag_file_count";

  // Legacy metrix
  AtomicLong totalNumberOfRecord;
  AtomicLong totalSizeOfData;
  static final String RECORD_NUMBER = "record_number";
  static final String BYTE_NUMBER = "byte_number";

  long startTime;
  long endTime;
  static final String START_TIME = "start_time";
  static final String END_TIME = "end_time";

  SnowflakeTelemetryPipeStatus(final String tableName, final String stageName, final String pipeName)
  {
    this.tableName = tableName;
    this.stageName = stageName;
    this.pipeName = pipeName;

    this.processedOffset = new AtomicLong(-1);
    this.flushedOffset = new AtomicLong(-1);
    this.committedOffset = new AtomicLong(0);
    this.purgedOffset = new AtomicLong(-1);

    this.totalNumberOfRecord = new AtomicLong(0);
    this.totalSizeOfData = new AtomicLong(0);
  }

  void dumpTo(ObjectNode msg)
  {
    msg.put(TABLE_NAME, tableName);
    msg.put(STAGE_NAME, stageName);
    msg.put(PIPE_NAME, pipeName);

    msg.put(PROCESSED_OFFSET, processedOffset.get());
    msg.put(FLUSHED_OFFSET, flushedOffset.get());
    msg.put(COMMITTED_OFFSET, committedOffset.get());
    msg.put(PURGED_OFFSET, purgedOffset.get());
    msg.put(FILE_COUNT_ON_STAGE, fileCountOnStage);
    msg.put(FILE_COUNT_ON_INGESTION, fileCountOnIngestion);
    msg.put(FILE_COUNT_PURGED, fileCountPurged);
    msg.put(FILE_COUNT_TABLE_STAGE, fileCountTableStage);
    msg.put(CLEANER_RESTART_COUNT, cleanerRestartCount);
    msg.put(AVERAGE_KAFKA_LAG, averageKafkaLag);
    msg.put(AVERAGE_KAFKA_LAG_RECORD_COUNT, averageKafkaLagRecordCount);
    msg.put(AVERAGE_INGESTION_LAG, averageIngestionLag);
    msg.put(AVERAGE_INGESTION_LAG_FILE_COUNT, averageIngestionLagFileCount);
    msg.put(RECORD_NUMBER, totalNumberOfRecord.get());
    msg.put(BYTE_NUMBER, totalSizeOfData.get());
    msg.put(START_TIME, startTime);
    msg.put(END_TIME, endTime);
  }
}
