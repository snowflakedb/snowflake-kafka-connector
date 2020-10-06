package com.snowflake.kafka.connector.internal;

import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.node.ObjectNode;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class SnowflakeTelemetryPipeStatus extends SnowflakeTelemetryBasicInfo {
  // Offset info
  AtomicLong processedOffset;           // processed offset + 1 (offset that is most recent in buffer)
  AtomicLong flushedOffset;             // flushed offset + 1 (files on stage)
  AtomicLong committedOffset;           // committed offset + 1 (files being ingested)
  AtomicLong purgedOffset;              // purged offset + 1 (files purged or moved to table stage)
  static final String PROCESSED_OFFSET  = "processed_offset";
  static final String FLUSHED_OFFSET    = "flushed_offset";
  static final String COMMITTED_OFFSET  = "committed_offset";
  static final String PURGED_OFFSET     = "purged_offset";

  // Legacy metrix
  AtomicLong totalNumberOfRecord;       // total number of record
  AtomicLong totalSizeOfData;           // total size of data
  static final String RECORD_NUMBER     = "record_number";
  static final String BYTE_NUMBER       = "byte_number";

  // File count info
  AtomicLong fileCountOnStage;                  // files that are currently on stage
  AtomicLong fileCountOnIngestion;              // files that are being ingested
  AtomicLong fileCountPurged;                   // files that are purged
  AtomicLong fileCountTableStage;               // files that are moved to table stage
  static final String FILE_COUNT_ON_STAGE       = "file_count_on_stage";
  static final String FILE_COUNT_ON_INGESTION   = "file_count_on_ingestion";
  static final String FILE_COUNT_PURGED         = "file_count_purged";
  static final String FILE_COUNT_TABLE_STAGE    = "file_count_table_stage";

  // Cleaner restart count
  AtomicLong cleanerRestartCount;               // how many times the cleaner restarted
  static final String CLEANER_RESTART_COUNT     = "cleaner_restart_count";

  //------------ following metrics are not cumulative, reset every time sent ------------//
  // Average lag of Kafka
  AtomicLong averageKafkaLag;                           // average lag on Kafka side
  AtomicLong averageKafkaLagRecordCount;                // record count
  static final String AVERAGE_KAFKA_LAG                 = "average_kafka_lag";
  static final String AVERAGE_KAFKA_LAG_RECORD_COUNT    = "average_kafka_lag_record_count";

  // Average lag of ingestion
  AtomicLong averageIngestionLag;                       // average lag between file upload and file delete
  AtomicLong averageIngestionLagFileCount;              // file count
  static final String AVERAGE_INGESTION_LAG             = "average_ingestion_lag";
  static final String AVERAGE_INGESTION_LAG_FILE_COUNT  = "average_ingestion_lag_file_count";

  // Average lag of commit
  AtomicLong averageCommitLag;                          // average lag between file upload and ingest api calling
  AtomicLong averageCommitLagFileCount;                 // file count
  static final String AVERAGE_COMMIT_LAG                = "average_commit_lag";
  static final String AVERAGE_COMMIT_LAG_FILE_COUNT     = "average_commit_lag_file_count";

  // Need to update two values atomically when calculating lag, thus
  // a lock is required to protect the access
  private final Lock lagLock;

  AtomicLong startTime;             // start time of the status recording period
  static final String START_TIME    = "start_time";
  static final String END_TIME      = "end_time";

  SnowflakeTelemetryPipeStatus(final String tableName, final String stageName, final String pipeName)
  {
    this.tableName = tableName;
    this.stageName = stageName;
    this.pipeName = pipeName;

    this.processedOffset = new AtomicLong(0);
    this.flushedOffset = new AtomicLong(0);
    this.committedOffset = new AtomicLong(0);
    this.purgedOffset = new AtomicLong(0);
    this.totalNumberOfRecord = new AtomicLong(0);
    this.totalSizeOfData = new AtomicLong(0);
    this.fileCountOnStage = new AtomicLong(0);
    this.fileCountOnIngestion = new AtomicLong(0);
    this.fileCountPurged = new AtomicLong(0);
    this.fileCountTableStage = new AtomicLong(0);
    this.cleanerRestartCount = new AtomicLong(0);

    this.averageKafkaLag = new AtomicLong(0);
    this.averageKafkaLagRecordCount = new AtomicLong(0);
    this.averageIngestionLag = new AtomicLong(0);
    this.averageIngestionLagFileCount = new AtomicLong(0);
    this.averageCommitLag = new AtomicLong(0);
    this.averageCommitLagFileCount = new AtomicLong(0);
    this.startTime = new AtomicLong(System.currentTimeMillis());

    this.lagLock = new ReentrantLock();
  }

  void updateKafkaLag(final long lag)
  {
    updateLag(lag, averageKafkaLagRecordCount, averageKafkaLag);
  }

  void updateIngestionLag(final long lag)
  {
    updateLag(lag, averageIngestionLagFileCount, averageIngestionLag);
  }

  void updateCommitLag(final long lag)
  {
    updateLag(lag, averageCommitLagFileCount, averageCommitLag);
  }

  private void updateLag(final long lag, AtomicLong averageFileCount, AtomicLong averageLag)
  {
    lagLock.lock();
    try {
      long lagFileCount = averageFileCount.getAndIncrement();
      // Weighted average to calculate the cumulative average lag
      averageLag.updateAndGet(value -> (value * lagFileCount + lag) / (lagFileCount + 1));
    }
    finally {
      lagLock.unlock();
    }
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
    msg.put(RECORD_NUMBER, totalNumberOfRecord.get());
    msg.put(BYTE_NUMBER, totalSizeOfData.get());
    msg.put(FILE_COUNT_ON_STAGE, fileCountOnStage.get());
    msg.put(FILE_COUNT_ON_INGESTION, fileCountOnIngestion.get());
    msg.put(FILE_COUNT_PURGED, fileCountPurged.get());
    msg.put(FILE_COUNT_TABLE_STAGE, fileCountTableStage.get());
    msg.put(CLEANER_RESTART_COUNT, cleanerRestartCount.get());

    lagLock.lock();
    try
    {
      msg.put(AVERAGE_KAFKA_LAG, averageKafkaLag.getAndSet(0));
      msg.put(AVERAGE_KAFKA_LAG_RECORD_COUNT, averageKafkaLagRecordCount.getAndSet(0));
      msg.put(AVERAGE_INGESTION_LAG, averageIngestionLag.getAndSet(0));
      msg.put(AVERAGE_INGESTION_LAG_FILE_COUNT, averageIngestionLagFileCount.getAndSet(0));
      msg.put(AVERAGE_COMMIT_LAG, averageCommitLag.getAndSet(0));
      msg.put(AVERAGE_COMMIT_LAG_FILE_COUNT, averageCommitLagFileCount.getAndSet(0));
    }
    finally {
      lagLock.unlock();
    }

    msg.put(START_TIME, startTime.getAndSet(System.currentTimeMillis()));
    msg.put(END_TIME, System.currentTimeMillis());
  }
}
