package com.snowflake.kafka.connector.internal;

import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.node.ObjectNode;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class SnowflakeTelemetryPipeStatus extends SnowflakeTelemetryBasicInfo {
  // Offset info
  AtomicLong processedOffset;           // processed offset (offset that is most recent in buffer)
  AtomicLong flushedOffset;             // flushed offset (files on stage)
  AtomicLong committedOffset;           // committed offset (files being ingested)
  AtomicLong purgedOffset;              // purged offset (files purged or moved to table stage)
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
  AtomicLong fileCountOnStage;                              // files that are currently on stage
  AtomicLong fileCountOnIngestion;                          // files that are being ingested
  AtomicLong fileCountPurged;                               // files that are purged
  AtomicLong fileCountTableStageIngestFail;                 // files that are moved to table stage due to ingestion failure
  AtomicLong fileCountTableStageBrokenRecord;               // files that are moved to table stage due to broken record
  static final String FILE_COUNT_ON_STAGE                   = "file_count_on_stage";
  static final String FILE_COUNT_ON_INGESTION               = "file_count_on_ingestion";
  static final String FILE_COUNT_PURGED                     = "file_count_purged";
  static final String FILE_COUNT_TABLE_STAGE_INGEST_FAIL    = "file_count_table_stage_ingest_fail";
  static final String FILE_COUNT_TABLE_STAGE_BROKEN_RECORD  = "file_count_table_stage_broken_record";

  // Cleaner restart count
  AtomicLong cleanerRestartCount;               // how many times the cleaner restarted
  static final String CLEANER_RESTART_COUNT     = "cleaner_restart_count";

  // Memory usage
  AtomicLong memoryUsage;               // buffer size of the pipe in Bytes
  static final String MEMORY_USAGE      = "memory_usage";

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
    super(tableName, stageName, pipeName);

    // Initial value of processed/flushed/committed/purged offset should be set to -1,
    // because the offset stands for the last offset of the record that are at the status.
    // When record with offset 0 is processed, processedOffset will be updated to 0.
    // Therefore initial value should be set to -1 to indicate that no record have been processed.
    this.processedOffset = new AtomicLong(-1);
    this.flushedOffset = new AtomicLong(-1);
    this.committedOffset = new AtomicLong(-1);
    this.purgedOffset = new AtomicLong(-1);
    this.totalNumberOfRecord = new AtomicLong(0);
    this.totalSizeOfData = new AtomicLong(0);
    this.fileCountOnStage = new AtomicLong(0);
    this.fileCountOnIngestion = new AtomicLong(0);
    this.fileCountPurged = new AtomicLong(0);
    this.fileCountTableStageIngestFail = new AtomicLong(0);
    this.fileCountTableStageBrokenRecord = new AtomicLong(0);
    this.cleanerRestartCount = new AtomicLong(0);
    this.memoryUsage = new AtomicLong(0);

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

  boolean empty()
  {
    // Check that all properties are still at the default value.
    return this.processedOffset.get() == -1 &&
            this.flushedOffset.get() == -1 &&
            this.committedOffset.get() == -1 &&
            this.purgedOffset.get() == -1 &&
            this.totalNumberOfRecord.get() == 0 &&
            this.totalSizeOfData.get() == 0 &&
            this.fileCountOnStage.get() == 0 &&
            this.fileCountOnIngestion.get() == 0 &&
            this.fileCountPurged.get() == 0 &&
            this.fileCountTableStageIngestFail.get() == 0 &&
            this.fileCountTableStageBrokenRecord.get() == 0 &&
            this.cleanerRestartCount.get() == 0 &&
            this.memoryUsage.get() == 0 &&
            this.averageKafkaLag.get() == 0 &&
            this.averageKafkaLagRecordCount.get() == 0 &&
            this.averageIngestionLag.get() == 0 &&
            this.averageIngestionLagFileCount.get() == 0 &&
            this.averageCommitLag.get() == 0 &&
            this.averageCommitLagFileCount.get() == 0;
  }

  @Override
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
    msg.put(FILE_COUNT_TABLE_STAGE_INGEST_FAIL, fileCountTableStageIngestFail.get());
    msg.put(FILE_COUNT_TABLE_STAGE_BROKEN_RECORD, fileCountTableStageBrokenRecord.get());
    msg.put(CLEANER_RESTART_COUNT, cleanerRestartCount.get());
    msg.put(MEMORY_USAGE, memoryUsage.get());

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
