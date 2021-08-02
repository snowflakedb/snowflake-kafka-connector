package com.snowflake.kafka.connector.internal;

import static com.snowflake.kafka.connector.internal.metrics.MetricsUtil.*;

import com.codahale.metrics.*;
import com.codahale.metrics.Timer;
import com.google.common.collect.Maps;
import com.snowflake.kafka.connector.internal.metrics.MetricsJmxReporter;
import com.snowflake.kafka.connector.internal.metrics.MetricsUtil;
import com.snowflake.kafka.connector.internal.metrics.MetricsUtil.EventType;
import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.node.ObjectNode;

public class SnowflakeTelemetryPipeStatus extends SnowflakeTelemetryBasicInfo {
  // ---------- Offset info ----------

  // processed offset (offset that is most recent in buffer)
  AtomicLong processedOffset;

  // flushed offset (files on stage)
  AtomicLong flushedOffset;

  // committed offset (files being ingested)
  // NOTE: These offsets are not necessarily the offsets that were ingested into SF table. These are
  // the offsets for which commit API has being called and KC has invoked ingestFiles for those set
  // of offsets.
  AtomicLong committedOffset;

  // purged offset (files purged or moved to table stage)
  AtomicLong purgedOffset;

  static final String PROCESSED_OFFSET = "processed_offset";
  static final String FLUSHED_OFFSET = "flushed_offset";
  static final String COMMITTED_OFFSET = "committed_offset";
  static final String PURGED_OFFSET = "purged_offset";

  // Legacy metrics
  AtomicLong totalNumberOfRecord; // total number of record
  AtomicLong totalSizeOfData; // total size of data
  static final String RECORD_NUMBER = "record_number";
  static final String BYTE_NUMBER = "byte_number";

  // File count info
  AtomicLong fileCountOnStage; // files that are currently on stage
  AtomicLong fileCountOnIngestion; // files that are being ingested
  AtomicLong fileCountPurged; // files that are purged
  private final AtomicLong
      fileCountTableStageIngestFail; // files that are moved to table stage due to ingestion failure
  private final AtomicLong
      fileCountTableStageBrokenRecord; // files that are moved to table stage due to broken record
  static final String FILE_COUNT_ON_STAGE = "file_count_on_stage";
  static final String FILE_COUNT_ON_INGESTION = "file_count_on_ingestion";
  static final String FILE_COUNT_PURGED = "file_count_purged";
  static final String FILE_COUNT_TABLE_STAGE_INGEST_FAIL = "file_count_table_stage_ingest_fail";
  static final String FILE_COUNT_TABLE_STAGE_BROKEN_RECORD = "file_count_table_stage_broken_record";

  // Cleaner restart count
  AtomicLong cleanerRestartCount; // how many times the cleaner restarted
  static final String CLEANER_RESTART_COUNT = "cleaner_restart_count";

  // Memory usage
  AtomicLong memoryUsage; // buffer size of the pipe in Bytes
  static final String MEMORY_USAGE = "memory_usage";

  // ------------ following metrics are not cumulative, reset every time sent ------------//
  // Average lag of Kafka
  AtomicLong averageKafkaLagMs; // average lag on Kafka side
  AtomicLong averageKafkaLagRecordCount; // record count
  static final String AVERAGE_KAFKA_LAG_MS = "average_kafka_lag";
  static final String AVERAGE_KAFKA_LAG_RECORD_COUNT = "average_kafka_lag_record_count";

  // Average lag of ingestion
  AtomicLong averageIngestionLagMs; // average lag between file upload and file delete
  AtomicLong averageIngestionLagFileCount; // file count
  static final String AVERAGE_INGESTION_LAG_MS = "average_ingestion_lag";
  static final String AVERAGE_INGESTION_LAG_FILE_COUNT = "average_ingestion_lag_file_count";

  // Average lag of commit
  AtomicLong averageCommitLagMs; // average lag between file upload and ingest api calling
  AtomicLong averageCommitLagFileCount; // file count
  static final String AVERAGE_COMMIT_LAG_MS = "average_commit_lag";
  static final String AVERAGE_COMMIT_LAG_FILE_COUNT = "average_commit_lag_file_count";

  // Need to update two values atomically when calculating lag, thus
  // a lock is required to protect the access
  private final Lock lagLock;

  AtomicLong startTime; // start time of the status recording period
  static final String START_TIME = "start_time";
  static final String END_TIME = "end_time";

  // JMX Metrics related to Latencies
  private ConcurrentMap<EventType, Timer> eventsByType = Maps.newConcurrentMap();

  // A boolean to turn on or off a JMX metric as required.
  private final boolean enableCustomJMXConfig;

  // May not be set if jmx is set to false
  Meter fileCountTableStageBrokenRecordMeter, fileCountTableStageIngestFailMeter;

  SnowflakeTelemetryPipeStatus(
      final String tableName,
      final String stageName,
      final String pipeName,
      final boolean enableCustomJMXConfig,
      final MetricsJmxReporter metricsJmxReporter) {
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

    this.averageKafkaLagMs = new AtomicLong(0);
    this.averageKafkaLagRecordCount = new AtomicLong(0);
    this.averageIngestionLagMs = new AtomicLong(0);
    this.averageIngestionLagFileCount = new AtomicLong(0);
    this.averageCommitLagMs = new AtomicLong(0);
    this.averageCommitLagFileCount = new AtomicLong(0);
    this.startTime = new AtomicLong(System.currentTimeMillis());

    this.lagLock = new ReentrantLock();
    this.enableCustomJMXConfig = enableCustomJMXConfig;
    if (enableCustomJMXConfig) {
      registerPipeJMXMetrics(pipeName, metricsJmxReporter);
    }
  }

  void updateKafkaLag(final long lag) {
    updateLag(lag, averageKafkaLagRecordCount, averageKafkaLagMs, EventType.KAFKA_LAG);
  }

  void updateIngestionLag(final long lag) {
    updateLag(lag, averageIngestionLagFileCount, averageIngestionLagMs, EventType.INGESTION_LAG);
  }

  void updateCommitLag(final long lag) {
    updateLag(lag, averageCommitLagFileCount, averageCommitLagMs, EventType.COMMIT_LAG);
  }

  private void updateLag(
      final long lag, AtomicLong averageFileCount, AtomicLong averageLag, EventType eventType) {
    if (this.enableCustomJMXConfig) {
      // Map will only be non empty if jmx is enabled.
      eventsByType.get(eventType).update(lag, TimeUnit.MILLISECONDS);
    }
    lagLock.lock();
    try {
      long lagFileCount = averageFileCount.getAndIncrement();
      // Weighted average to calculate the cumulative average lag
      averageLag.updateAndGet(value -> (value * lagFileCount + lag) / (lagFileCount + 1));
    } finally {
      lagLock.unlock();
    }
  }

  /**
   * When either key or value is broken.
   *
   * @param n number of records
   */
  public void updateBrokenRecordMetrics(long n) {
    this.fileCountTableStageBrokenRecord.addAndGet(n);
    if (enableCustomJMXConfig) {
      this.fileCountTableStageBrokenRecordMeter.mark(n);
    }
  }

  /**
   * When Ingestion status of n number of files is not found/failed.
   *
   * @param n number of files failed ingestion
   */
  public void updateFailedIngestionMetrics(long n) {
    this.fileCountTableStageIngestFail.addAndGet(n);
    if (enableCustomJMXConfig) {
      this.fileCountTableStageIngestFailMeter.mark(n);
    }
  }

  boolean empty() {
    // Check that all properties are still at the default value.
    return this.processedOffset.get() == -1
        && this.flushedOffset.get() == -1
        && this.committedOffset.get() == -1
        && this.purgedOffset.get() == -1
        && this.totalNumberOfRecord.get() == 0
        && this.totalSizeOfData.get() == 0
        && this.fileCountOnStage.get() == 0
        && this.fileCountOnIngestion.get() == 0
        && this.fileCountPurged.get() == 0
        && this.fileCountTableStageIngestFail.get() == 0
        && this.fileCountTableStageBrokenRecord.get() == 0
        && this.cleanerRestartCount.get() == 0
        && this.memoryUsage.get() == 0
        && this.averageKafkaLagMs.get() == 0
        && this.averageKafkaLagRecordCount.get() == 0
        && this.averageIngestionLagMs.get() == 0
        && this.averageIngestionLagFileCount.get() == 0
        && this.averageCommitLagMs.get() == 0
        && this.averageCommitLagFileCount.get() == 0;
  }

  @Override
  void dumpTo(ObjectNode msg) {
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
    try {
      msg.put(AVERAGE_KAFKA_LAG_MS, averageKafkaLagMs.getAndSet(0));
      msg.put(AVERAGE_KAFKA_LAG_RECORD_COUNT, averageKafkaLagRecordCount.getAndSet(0));
      msg.put(AVERAGE_INGESTION_LAG_MS, averageIngestionLagMs.getAndSet(0));
      msg.put(AVERAGE_INGESTION_LAG_FILE_COUNT, averageIngestionLagFileCount.getAndSet(0));
      msg.put(AVERAGE_COMMIT_LAG_MS, averageCommitLagMs.getAndSet(0));
      msg.put(AVERAGE_COMMIT_LAG_FILE_COUNT, averageCommitLagFileCount.getAndSet(0));
    } finally {
      lagLock.unlock();
    }

    msg.put(START_TIME, startTime.getAndSet(System.currentTimeMillis()));
    msg.put(END_TIME, System.currentTimeMillis());
  }

  // --------------- JMX Metrics --------------- //

  /**
   * Registers all the Metrics inside the metricRegistry. The registered metric will be a subclass
   * of {@link Metric}
   *
   * @param pipeName pipeName
   * @param metricsJmxReporter wrapper class for registering all metrics related to above connector
   *     and pipe
   */
  private void registerPipeJMXMetrics(
      final String pipeName, MetricsJmxReporter metricsJmxReporter) {
    MetricRegistry currentMetricRegistry = metricsJmxReporter.getMetricRegistry();

    // Lazily remove all registered metrics from the registry since this can be invoked during
    // partition reassignment
    LOGGER.debug(
        Logging.logMessage(
            "Registering metrics for pipe:{}, existing:{}",
            pipeName,
            metricsJmxReporter.getMetricRegistry().getMetrics().keySet().toString()));
    metricsJmxReporter.removeMetricsFromRegistry(pipeName);

    try {
      // Latency JMX
      // create meter per event type
      Arrays.stream(EventType.values())
          .forEach(
              eventType ->
                  eventsByType.put(
                      eventType,
                      currentMetricRegistry.timer(
                          constructMetricName(
                              pipeName, LATENCY_SUB_DOMAIN, eventType.getMetricName()))));

      // Offset JMX
      currentMetricRegistry.register(
          constructMetricName(pipeName, OFFSET_SUB_DOMAIN, MetricsUtil.PROCESSED_OFFSET),
          (Gauge<Long>) () -> processedOffset.get());

      currentMetricRegistry.register(
          constructMetricName(pipeName, OFFSET_SUB_DOMAIN, MetricsUtil.FLUSHED_OFFSET),
          (Gauge<Long>) () -> flushedOffset.get());

      currentMetricRegistry.register(
          constructMetricName(pipeName, OFFSET_SUB_DOMAIN, MetricsUtil.COMMITTED_OFFSET),
          (Gauge<Long>) () -> committedOffset.get());

      currentMetricRegistry.register(
          constructMetricName(pipeName, OFFSET_SUB_DOMAIN, MetricsUtil.PURGED_OFFSET),
          (Gauge<Long>) () -> purgedOffset.get());

      // File count JMX
      currentMetricRegistry.register(
          constructMetricName(pipeName, FILE_COUNT_SUB_DOMAIN, MetricsUtil.FILE_COUNT_ON_INGESTION),
          (Gauge<Long>) () -> fileCountOnIngestion.get());

      currentMetricRegistry.register(
          constructMetricName(pipeName, FILE_COUNT_SUB_DOMAIN, MetricsUtil.FILE_COUNT_ON_STAGE),
          (Gauge<Long>) () -> fileCountOnStage.get());

      metricsJmxReporter
          .getMetricRegistry()
          .register(
              constructMetricName(pipeName, FILE_COUNT_SUB_DOMAIN, MetricsUtil.FILE_COUNT_PURGED),
              (Gauge<Long>) () -> fileCountPurged.get());

      fileCountTableStageBrokenRecordMeter =
          currentMetricRegistry.meter(
              constructMetricName(
                  pipeName,
                  FILE_COUNT_SUB_DOMAIN,
                  MetricsUtil.FILE_COUNT_TABLE_STAGE_BROKEN_RECORD));

      fileCountTableStageIngestFailMeter =
          currentMetricRegistry.meter(
              constructMetricName(
                  pipeName, FILE_COUNT_SUB_DOMAIN, FILE_COUNT_TABLE_STAGE_INGESTION_FAIL));

    } catch (IllegalArgumentException ex) {
      LOGGER.warn("Metrics already present:{}", ex.getMessage());
    }
  }
}
