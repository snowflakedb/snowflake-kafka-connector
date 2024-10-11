package com.snowflake.kafka.connector.internal.telemetry;

import static com.snowflake.kafka.connector.internal.metrics.MetricsUtil.FILE_COUNT_SUB_DOMAIN;
import static com.snowflake.kafka.connector.internal.metrics.MetricsUtil.FILE_COUNT_TABLE_STAGE_INGESTION_FAIL;
import static com.snowflake.kafka.connector.internal.metrics.MetricsUtil.LATENCY_SUB_DOMAIN;
import static com.snowflake.kafka.connector.internal.metrics.MetricsUtil.OFFSET_SUB_DOMAIN;
import static com.snowflake.kafka.connector.internal.metrics.MetricsUtil.constructMetricName;
import static com.snowflake.kafka.connector.internal.telemetry.TelemetryConstants.AVERAGE_COMMIT_LAG_FILE_COUNT;
import static com.snowflake.kafka.connector.internal.telemetry.TelemetryConstants.AVERAGE_COMMIT_LAG_MS;
import static com.snowflake.kafka.connector.internal.telemetry.TelemetryConstants.AVERAGE_INGESTION_LAG_FILE_COUNT;
import static com.snowflake.kafka.connector.internal.telemetry.TelemetryConstants.AVERAGE_INGESTION_LAG_MS;
import static com.snowflake.kafka.connector.internal.telemetry.TelemetryConstants.AVERAGE_KAFKA_LAG_MS;
import static com.snowflake.kafka.connector.internal.telemetry.TelemetryConstants.AVERAGE_KAFKA_LAG_RECORD_COUNT;
import static com.snowflake.kafka.connector.internal.telemetry.TelemetryConstants.BYTE_NUMBER;
import static com.snowflake.kafka.connector.internal.telemetry.TelemetryConstants.CLEANER_RESTART_COUNT;
import static com.snowflake.kafka.connector.internal.telemetry.TelemetryConstants.COMMITTED_OFFSET;
import static com.snowflake.kafka.connector.internal.telemetry.TelemetryConstants.END_TIME;
import static com.snowflake.kafka.connector.internal.telemetry.TelemetryConstants.FILE_COUNT_ON_INGESTION;
import static com.snowflake.kafka.connector.internal.telemetry.TelemetryConstants.FILE_COUNT_ON_STAGE;
import static com.snowflake.kafka.connector.internal.telemetry.TelemetryConstants.FILE_COUNT_PURGED;
import static com.snowflake.kafka.connector.internal.telemetry.TelemetryConstants.FILE_COUNT_TABLE_STAGE_BROKEN_RECORD;
import static com.snowflake.kafka.connector.internal.telemetry.TelemetryConstants.FILE_COUNT_TABLE_STAGE_INGEST_FAIL;
import static com.snowflake.kafka.connector.internal.telemetry.TelemetryConstants.FLUSHED_OFFSET;
import static com.snowflake.kafka.connector.internal.telemetry.TelemetryConstants.MEMORY_USAGE;
import static com.snowflake.kafka.connector.internal.telemetry.TelemetryConstants.PARTITION_ID;
import static com.snowflake.kafka.connector.internal.telemetry.TelemetryConstants.PIPE_NAME;
import static com.snowflake.kafka.connector.internal.telemetry.TelemetryConstants.PROCESSED_OFFSET;
import static com.snowflake.kafka.connector.internal.telemetry.TelemetryConstants.PURGED_OFFSET;
import static com.snowflake.kafka.connector.internal.telemetry.TelemetryConstants.RECORD_NUMBER;
import static com.snowflake.kafka.connector.internal.telemetry.TelemetryConstants.STAGE_NAME;
import static com.snowflake.kafka.connector.internal.telemetry.TelemetryConstants.START_TIME;
import static com.snowflake.kafka.connector.internal.telemetry.TelemetryConstants.TABLE_NAME;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.snowflake.kafka.connector.internal.metrics.MetricsJmxReporter;
import com.snowflake.kafka.connector.internal.metrics.MetricsUtil;
import com.snowflake.kafka.connector.internal.metrics.MetricsUtil.EventType;
import java.util.Arrays;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.LongUnaryOperator;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Extension of {@link SnowflakeTelemetryBasicInfo} class used to send data to snowflake
 * periodically.
 *
 * <p>Check frequency of {@link
 * SnowflakeTelemetryService#reportKafkaPartitionUsage(SnowflakeTelemetryBasicInfo, boolean)}
 *
 * <p>Most of the data sent to Snowflake is an aggregated data.
 */
public class SnowflakeTelemetryPipeStatus extends SnowflakeTelemetryBasicInfo {
  // ---------- Offset info ----------

  // processed offset (offset that is most recent in buffer)
  private AtomicLong processedOffset;

  // flushed offset (files on stage)
  private AtomicLong flushedOffset;

  // committed offset (files being ingested)
  // NOTE: These offsets are not necessarily the offsets that were ingested into SF table. These are
  // the offsets for which commit API has being called and KC has invoked ingestFiles for those set
  // of offsets.
  private AtomicLong committedOffset;

  // purged offset (files purged or moved to table stage)
  private AtomicLong purgedOffset;

  // Legacy metrics
  private AtomicLong totalNumberOfRecord; // total number of record
  private AtomicLong totalSizeOfData; // total size of data

  // File count info
  private AtomicLong fileCountOnStage; // files that are currently on stage
  private AtomicLong fileCountOnIngestion; // files that are being ingested
  private AtomicLong fileCountPurged; // files that are purged
  private final AtomicLong
      fileCountTableStageIngestFail; // files that are moved to table stage due to ingestion failure
  private final AtomicLong
      fileCountTableStageBrokenRecord; // files that are moved to table stage due to broken record

  // Cleaner restart count
  AtomicLong cleanerRestartCount; // how many times the cleaner restarted

  // Memory usage
  AtomicLong memoryUsage; // buffer size of the pipe in Bytes

  // ------------ following metrics are not cumulative, reset every time sent ------------//
  // Average lag of Kafka
  private AtomicLong averageKafkaLagMs; // average lag on Kafka side
  private AtomicLong averageKafkaLagRecordCount; // record count

  // Average lag of ingestion
  private AtomicLong averageIngestionLagMs; // average lag between file upload and file delete
  private AtomicLong averageIngestionLagFileCount; // file count

  // Average lag of commit
  private AtomicLong averageCommitLagMs; // average lag between file upload and ingest api calling
  private AtomicLong averageCommitLagFileCount; // file count

  // Need to update two values atomically when calculating lag, thus
  // a lock is required to protect the access
  private final Lock lagLock;

  private AtomicLong startTime; // start time of the status recording period

  // JMX Metrics related to Latencies
  private ConcurrentMap<EventType, Timer> eventsByType = Maps.newConcurrentMap();

  // A boolean to turn on or off a JMX metric as required.
  private final boolean enableCustomJMXConfig;

  // May not be set if jmx is set to false
  private Meter fileCountTableStageBrokenRecordMeter, fileCountTableStageIngestFailMeter;

  private final String stageName;
  private final String pipeName;
  private final int partitionId;

  public SnowflakeTelemetryPipeStatus(
      final String tableName,
      final String stageName,
      final String pipeName,
      final int partitionId,
      final boolean enableCustomJMXConfig,
      final MetricsJmxReporter metricsJmxReporter) {
    super(tableName, SnowflakeTelemetryService.TelemetryType.KAFKA_PIPE_USAGE);
    this.stageName = stageName;
    this.pipeName = pipeName;
    this.partitionId = partitionId;

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

  /**
   * Kafka Lag is time between kafka connector sees the record and time record was inserted into
   * kafka.
   *
   * <p>Check the implementation of updateLag on what is done with this lag.
   *
   * @param lag
   */
  public void updateKafkaLag(final long lag) {
    updateLag(lag, averageKafkaLagRecordCount, averageKafkaLagMs, EventType.KAFKA_LAG);
  }

  /**
   * Ingestion Lag is time between kafka connector flushes the file and time file was first found
   * from insertReport/loadHistory API.
   *
   * <p>Check the implementation of updateLag on what is done with this lag.
   *
   * @param lag
   */
  public void updateIngestionLag(final long lag) {
    updateLag(lag, averageIngestionLagFileCount, averageIngestionLagMs, EventType.INGESTION_LAG);
  }

  /**
   * Commit Lag is time between kafka connector commits the after calling insertFiles API and time
   * file was flushed into internal stage.
   *
   * <p>Check the implementation of updateLag on what is done with this lag.
   *
   * @param lag
   */
  public void updateCommitLag(final long lag) {
    updateLag(lag, averageCommitLagFileCount, averageCommitLagMs, EventType.COMMIT_LAG);
  }

  /**
   * The current lag is just added to the running average to calculate the new average.
   *
   * @param lag currentLag/current time difference between two data points
   * @param averageFileCount current running average's denominator
   * @param averageLag current running average
   * @param eventType
   */
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

  @Override
  public boolean isEmpty() {
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
  public void dumpTo(ObjectNode msg) {
    msg.put(TABLE_NAME, tableName);
    msg.put(STAGE_NAME, stageName);
    msg.put(PIPE_NAME, pipeName);
    msg.put(PARTITION_ID, partitionId);

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
        "Registering metrics for pipe:{}, existing:{}",
        pipeName,
        metricsJmxReporter.getMetricRegistry().getMetrics().keySet().toString());
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

  // --------------- Setter for Offset counts --------------- //

  public void setProcessedOffset(long processedOffset) {
    this.processedOffset.set(processedOffset);
  }

  public void setFlushedOffset(long flushedOffset) {
    this.flushedOffset.set(flushedOffset);
  }

  public void setCommittedOffset(long committedOffset) {
    this.committedOffset.set(committedOffset);
  }

  /**
   * Either keeps the same offset or updates the purgedOffset if a higher value offset is found in
   * insertReport Snowpipe API
   *
   * @param unaryOperator the function to apply on purgedOffset
   */
  public void setPurgedOffsetAtomically(LongUnaryOperator unaryOperator) {
    this.purgedOffset.updateAndGet(unaryOperator);
  }

  // --------------- File Counts at various stages of ingestion --------------- //

  public void addAndGetFileCountOnStage(long fileCountOnStage) {
    this.fileCountOnStage.addAndGet(fileCountOnStage);
  }

  public void addAndGetFileCountOnIngestion(long fileCountOnIngestion) {
    this.fileCountOnIngestion.addAndGet(fileCountOnIngestion);
  }

  public void addAndGetFileCountPurged(long fileCountPurged) {
    this.fileCountPurged.addAndGet(fileCountPurged);
  }

  public long incrementAndGetCleanerRestartCount() {
    return this.cleanerRestartCount.incrementAndGet();
  }

  public void addAndGetTotalNumberOfRecord(long totalNumberOfRecord) {
    this.totalNumberOfRecord.addAndGet(totalNumberOfRecord);
  }

  public void addAndGetTotalSizeOfData(long totalSizeOfData) {
    this.totalSizeOfData.addAndGet(totalSizeOfData);
  }

  public void addAndGetMemoryUsage(long memoryUsage) {
    this.memoryUsage.addAndGet(memoryUsage);
  }

  public void resetMemoryUsage() {
    this.memoryUsage.set(0l);
  }

  // --------------- For testing --------------- //

  @VisibleForTesting
  public void setCleanerRestartCount(long cleanerRestartCount) {
    this.cleanerRestartCount.set(cleanerRestartCount);
  }

  @VisibleForTesting
  public void setAverageKafkaLagMs(long averageKafkaLagMs) {
    this.averageKafkaLagMs.set(averageKafkaLagMs);
  }

  @VisibleForTesting
  public void setAverageKafkaLagRecordCount(long averageKafkaLagRecordCount) {
    this.averageKafkaLagRecordCount.set(averageKafkaLagRecordCount);
  }

  @VisibleForTesting
  public void setAverageIngestionLagMs(long averageIngestionLagMs) {
    this.averageIngestionLagMs.set(averageIngestionLagMs);
  }

  @VisibleForTesting
  public void setAverageIngestionLagFileCount(long averageIngestionLagFileCount) {
    this.averageIngestionLagFileCount.set(averageIngestionLagFileCount);
  }

  @VisibleForTesting
  public void setAverageCommitLagMs(long averageCommitLagMs) {
    this.averageCommitLagMs.set(averageCommitLagMs);
  }

  @VisibleForTesting
  public void setAverageCommitLagFileCount(long averageCommitLagFileCount) {
    this.averageCommitLagFileCount.set(averageCommitLagFileCount);
  }
}
