package com.snowflake.kafka.connector.internal;

import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.SNOWPIPE_SINGLE_TABLE_MULTIPLE_TOPICS_FIX_ENABLED;
import static com.snowflake.kafka.connector.internal.FileNameUtils.searchForMissingOffsets;
import static com.snowflake.kafka.connector.internal.metrics.MetricsUtil.BUFFER_RECORD_COUNT;
import static com.snowflake.kafka.connector.internal.metrics.MetricsUtil.BUFFER_SIZE_BYTES;
import static com.snowflake.kafka.connector.internal.metrics.MetricsUtil.BUFFER_SUB_DOMAIN;
import static java.util.Objects.isNull;
import static org.apache.kafka.common.record.TimestampType.NO_TIMESTAMP_TYPE;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig;
import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.config.TopicToTableModeExtractor;
import com.snowflake.kafka.connector.internal.metrics.MetricsJmxReporter;
import com.snowflake.kafka.connector.internal.metrics.MetricsUtil;
import com.snowflake.kafka.connector.internal.telemetry.SnowflakeTelemetryPipeCreation;
import com.snowflake.kafka.connector.internal.telemetry.SnowflakeTelemetryPipeStatus;
import com.snowflake.kafka.connector.internal.telemetry.SnowflakeTelemetryService;
import com.snowflake.kafka.connector.records.RecordService;
import com.snowflake.kafka.connector.records.RecordServiceFactory;
import com.snowflake.kafka.connector.records.SnowflakeJsonSchema;
import com.snowflake.kafka.connector.records.SnowflakeMetadataConfig;
import com.snowflake.kafka.connector.records.SnowflakeRecordContent;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;

/**
 * This is per task configuration. A task can be assigned multiple partitions. Major methods are
 * startTask, insert, getOffset and close methods.
 *
 * <p>StartTask: Called when partitions are assigned. Responsible for generating the POJOs.
 *
 * <p>Insert and getOffset are called when {@link
 * com.snowflake.kafka.connector.SnowflakeSinkTask#put(Collection)} and {@link
 * com.snowflake.kafka.connector.SnowflakeSinkTask#preCommit(Map)} APIs are called.
 */
class SnowflakeSinkServiceV1 implements SnowflakeSinkService {
  private final KCLogger LOGGER = new KCLogger(SnowflakeSinkServiceV1.class.getName());

  private static final long ONE_HOUR = 60 * 60 * 1000L;
  private static final long TEN_MINUTES = 10 * 60 * 1000L;
  protected static final long CLEAN_TIME = 60 * 1000L; // one minutes

  // Set in config (Time based flush) in seconds
  private long flushTime;
  // Set in config (buffer size based flush) in bytes
  private long fileSize;

  // Set in config (Threshold before we send the buffer to internal stage) corresponds to # of
  // records in kafka
  private long recordNum;
  private final SnowflakeConnectionService conn;
  private final Map<String, ServiceContext> pipes;
  private final RecordService recordService;
  private boolean isStopped;
  private final SnowflakeTelemetryService telemetryService;
  private Map<String, String> topic2TableMap;

  // Behavior to be set at the start of connector start. (For tombstone records)
  private SnowflakeSinkConnectorConfig.BehaviorOnNullValues behaviorOnNullValues;

  // default is true unless the configuration provided is false;
  // If this is true, we will enable Mbean for required classes and emit JMX metrics for monitoring
  private boolean enableCustomJMXMonitoring = SnowflakeSinkConnectorConfig.JMX_OPT_DEFAULT;

  @Nullable private ScheduledExecutorService cleanerServiceExecutor;

  // if enabled, the prefix for stage files for a given table will contain information about source
  // topic hashcode. This is required in scenarios when multiple topics are configured to ingest
  // data into a single table.
  private boolean enableStageFilePrefixExtension = false;

  // CC-30278 if disabled cleaner won't delete reprocess files on stage
  private boolean enableReprocessFilesCleanup = true;

  private final Set<String> perTableWarningNotifications = new HashSet<>();

  private final long v2CleanerIntervalSeconds;

  SnowflakeSinkServiceV1(SnowflakeConnectionService conn, long v2CleanerIntervalSeconds) {
    if (conn == null || conn.isClosed()) {
      throw SnowflakeErrors.ERROR_5010.getException();
    }

    this.fileSize = SnowflakeSinkConnectorConfig.BUFFER_SIZE_BYTES_DEFAULT;
    this.recordNum = SnowflakeSinkConnectorConfig.BUFFER_COUNT_RECORDS_DEFAULT;
    this.flushTime = SnowflakeSinkConnectorConfig.BUFFER_FLUSH_TIME_SEC_DEFAULT;
    this.pipes = new HashMap<>();
    this.conn = conn;
    isStopped = false;
    this.telemetryService = conn.getTelemetryClient();
    this.recordService = RecordServiceFactory.createRecordService(false, false);
    this.topic2TableMap = new HashMap<>();
    this.v2CleanerIntervalSeconds = v2CleanerIntervalSeconds;

    // Setting the default value in constructor
    // meaning it will not ignore the null values (Tombstone records wont be ignored/filtered)
    this.behaviorOnNullValues = SnowflakeSinkConnectorConfig.BehaviorOnNullValues.DEFAULT;
  }

  /**
   * Create new ingestion task from existing table and stage, tries to reuse existing pipe and
   * recover previous task, otherwise, create a new pipe.
   *
   * @param tableName destination table name in Snowflake
   * @param topicPartition TopicPartition passed from Kafka
   */
  @Override
  public void startPartition(final String tableName, final TopicPartition topicPartition) {
    Utils.GeneratedName generatedTableName =
        Utils.generateTableName(topicPartition.topic(), topic2TableMap);
    if (!tableName.equals(generatedTableName.getName())) {
      LOGGER.warn(
          "tableNames do not match, this is acceptable in tests but not in production! Resorting to"
              + " originalName and assuming no potential clashes on file prefixes. original={},"
              + " recalculated={}",
          tableName,
          generatedTableName.getName());
      generatedTableName = Utils.GeneratedName.generated(tableName);
    }
    String stageName = Utils.stageName(conn.getConnectorName(), tableName);
    String nameIndex = getNameIndex(topicPartition.topic(), topicPartition.partition());
    if (pipes.containsKey(nameIndex)) {
      LOGGER.warn("task is already registered with {} partition", nameIndex);
    } else {
      String pipeName =
          Utils.pipeName(conn.getConnectorName(), tableName, topicPartition.partition());

      pipes.put(
          nameIndex,
          new ServiceContext(
              generatedTableName,
              stageName,
              pipeName,
              topicPartition.topic(),
              conn,
              topicPartition.partition(),
              cleanerServiceExecutor,
              v2CleanerIntervalSeconds));

      if (enableStageFilePrefixExtension
          && TopicToTableModeExtractor.determineTopic2TableMode(
                  topic2TableMap, topicPartition.topic())
              == TopicToTableModeExtractor.Topic2TableMode.MANY_TOPICS_SINGLE_TABLE) {
        // if snowflake.snowpipe.stageFileNameExtensionEnabled is enabled and table is used by
        // multiple topics, we may end up in a situation, when data from different topics may have
        // ended up in the same bucket - after enabling this fix, that data will stay on stage
        // forever - we want to give user information about such situation and we will list all
        // files, which wouldn't be processed by connector anymore.
        String key = String.format("%s-%d", tableName, topicPartition.partition());
        synchronized (perTableWarningNotifications) {
          if (!perTableWarningNotifications.contains(key)) {
            perTableWarningNotifications.add(key);
            ForkJoinPool.commonPool()
                .submit(
                    () ->
                        checkTableStageForObsoleteFiles(
                            stageName, tableName, topicPartition.partition()));
          }
        }
      }
    }
  }

  @Override
  public void startPartitions(
      Collection<TopicPartition> partitions, Map<String, String> topic2Table) {
    partitions.forEach(tp -> this.startPartition(Utils.tableName(tp.topic(), topic2Table), tp));
  }

  @Override
  public void insert(final Collection<SinkRecord> records) {
    if (LOGGER.isTraceEnabled()) {
      Pair<String, String> offsets = getOffsets(records);
      LOGGER.debug(
          "Inserting {} records, firstOffset: {}, lastOffset: {}",
          records.size(),
          offsets.getLeft(),
          offsets.getRight());
    }

    // note that records can be empty
    for (SinkRecord record : records) {
      // check if it needs to handle null value records
      if (recordService.shouldSkipNullValue(record, behaviorOnNullValues)) {
        continue;
      }
      // Might happen a count of record based flushing
      insert(record);
    }

    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace(
          "Checking all sink context to see if they need to be flushed based on time: {}",
          Arrays.toString(pipes.values().toArray()));
    }

    for (ServiceContext pipe : pipes.values()) {
      // Time based flushing
      if (pipe.shouldFlush()) {
        pipe.flushBuffer();
      }
    }
  }

  private Pair<String, String> getOffsets(Collection<SinkRecord> records) {
    if (isNull(records) || records.isEmpty()) {
      return Pair.of("<empty>", "<empty>");
    }

    String first =
        records.stream()
            .map(SinkRecord::kafkaOffset)
            .reduce(Long::min)
            .map(String::valueOf)
            .orElse("<empty>");

    String last =
        records.stream()
            .map(SinkRecord::kafkaOffset)
            .reduce(Long::max)
            .map(String::valueOf)
            .orElse("<empty>");

    return Pair.of(first, last);
  }

  @Override
  public void insert(SinkRecord record) {
    String nameIndex = getNameIndex(record.topic(), record.kafkaPartition());
    // init a new topic partition
    if (!pipes.containsKey(nameIndex)) {
      LOGGER.warn(
          "Topic: {} Partition: {} hasn't been initialized by OPEN " + "function",
          record.topic(),
          record.kafkaPartition());
      startPartition(
          Utils.tableName(record.topic(), this.topic2TableMap),
          new TopicPartition(record.topic(), record.kafkaPartition()));
    }
    LOGGER.trace("Inserting record for pipe {} with offset {}", nameIndex, record.kafkaOffset());
    pipes.get(nameIndex).insert(record);
  }

  @Override
  public long getOffset(final TopicPartition topicPartition) {
    String name = getNameIndex(topicPartition.topic(), topicPartition.partition());
    if (pipes.containsKey(name)) {
      return pipes.get(name).getOffset();
    } else {
      LOGGER.warn(
          "Topic: {} Partition: {} hasn't been initialized to get offset",
          topicPartition.topic(),
          topicPartition.partition());
      return 0;
    }
  }

  @Override
  public int getPartitionCount() {
    return pipes.size();
  }

  // used for testing only
  @Override
  public void callAllGetOffset() {
    for (ServiceContext pipe : pipes.values()) {
      pipe.getOffset();
    }
  }

  @Override
  public void close(Collection<TopicPartition> partitions) {
    partitions.forEach(
        tp -> {
          String name = getNameIndex(tp.topic(), tp.partition());
          ServiceContext sc = pipes.remove(name);
          if (sc != null) {
            try {
              sc.close();
            } catch (Exception e) {
              LOGGER.error(
                  "Failed to close sink service for Topic: {}, Partition: " + "{}\nMessage:{}",
                  tp.topic(),
                  tp.partition(),
                  e.getMessage());
            } finally {
              sc.unregisterPipeJMXMetrics();
            }
          } else {
            LOGGER.warn(
                "Failed to close sink service for Topic: {}, Partition: {}, "
                    + "sink service hasn't been initialized",
                tp.topic(),
                tp.partition());
          }
        });
  }

  @Override
  public void closeAll() {
    this.isStopped = true; // release all cleaner and flusher threads
    pipes.forEach(
        (name, context) -> {
          context.close();
          context.unregisterPipeJMXMetrics();
        });
    pipes.clear();
  }

  @Override
  public void stop() {
    if (cleanerServiceExecutor != null) {
      cleanerServiceExecutor.shutdown();
      cleanerServiceExecutor = null;
    }
    this.isStopped = true; // release all cleaner and flusher threads
  }

  @Override
  public boolean isClosed() {
    return this.isStopped;
  }

  @Override
  public void setRecordNumber(final long num) {
    if (num < 0) {
      LOGGER.error("number of record in each file is {}, it is negative, reset to" + " 0");
      this.recordNum = 0;
    } else {
      this.recordNum = num;
      LOGGER.info("set number of record limitation to {}", num);
    }
  }

  @Override
  public void setFileSize(final long size) {
    if (size < SnowflakeSinkConnectorConfig.BUFFER_SIZE_BYTES_MIN) {
      LOGGER.error(
          "file size is {} bytes, it is smaller than the minimum file "
              + "size {} bytes, reset to the default file size",
          size,
          SnowflakeSinkConnectorConfig.BUFFER_SIZE_BYTES_DEFAULT);
      this.fileSize = SnowflakeSinkConnectorConfig.BUFFER_SIZE_BYTES_DEFAULT;
    } else {
      this.fileSize = size;
      LOGGER.info("set file size limitation to {} bytes", size);
    }
  }

  @Override
  public void setFlushTime(final long time) {
    if (time < SnowflakeSinkConnectorConfig.BUFFER_FLUSH_TIME_SEC_MIN) {
      LOGGER.error(
          "flush time is {} seconds, it is smaller than the minimum "
              + "flush time {} seconds, reset to the minimum flush time",
          time,
          SnowflakeSinkConnectorConfig.BUFFER_FLUSH_TIME_SEC_MIN);
      this.flushTime = SnowflakeSinkConnectorConfig.BUFFER_FLUSH_TIME_SEC_MIN;
    } else {
      this.flushTime = time;
      LOGGER.info("set flush time to {} seconds", time);
    }
  }

  @Override
  public void setTopic2TableMap(Map<String, String> topic2TableMap) {
    this.topic2TableMap = topic2TableMap;
  }

  @Override
  public void setMetadataConfig(SnowflakeMetadataConfig configMap) {
    this.recordService.setMetadataConfig(configMap);
  }

  private long getRecordNumber() {
    return this.recordNum;
  }

  private long getFlushTime() {
    return this.flushTime;
  }

  private long getFileSize() {
    return this.fileSize;
  }

  @Override
  public void setBehaviorOnNullValuesConfig(
      SnowflakeSinkConnectorConfig.BehaviorOnNullValues behavior) {
    this.behaviorOnNullValues = behavior;
  }

  // enable use of new stage files processor
  void enableStageFilesProcessor(int threadCount) {
    if (cleanerServiceExecutor != null) {
      cleanerServiceExecutor.shutdown();
    }
    cleanerServiceExecutor = new ScheduledThreadPoolExecutor(Math.max(1, threadCount));
  }

  @Override
  public void setCustomJMXMetrics(boolean enableJMX) {
    this.enableCustomJMXMonitoring = enableJMX;
  }

  @Override
  public SnowflakeSinkConnectorConfig.BehaviorOnNullValues getBehaviorOnNullValuesConfig() {
    return this.behaviorOnNullValues;
  }

  /**
   * Loop through all pipes in memory and find out the metric registry instance for that pipe. The
   * pipes object's key is not pipeName hence need to loop over.
   *
   * @param pipeName associated MetricRegistry to fetch
   * @return Optional MetricRegistry. (Empty if pipe was not found in pipes map)
   */
  @Override
  public Optional<MetricRegistry> getMetricRegistry(final String pipeName) {
    for (Map.Entry<String, ServiceContext> entry : this.pipes.entrySet()) {
      if (entry.getValue().pipeName.equalsIgnoreCase(pipeName)) {
        return Optional.of(entry.getValue().getMetricRegistry());
      }
    }
    return Optional.empty();
  }

  @VisibleForTesting
  protected static String getNameIndex(String topic, int partition) {
    return topic + "_" + partition;
  }

  public void configureSingleTableLoadFromMultipleTopics(boolean fixEnabled) {
    enableStageFilePrefixExtension = fixEnabled;
  }

  /**
   * util method, checks if there are stage files present matching "appName/table/partition/" file
   * name format, if they are - lists them and asks user to manually delete them. The file format
   * for tables used by multiple topics is "appName/table/{hashOf(tableName) << 16 | 0x8000 |
   * partition}/"
   */
  private void checkTableStageForObsoleteFiles(String stageName, String tableName, int partition) {
    try {
      String prefix = FileNameUtils.filePrefix(conn.getConnectorName(), tableName, null, partition);
      List<String> stageFiles = conn.listStage(stageName, prefix);
      if (!stageFiles.isEmpty()) {
        LOGGER.warn(
            "NOTE: For table {} there are {} files matching {} prefix.",
            tableName,
            stageFiles.size(),
            prefix);
        stageFiles.sort(String::compareToIgnoreCase);
        LOGGER.warn("Please consider manually deleting these files:");
        for (List<String> names : Lists.partition(stageFiles, 10)) {
          LOGGER.warn(String.join(", ", names));
        }
      }
    } catch (Exception err) {
      LOGGER.warn("could not query stage - {}<{}>", err.getMessage(), err.getClass().getName());
    }
  }

  public void configureEnableReprocessFilesCleanup(boolean enable) {
    enableReprocessFilesCleanup = enable;
  }

  private class ServiceContext {
    private final String tableName;
    private final String stageName;
    private final String pipeName;
    private final SnowflakeConnectionService conn;
    private final SnowflakeIngestionService ingestionService;
    private List<String> fileNames;

    // Includes a list of files:
    // 1. Which are added after a flush into internal stage is successful
    // 2. While an app restarts and we do list on an internal stage to find out what needs to be
    // done on leaked files.
    private List<String> cleanerFileNames;
    private SnowpipeBuffer buffer;
    private final String prefix;
    private final AtomicLong committedOffset; // loaded offset + 1
    private final AtomicLong flushedOffset; // flushed offset (file on stage)
    private final AtomicLong processedOffset; // processed offset
    private long previousFlushTimeStamp;

    // threads
    @Nullable private final ExecutorService cleanerExecutor;
    @Nullable private final ExecutorService reprocessCleanerExecutor;
    private final Lock bufferLock;
    private final Lock fileListLock;
    // v2 file cleaner logic - either cleaner executors or stageFileProcessorClient is used
    private final boolean useStageFilesProcessor;
    @Nullable private final StageFilesProcessor.ProgressRegister stageFileProcessorClient;

    // telemetry
    private final SnowflakeTelemetryPipeStatus pipeStatus;
    // non null
    private final MetricRegistry metricRegistry;

    // Wrapper on Metric registry instance which will hold all registered metrics for this pipe
    private final MetricsJmxReporter metricsJmxReporter;

    // buffer metrics, updated everytime when a buffer is flushed to internal stage
    private Histogram partitionBufferSizeBytesHistogram; // in Bytes
    private Histogram partitionBufferCountHistogram;

    // make the initialization lazy
    private boolean hasInitialized = false;
    private boolean forceCleanerFileReset = false;

    private ServiceContext(
        Utils.GeneratedName generatedTableName,
        String stageName,
        String pipeName,
        String topicName,
        SnowflakeConnectionService conn,
        int partition,
        ScheduledExecutorService v2CleanerExecutor,
        long v2CleanerIntervalSeconds) {
      this.pipeName = pipeName;
      this.tableName = generatedTableName.getName();
      this.stageName = stageName;
      this.conn = conn;
      this.fileNames = new LinkedList<>();
      this.cleanerFileNames = new LinkedList<>();
      this.buffer = new SnowpipeBuffer();
      this.ingestionService = conn.buildIngestService(stageName, pipeName);
      // SNOW-1642799 = if multiple topics load data into single table, we need to ensure the file
      // prefix is unique per topic - otherwise, file cleaners for different topics will try to
      // clean the same prefixed files creating a race condition and a potential to delete
      // not yet ingested files created by another topic
      if (generatedTableName.isNameFromMap() && !enableStageFilePrefixExtension) {
        LOGGER.warn(
            "The table {} may be used as ingestion target by multiple topics - including this one"
                + " '{}'.\nTo prevent potential data loss consider setting '{}' to true",
            tableName,
            topicName,
            SNOWPIPE_SINGLE_TABLE_MULTIPLE_TOPICS_FIX_ENABLED);
      }
      {
        final String topicForPrefix =
            generatedTableName.isNameFromMap() && enableStageFilePrefixExtension ? topicName : "";
        this.prefix =
            FileNameUtils.filePrefix(conn.getConnectorName(), tableName, topicForPrefix, partition);
      }
      this.processedOffset = new AtomicLong(-1);
      this.flushedOffset = new AtomicLong(-1);
      this.committedOffset = new AtomicLong(0);
      this.previousFlushTimeStamp = System.currentTimeMillis();

      this.bufferLock = new ReentrantLock();
      this.fileListLock = new ReentrantLock();
      this.metricRegistry = new MetricRegistry();
      this.metricsJmxReporter =
          new MetricsJmxReporter(this.metricRegistry, conn.getConnectorName());

      this.pipeStatus =
          new SnowflakeTelemetryPipeStatus(
              tableName,
              stageName,
              pipeName,
              partition,
              enableCustomJMXMonitoring,
              this.metricsJmxReporter);

      if (enableCustomJMXMonitoring) {
        partitionBufferCountHistogram =
            this.metricRegistry.histogram(
                MetricsUtil.constructMetricName(pipeName, BUFFER_SUB_DOMAIN, BUFFER_RECORD_COUNT));
        partitionBufferSizeBytesHistogram =
            this.metricRegistry.histogram(
                MetricsUtil.constructMetricName(pipeName, BUFFER_SUB_DOMAIN, BUFFER_SIZE_BYTES));
        LOGGER.info(
            "Registered {} metrics for pipeName:{}", metricRegistry.getMetrics().size(), pipeName);
      }

      this.useStageFilesProcessor = v2CleanerExecutor != null;
      if (useStageFilesProcessor) {
        LOGGER.info("Using StageFileProcessor");

        StageFilesProcessor processor =
            new StageFilesProcessor(
                pipeName,
                tableName,
                stageName,
                prefix,
                topicName,
                partition,
                conn,
                ingestionService,
                pipeStatus,
                telemetryService,
                v2CleanerExecutor,
                v2CleanerIntervalSeconds);
        this.stageFileProcessorClient = processor.trackFilesAsync();
        this.cleanerExecutor = null;
        this.reprocessCleanerExecutor = null;
      } else {
        LOGGER.info("Using cleaner executor");

        this.cleanerExecutor = Executors.newSingleThreadExecutor();
        this.reprocessCleanerExecutor = Executors.newSingleThreadExecutor();
        this.stageFileProcessorClient = null;
      }

      LOGGER.info("pipe: {} - service started", pipeName);
    }

    private void init(long recordOffset) {
      LOGGER.info("init pipe: {}", pipeName);
      SnowflakeTelemetryPipeCreation pipeCreation =
          new SnowflakeTelemetryPipeCreation(tableName, stageName, pipeName);

      // wait for sinkConnector to start
      createTableAndStage(pipeCreation);
      // recover will only check pipe status and create pipe if it does not exist.
      recover(pipeCreation);

      if (!useStageFilesProcessor) {
        try {
          LOGGER.info("Starting cleaner with offset: {}", recordOffset);
          startCleaner(recordOffset, pipeCreation);
        } catch (Exception e) {
          LOGGER.warn("Cleaner and Flusher threads shut down before initialization");
        }
        // with v2 cleaner enabled, this event is raised by the cleaner itself
        telemetryService.reportKafkaPartitionStart(pipeCreation);
      }
    }

    private boolean resetCleanerFiles() {
      try {
        LOGGER.warn("Resetting cleaner files {}", pipeName);
        pipeStatus.incrementAndGetCleanerRestartCount();
        // list stage again and try to clean the files leaked on stage
        // this can throw unchecked, it needs to be wrapped in a try/catch
        // if it fails again do not reset forceCleanerFileReset
        List<String> tmpCleanerFileNames = conn.listStage(stageName, prefix);
        fileListLock.lock();
        try {
          cleanerFileNames.addAll(tmpCleanerFileNames);
          cleanerFileNames = cleanerFileNames.stream().distinct().collect(Collectors.toList());
        } finally {
          fileListLock.unlock();
        }
        forceCleanerFileReset = false;
        LOGGER.warn("Resetting cleaner files {} done", pipeName);
        if (LOGGER.isInfoEnabled()) {
          LOGGER.info(
              "For pipe {} cleaner files after reset: {}",
              pipeName,
              String.join(", ", cleanerFileNames));
        }
      } catch (Throwable t) {
        LOGGER.warn("Cleaner file reset encountered an error:\n{}", t.getMessage());
      }

      return forceCleanerFileReset;
    }

    // If there are files already on stage, we need to decide whether we will reprocess the offsets
    // or we will purge them.
    private void startCleaner(long recordOffset, SnowflakeTelemetryPipeCreation pipeCreation) {
      // When cleaner start, scan stage for all files of this pipe.
      // If we know that we are going to reprocess the file, then safely delete the file.
      List<String> currentFilesOnStage = conn.listStage(stageName, prefix);
      List<String> reprocessFiles = new ArrayList<>();

      if (enableReprocessFilesCleanup) {
        filterFileReprocess(currentFilesOnStage, reprocessFiles, recordOffset);
      }

      // Telemetry
      pipeCreation.setFileCountRestart(currentFilesOnStage.size());
      pipeCreation.setFileCountReprocessPurge(reprocessFiles.size());
      // Files left on stage must be on ingestion, otherwise offset won't be committed and
      // the file will be removed by the reprocess filter.
      pipeStatus.addAndGetFileCountOnIngestion(currentFilesOnStage.size());
      pipeStatus.addAndGetFileCountOnStage(currentFilesOnStage.size());

      fileListLock.lock();
      try {
        cleanerFileNames.addAll(currentFilesOnStage);
      } finally {
        fileListLock.unlock();
      }

      cleanerExecutor.submit(
          () -> {
            LOGGER.info("pipe {}: cleaner started", pipeName);
            while (!isStopped) {
              try {
                telemetryService.reportKafkaPartitionUsage(pipeStatus, false);
                Thread.sleep(CLEAN_TIME);

                if (forceCleanerFileReset && resetCleanerFiles()) {
                  continue;
                }

                checkStatus();
              } catch (InterruptedException e) {
                LOGGER.info("Cleaner terminated by an interrupt:\n{}", e.getMessage());
                break;
              } catch (Exception e) {
                LOGGER.warn(
                    "Cleaner encountered an exception {}:\n{}\n{}",
                    e.getClass(),
                    e.getMessage(),
                    e.getStackTrace());
                telemetryService.reportKafkaConnectFatalError(e.getMessage());
                forceCleanerFileReset = true;
              }
            }
          });

      if (enableReprocessFilesCleanup && reprocessFiles.size() > 0) {
        // After we start the cleaner thread, delay a while and start deleting files.
        reprocessCleanerExecutor.submit(
            () -> {
              try {
                Thread.sleep(CLEAN_TIME);
                LOGGER.info(
                    "Purging files already present on the stage before start. ReprocessFileSize:{}",
                    reprocessFiles.size());
                purge(reprocessFiles);
              } catch (Exception e) {
                LOGGER.error(
                    "Reprocess cleaner encountered an exception {}:\n{}\n{}",
                    e.getClass(),
                    e.getMessage(),
                    e.getStackTrace());
              }
            });
      }
    }

    /**
     * Does in place manipulation of passed currentFilesOnStage. The caller of this function passes
     * in the list of files(name) on the stage. (ls @stageName)
     *
     * <p>In return it expects the list of files (reprocessFiles) which is a subset of
     * currentFilesOnStage.
     *
     * <p>How do we find list of reprocessFiles?
     *
     * <p>1. Find out the start offset from the list of files currently on stage.
     *
     * <p>2. If the current offset passed by the connector is less than any of the start offset of
     * found files, we will reprocess this files and at the same time remove from
     * currentListOfFiles. (Idea being if the current offset is still found on stage, it is not
     * purged, so we will reprocess)
     *
     * @param currentFilesOnStage LIST.OF((ls @stageNAME))
     * @param reprocessFiles Empty but we will fill this.
     * @param recordOffset current offset
     */
    private void filterFileReprocess(
        List<String> currentFilesOnStage, List<String> reprocessFiles, long recordOffset) {
      // iterate over a copy since reprocess files get removed from it
      new LinkedList<>(currentFilesOnStage)
          .forEach(
              name -> {
                long fileStartOffset = FileNameUtils.fileNameToStartOffset(name);
                // If start offset of this file is greater than the offset of the record that is
                // sent to the connector,
                // all content of this file will be reprocessed. Thus this file can be deleted.
                if (recordOffset <= fileStartOffset) {
                  reprocessFiles.add(name);
                  currentFilesOnStage.remove(name);
                }
              });
    }

    private void stopCleaner() {
      cleanerExecutor.shutdownNow();
      reprocessCleanerExecutor.shutdownNow();
      LOGGER.info("pipe {}: cleaner terminated", pipeName);
    }

    private void insert(final SinkRecord record) {
      // init pipe
      if (!hasInitialized) {
        LOGGER.info("Initializing with offset: {}", record.kafkaOffset());
        // This will only be called once at the beginning when an offset arrives for first time
        // after connector starts/rebalance
        init(record.kafkaOffset());
        metricsJmxReporter.start();
        this.hasInitialized = true;
      }
      // only get offset token once when service context is initialized
      // ignore ingested filesg
      if (record.kafkaOffset() > processedOffset.get()) {
        SinkRecord snowflakeRecord = record;
        if (shouldConvertContent(snowflakeRecord.value())) {
          LOGGER.trace("Converting native record value, offset: {}", snowflakeRecord.kafkaOffset());
          snowflakeRecord = handleNativeRecord(snowflakeRecord, false);
        }
        if (shouldConvertContent(snowflakeRecord.key())) {
          LOGGER.trace("Converting native record key, offset: {}", snowflakeRecord.kafkaOffset());
          snowflakeRecord = handleNativeRecord(snowflakeRecord, true);
        }

        // broken record
        if (isRecordBroken(snowflakeRecord)) {
          LOGGER.warn(
              "Writing broken record to a table stage, offset: {},", snowflakeRecord.kafkaOffset());
          writeBrokenDataToTableStage(snowflakeRecord);
          // don't move committed offset in this case
          // only move it in the normal cases
        } else {
          // lag telemetry, note that sink record timestamp might be null
          if (snowflakeRecord.timestamp() != null
              && snowflakeRecord.timestampType() != NO_TIMESTAMP_TYPE) {
            pipeStatus.updateKafkaLag(System.currentTimeMillis() - snowflakeRecord.timestamp());
          }

          SnowpipeBuffer tmpBuff = null;
          bufferLock.lock();
          try {
            processedOffset.set(snowflakeRecord.kafkaOffset());
            pipeStatus.setProcessedOffset(snowflakeRecord.kafkaOffset());
            buffer.insert(snowflakeRecord);
            if (buffer.getBufferSizeBytes() >= getFileSize()
                || (getRecordNumber() != 0 && buffer.getNumOfRecords() >= getRecordNumber())) {
              LOGGER.info(
                  "Buffer ready to flush, moving content to a temporary buffer, buffer details: {}",
                  buffer);
              tmpBuff = buffer;
              this.buffer = new SnowpipeBuffer();
            }
          } finally {
            bufferLock.unlock();
          }

          if (useStageFilesProcessor) {
            LOGGER.trace("Assigning {} offset to StageFileProcessor", record.kafkaOffset());
            stageFileProcessorClient.newOffset(record.kafkaOffset());
          }

          if (tmpBuff != null) {
            flush(tmpBuff);
          }
        }
      }
    }

    private boolean shouldConvertContent(final Object content) {
      return content != null && !(content instanceof SnowflakeRecordContent);
    }

    private boolean isRecordBroken(final SinkRecord record) {
      return isContentBroken(record.value()) || isContentBroken(record.key());
    }

    private boolean isContentBroken(final Object content) {
      return content != null && ((SnowflakeRecordContent) content).isBroken();
    }

    private SinkRecord handleNativeRecord(SinkRecord record, boolean isKey) {
      SnowflakeRecordContent newSFContent;
      Schema schema = isKey ? record.keySchema() : record.valueSchema();
      Object content = isKey ? record.key() : record.value();
      try {
        newSFContent = new SnowflakeRecordContent(schema, content, false);
      } catch (Exception e) {
        LOGGER.error("Native content parser error:\n{}", e.getMessage());
        try {
          // try to serialize this object and send that as broken record
          ByteArrayOutputStream out = new ByteArrayOutputStream();
          ObjectOutputStream os = new ObjectOutputStream(out);
          os.writeObject(content);
          newSFContent = new SnowflakeRecordContent(out.toByteArray());
        } catch (Exception serializeError) {
          LOGGER.error(
              "Failed to convert broken native record to byte data:\n{}",
              serializeError.getMessage());
          throw e;
        }
      }
      // create new sinkRecord
      Schema keySchema = isKey ? new SnowflakeJsonSchema() : record.keySchema();
      Object keyContent = isKey ? newSFContent : record.key();
      Schema valueSchema = isKey ? record.valueSchema() : new SnowflakeJsonSchema();
      Object valueContent = isKey ? record.value() : newSFContent;
      return new SinkRecord(
          record.topic(),
          record.kafkaPartition(),
          keySchema,
          keyContent,
          valueSchema,
          valueContent,
          record.kafkaOffset(),
          record.timestamp(),
          record.timestampType(),
          record.headers());
    }

    private boolean shouldFlush() {
      return (System.currentTimeMillis() - this.previousFlushTimeStamp) >= (getFlushTime() * 1000);
    }

    private void flushBuffer() {

      // Just checking buffer size, no atomic operation required
      if (buffer.isEmpty()) {
        LOGGER.info(
            "Buffer for pipe: {}, tableName: {}, stageName: {}, nothing to be flushed",
            pipeName,
            tableName,
            stageName);
        return;
      }

      LOGGER.info(
          "Flushing buffer for pipe: {}, tableName: {}, stageName: {}",
          pipeName,
          tableName,
          stageName);
      SnowpipeBuffer tmpBuff;
      bufferLock.lock();
      try {
        tmpBuff = buffer;
        this.buffer = new SnowpipeBuffer();
      } finally {
        bufferLock.unlock();
      }
      flush(tmpBuff);

      LOGGER.info(
          "Buffer flushed for pipe: {}, tableName: {}, stageName: {}",
          pipeName,
          tableName,
          stageName);
    }

    private void writeBrokenDataToTableStage(SinkRecord record) {
      SnowflakeRecordContent key = (SnowflakeRecordContent) record.key();
      SnowflakeRecordContent value = (SnowflakeRecordContent) record.value();
      if (key != null) {
        String fileName = FileNameUtils.brokenRecordFileName(prefix, record.kafkaOffset(), true);
        conn.putToTableStage(tableName, fileName, snowflakeContentToByteArray(key));
        pipeStatus.updateBrokenRecordMetrics(1l);
      }
      if (value != null) {
        String fileName = FileNameUtils.brokenRecordFileName(prefix, record.kafkaOffset(), false);
        conn.putToTableStage(tableName, fileName, snowflakeContentToByteArray(value));
        pipeStatus.updateBrokenRecordMetrics(1l);
      }
    }

    private byte[] snowflakeContentToByteArray(SnowflakeRecordContent content) {
      if (content == null) {
        return null;
      }
      if (content.isBroken()) {
        return content.getBrokenData();
      }
      return Arrays.asList(content.getData()).toString().getBytes();
    }

    private long getOffset() {
      if (fileNames.isEmpty()) {
        long offsetToReturn = committedOffset.get();
        LOGGER.info("No files to commit, returning {} offset", offsetToReturn);
        return offsetToReturn;
      }

      List<String> fileNamesCopy = new ArrayList<>();
      List<String> fileNamesForMetrics = new ArrayList<>();
      fileListLock.lock();
      try {
        fileNamesCopy.addAll(fileNames);
        fileNamesForMetrics.addAll(fileNames);
        fileNames = new LinkedList<>();
      } finally {
        fileListLock.unlock();
      }

      LOGGER.info("pipe {}, ingest files: {}", pipeName, fileNamesCopy);

      ingestionService.ingestFiles(fileNamesCopy);

      LOGGER.info("pipe {}, ingested files: {}", pipeName, fileNamesCopy);

      // committedOffset should be updated only when ingestFiles has succeeded.
      long flushedOffset = this.flushedOffset.get();
      LOGGER.info("Setting commitedOffset to {}", flushedOffset);
      committedOffset.set(flushedOffset);

      // update telemetry data
      long currentTime = System.currentTimeMillis();
      pipeStatus.setCommittedOffset(committedOffset.get() - 1);
      pipeStatus.addAndGetFileCountOnIngestion(fileNamesForMetrics.size());
      fileNamesForMetrics.forEach(
          name ->
              pipeStatus.updateCommitLag(currentTime - FileNameUtils.fileNameToTimeIngested(name)));

      return committedOffset.get();
    }

    private void flush(final SnowpipeBuffer buff) {
      if (buff == null || buff.isEmpty()) {
        LOGGER.info("Buffer empty, nothing to be flushed");
        return;
      }
      this.previousFlushTimeStamp = System.currentTimeMillis();

      // If we failed to submit/put, throw an runtime exception that kills the connector.
      // SnowflakeThreadPoolUtils.flusherThreadPool.submit(
      String fileName = FileNameUtils.fileName(prefix, buff.getFirstOffset(), buff.getLastOffset());
      String content = buff.getData();
      LOGGER.info("Putting buffer to stage: {}", fileName);
      conn.putWithCache(stageName, fileName, content);

      // compute metrics which will be exported to JMX for now.
      // TODO: Send it to Telemetry API too
      computeBufferMetrics(buff);

      // This is safe and atomic
      flushedOffset.updateAndGet((value) -> Math.max(buff.getLastOffset() + 1, value));
      pipeStatus.setFlushedOffset(flushedOffset.get() - 1);
      pipeStatus.addAndGetFileCountOnStage(1L); // plus one
      pipeStatus.resetMemoryUsage();

      fileListLock.lock();
      try {
        fileNames.add(fileName);
        if (useStageFilesProcessor) {
          stageFileProcessorClient.registerNewStageFile(fileName);
        } else {
          cleanerFileNames.add(fileName);
        }
      } finally {
        fileListLock.unlock();
      }

      LOGGER.info("pipe {}, flush pipe: {}", pipeName, fileName);
    }

    private void checkStatus() {
      // We are using a temporary list which will reset the cleanerFileNames
      // After this checkStatus() call, we will have an updated cleanerFileNames which are subset of
      // existing cleanerFileNames
      // this time th
      List<String> tmpFileNames;

      fileListLock.lock();
      try {
        tmpFileNames = cleanerFileNames;
        cleanerFileNames = new LinkedList<>();
      } finally {
        fileListLock.unlock();
      }

      long currentTime = System.currentTimeMillis();
      List<String> loadedFiles = new LinkedList<>();
      List<String> failedFiles = new LinkedList<>();

      // ingest report
      // This will update the loadedFiles (successfully loaded) &
      // failedFiles: PARTIAL + FAILED
      // In any cases tmpFileNames will be updated.
      // If we get all files in ingestReport, tmpFileNames will be empty
      filterResultFromSnowpipeScan(
          ingestionService.readIngestReport(tmpFileNames), tmpFileNames, loadedFiles, failedFiles);

      // old files
      List<String> oldFiles = new LinkedList<>();

      // iterate over a copy since failed files get removed from it
      // Iterate over those files which were not found in ingest report call and are sitting more
      // than an hour earlier.
      // Also add those files into oldFiles which are not purged/found in ingestReport since last 10
      // minutes.
      new LinkedList<>(tmpFileNames)
          .forEach(
              name -> {
                long time = FileNameUtils.fileNameToTimeIngested(name);
                if (time < currentTime - ONE_HOUR) {
                  failedFiles.add(name);
                  tmpFileNames.remove(name);
                } else if (time < currentTime - TEN_MINUTES) {
                  oldFiles.add(name);
                }
              });
      // load history
      // Use loadHistoryScan API to scan last one hour of data and if filter files from above
      // filtered list.
      // This is the last filtering we do and after this, we start purging loadedFiles and moving
      // failedFiles to tableStage
      if (!oldFiles.isEmpty()) {
        filterResultFromSnowpipeScan(
            ingestionService.readOneHourHistory(tmpFileNames, currentTime - ONE_HOUR),
            tmpFileNames,
            loadedFiles,
            failedFiles);
      }
      purge(loadedFiles);

      moveToTableStage(failedFiles);

      fileListLock.lock();
      try {
        // Add back all those files which were neither found in ingestReport nor in loadHistoryScan
        cleanerFileNames.addAll(tmpFileNames);
      } finally {
        fileListLock.unlock();
      }

      // update purged offset in telemetry
      loadedFiles.forEach(
          name ->
              pipeStatus.setPurgedOffsetAtomically(
                  value -> Math.max(FileNameUtils.fileNameToEndOffset(name), value)));
      // update file count in telemetry
      int fileCountRemovedFromStage = loadedFiles.size() + failedFiles.size();
      pipeStatus.addAndGetFileCountOnStage(-fileCountRemovedFromStage);
      pipeStatus.addAndGetFileCountOnIngestion(-fileCountRemovedFromStage);
      pipeStatus.updateFailedIngestionMetrics(failedFiles.size());

      pipeStatus.addAndGetFileCountPurged(loadedFiles.size());
      // update lag information
      loadedFiles.forEach(
          name ->
              pipeStatus.updateIngestionLag(
                  currentTime - FileNameUtils.fileNameToTimeIngested(name)));
    }

    // fileStatus Map may include mapping of fileNames with their ingestion status.
    // It can be received either from insertReport API or loadHistoryScan
    private void filterResultFromSnowpipeScan(
        Map<String, InternalUtils.IngestedFileStatus> fileStatus,
        List<String> allFiles,
        List<String> loadedFiles,
        List<String> failedFiles) {
      fileStatus.forEach(
          (name, status) -> {
            switch (status) {
              case LOADED:
                loadedFiles.add(name);
                allFiles.remove(name);
                break;
              case FAILED:
              case PARTIALLY_LOADED:
                failedFiles.add(name);
                allFiles.remove(name);
                break;
              default:
                // otherwise, do nothing
            }
          });
    }

    private void purge(List<String> files) {
      if (!files.isEmpty()) {
        OffsetContinuityRanges offsets = searchForMissingOffsets(files);
        LOGGER.info(
            "Purging loaded files for pipe: {}, loadedFileCount: {}, continuousOffsets: {},"
                + " missingOffsets: {}",
            pipeName,
            files.size(),
            offsets.getContinuousOffsets(),
            offsets.getMissingOffsets());
        LOGGER.debug("Purging files: {}", files);
        conn.purgeStage(stageName, files);
      }
    }

    private void moveToTableStage(List<String> failedFiles) {
      if (!failedFiles.isEmpty()) {
        OffsetContinuityRanges offsets = searchForMissingOffsets(failedFiles);
        String baseLog =
            String.format(
                "Moving failed files for pipe: %s to tableStage failedFileCount: %d,"
                    + " continuousOffsets: %s, missingOffsets: %s",
                pipeName,
                failedFiles.size(),
                offsets.getContinuousOffsets(),
                offsets.getMissingOffsets());
        if (LOGGER.isDebugEnabled()) {
          LOGGER.info("{}, failedFiles: {}", baseLog, failedFiles);
        } else {
          LOGGER.info(baseLog);
        }
        conn.moveToTableStage(tableName, stageName, failedFiles);
      }
    }

    private void recover(SnowflakeTelemetryPipeCreation pipeCreation) {
      if (conn.pipeExist(pipeName)) {
        if (!conn.isPipeCompatible(tableName, stageName, pipeName)) {
          throw SnowflakeErrors.ERROR_5005.getException(
              "pipe name: " + pipeName, conn.getTelemetryClient());
        }
        LOGGER.info("pipe {}, recovered from existing pipe", pipeName);
        pipeCreation.setReusePipe(true);
      } else {
        LOGGER.info(
            "Creating pipe {} for stageName: {}, tableName: {}", pipeName, stageName, tableName);
        conn.createPipe(tableName, stageName, pipeName);
      }
    }

    private void close() {
      if (stageFileProcessorClient != null) {
        stageFileProcessorClient.close();
      } else {
        try {
          stopCleaner();
        } catch (Exception e) {
          LOGGER.warn("Failed to terminate Cleaner or Flusher");
        }
      }
      ingestionService.close();
      telemetryService.reportKafkaPartitionUsage(pipeStatus, true);
      LOGGER.info("pipe {}: service closed", pipeName);
    }

    /**
     * SinkConnector ans SinkTasks start at the same time, however, SinkTasks need create table and
     * wait SinkConnector to create stage. This method checks table and stage existence for at most
     * 120 times(10 min) And then throws exceptions if table or stage doesn't exit
     */
    private void createTableAndStage(SnowflakeTelemetryPipeCreation pipeCreation) {
      // create table if not exists
      if (conn.tableExist(tableName)) {
        if (conn.isTableCompatible(tableName)) {
          LOGGER.info("Using existing table {}.", tableName);
          pipeCreation.setReuseTable(true);
        } else {
          throw SnowflakeErrors.ERROR_5003.getException(
              "table name: " + tableName, telemetryService);
        }
      } else {
        LOGGER.info("Creating new table {}.", tableName);
        conn.createTable(tableName);
      }

      if (conn.stageExist(stageName)) {
        if (conn.isStageCompatible(stageName)) {
          LOGGER.info("Using existing stage {}.", stageName);
          pipeCreation.setReuseStage(true);
        } else {
          throw SnowflakeErrors.ERROR_5004.getException(
              "stage name: " + stageName, telemetryService);
        }
      } else {
        LOGGER.info("Creating new stage {}.", stageName);
        conn.createStage(stageName);
      }
    }

    /**
     * called when we flush the buffer to internal stage by calling put API.
     *
     * @param buffer that was pushed in stage
     */
    private void computeBufferMetrics(final SnowpipeBuffer buffer) {
      if (enableCustomJMXMonitoring) {
        partitionBufferSizeBytesHistogram.update(buffer.getBufferSizeBytes());
        partitionBufferCountHistogram.update(buffer.getNumOfRecords());
      }
    }

    /** Equivalent to unregistering all mbeans with a prefix JMX_METRIC_PREFIX */
    private void unregisterPipeJMXMetrics() {
      if (enableCustomJMXMonitoring) {
        metricsJmxReporter.removeMetricsFromRegistry(this.pipeName);
      }
    }

    /**
     * Get Metric registry instance of this pipe
     *
     * @return Metric Registry (Non Null)
     */
    public MetricRegistry getMetricRegistry() {
      return this.metricRegistry;
    }

    /**
     * Implementation of Buffer for Snowpipe based implementation of KC.
     *
     * <p>Please note {@link #insert(SinkRecord)} API is called from {@link
     * com.snowflake.kafka.connector.SnowflakeSinkTask#put(Collection)} API and it is possible the
     * buffered data is present across multiple PUT apis.
     *
     * <p>Check the usage of {@link #getData()} to understand when we would empty this buffer and
     * when we would generate files in internal stage for snowpipe to ingest later using Snowpipe's
     * REST APIs
     */
    private class SnowpipeBuffer extends PartitionBuffer<String> {
      private final StringBuilder stringBuilder;

      private SnowpipeBuffer() {
        super();
        stringBuilder = new StringBuilder();
      }

      @Override
      public void insert(SinkRecord record) {
        String data = recordService.getProcessedRecordForSnowpipe(record);
        if (getBufferSizeBytes() == 0L) {
          setFirstOffset(record.kafkaOffset());
        }

        stringBuilder.append(data);
        setNumOfRecords(getNumOfRecords() + 1);
        setBufferSizeBytes(getBufferSizeBytes() + data.length() * 2L); // 1 char = 2 bytes
        setLastOffset(record.kafkaOffset());
        pipeStatus.addAndGetMemoryUsage(data.length() * 2L);
      }

      public String getData() {
        String result = stringBuilder.toString();
        LOGGER.debug(
            "flush buffer: {} records, {} bytes, offset {} - {}",
            getNumOfRecords(),
            getBufferSizeBytes(),
            getFirstOffset(),
            getLastOffset());
        pipeStatus.addAndGetTotalSizeOfData(getBufferSizeBytes());
        pipeStatus.addAndGetTotalNumberOfRecord(getNumOfRecords());
        return result;
      }

      @Override
      public List<SinkRecord> getSinkRecords() {
        throw new UnsupportedOperationException(
            "SnowflakeSinkServiceV1 doesnt support getSinkRecords method");
      }
    }

    @Override
    public String toString() {
      return "ServiceContext{"
          + "tableName='"
          + tableName
          + '\''
          + ", stageName='"
          + stageName
          + '\''
          + ", pipeName='"
          + pipeName
          + '\''
          + ", fileNames="
          + Arrays.toString(fileNames.toArray())
          + ", prefix='"
          + prefix
          + '\''
          + ", committedOffset="
          + committedOffset
          + ", flushedOffset="
          + flushedOffset
          + ", processedOffset="
          + processedOffset
          + ", previousFlushTimeStamp="
          + previousFlushTimeStamp
          + ", useStageFilesProcessor="
          + useStageFilesProcessor
          + ", hasInitialized="
          + hasInitialized
          + '}';
    }
  }
}
