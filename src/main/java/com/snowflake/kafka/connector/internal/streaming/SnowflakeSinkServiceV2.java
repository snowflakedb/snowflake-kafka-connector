package com.snowflake.kafka.connector.internal.streaming;

import static com.snowflake.kafka.connector.Utils.getTableName;
import static com.snowflake.kafka.connector.internal.streaming.channel.TopicPartitionChannel.NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE;
import static com.snowflake.kafka.connector.internal.streaming.v2.PipeNameProvider.buildDefaultPipeName;

import com.codahale.metrics.MetricRegistry;
import com.google.common.annotations.VisibleForTesting;
import com.snowflake.kafka.connector.ConnectorConfigTools;
import com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams;
import com.snowflake.kafka.connector.config.SinkTaskConfig;
import com.snowflake.kafka.connector.config.SnowflakeValidation;
import com.snowflake.kafka.connector.dlq.KafkaRecordErrorReporter;
import com.snowflake.kafka.connector.internal.KCLogger;
import com.snowflake.kafka.connector.internal.SnowflakeConnectionService;
import com.snowflake.kafka.connector.internal.SnowflakeErrors;
import com.snowflake.kafka.connector.internal.SnowflakeSinkService;
import com.snowflake.kafka.connector.internal.metrics.MetricsJmxReporter;
import com.snowflake.kafka.connector.internal.metrics.TaskMetrics;
import com.snowflake.kafka.connector.internal.streaming.channel.TopicPartitionChannel;
import com.snowflake.kafka.connector.internal.streaming.v2.BackpressureException;
import com.snowflake.kafka.connector.internal.streaming.v2.client.StreamingClientPools;
import com.snowflake.kafka.connector.internal.streaming.v2.service.BatchOffsetFetcher;
import com.snowflake.kafka.connector.internal.streaming.v2.service.PartitionChannelManager;
import com.snowflake.kafka.connector.internal.streaming.v2.service.ThreadPools;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;

/**
 * This is per task configuration. A task can be assigned multiple partitions. Major methods are
 * startTask, insert, getOffset and close methods.
 *
 * <p>StartTask: Called when partitions are assigned. Responsible for generating the POJOs.
 *
 * <p>Insert and getOffset are called when {@link
 * com.snowflake.kafka.connector.SnowflakeSinkTask#put(Collection)} and {@link
 * com.snowflake.kafka.connector.SnowflakeSinkTask#preCommit(Map)} APIs are called.
 *
 * <p>This implementation of SinkService uses Streaming Snowpipe (Streaming Ingestion)
 *
 * <p>Hence this initializes the channel, opens, closes. The StreamingIngestChannel resides inside
 * {@link TopicPartitionChannel} which is per partition.
 */
public class SnowflakeSinkServiceV2 implements SnowflakeSinkService {

  private static final KCLogger LOGGER = new KCLogger(SnowflakeSinkServiceV2.class.getName());

  private final SnowflakeConnectionService conn;

  private final Optional<MetricsJmxReporter> metricsJmxReporter;
  private final String connectorName;

  private final SinkTaskConfig taskConfig;
  private final SinkTaskContext sinkTaskContext;

  private final BatchOffsetFetcher batchOffsetFetcher;

  private final PartitionChannelManager channelManager;
  private final TaskMetrics taskMetrics;

  /** Cooldown duration after a backpressure event before retrying inserts. */
  static final Duration BACKPRESSURE_COOLDOWN = Duration.ofSeconds(1);

  /** Timestamp until which all inserts are skipped due to backpressure. */
  @VisibleForTesting Instant backpressureUntil = Instant.MIN;

  public SnowflakeSinkServiceV2(
      SnowflakeConnectionService conn,
      SinkTaskConfig taskConfig,
      KafkaRecordErrorReporter recordErrorReporter,
      SinkTaskContext sinkTaskContext,
      Optional<MetricsJmxReporter> metricsJmxReporter,
      TaskMetrics taskMetrics) {
    this(
        conn,
        taskConfig,
        sinkTaskContext,
        metricsJmxReporter,
        () ->
            new BatchOffsetFetcher(
                taskConfig.getConnectorName(),
                taskConfig.getTaskId(),
                taskConfig,
                ThreadPools.getIoExecutor(taskConfig.getConnectorName()),
                taskMetrics),
        () ->
            new PartitionChannelManager(
                conn.getTelemetryClient(),
                taskConfig,
                recordErrorReporter,
                sinkTaskContext,
                metricsJmxReporter,
                taskMetrics,
                conn),
        taskMetrics);
  }

  SnowflakeSinkServiceV2(
      SnowflakeConnectionService conn,
      SinkTaskConfig taskConfig,
      SinkTaskContext sinkTaskContext,
      Optional<MetricsJmxReporter> metricsJmxReporter,
      Supplier<BatchOffsetFetcher> batchOffsetFetcherFactory,
      Supplier<PartitionChannelManager> channelManagerFactory,
      TaskMetrics taskMetrics) {
    if (conn == null || conn.isClosed()) {
      throw SnowflakeErrors.ERROR_5010.getException();
    }
    this.conn = conn;
    this.taskConfig = taskConfig;
    this.sinkTaskContext = sinkTaskContext;
    this.metricsJmxReporter = metricsJmxReporter;

    this.connectorName = taskConfig.getConnectorName();

    ThreadPools.registerTask(this.connectorName, taskConfig);

    this.taskMetrics = taskMetrics;
    this.batchOffsetFetcher = batchOffsetFetcherFactory.get();
    this.channelManager = channelManagerFactory.get();

    // Log validation configuration for operator visibility
    logValidationConfiguration();

    LOGGER.info(
        "SnowflakeSinkServiceV2 initialized for connector: {}, task: {}, tolerateErrors: {},"
            + " enableSanitization: {}",
        this.connectorName,
        taskConfig.getTaskId(),
        taskConfig.isTolerateErrors(),
        taskConfig.isEnableSanitization());
  }

  /**
   * Perform pre-flight safety checks on validation configuration. Verifies that error handling is
   * properly configured to prevent silent data loss or task crashes.
   *
   * <p>Safety checks: - If validation disabled: Warn that SSv2 Error Table is required to prevent
   * task crashes - If validation enabled: Verify DLQ or tolerance=none for safe error handling
   *
   * @throws IllegalStateException if configuration is unsafe and would cause data loss
   */
  private void logValidationConfiguration() {
    String errorsTolerance =
        taskConfig.isTolerateErrors()
            ? ConnectorConfigTools.ErrorTolerance.ALL.toString()
            : ConnectorConfigTools.ErrorTolerance.NONE.toString();
    String dlqTopic = taskConfig.getDlqTopicName();

    boolean dlqConfigured = dlqTopic != null && !dlqTopic.trim().isEmpty();
    boolean tolerateAll = "all".equalsIgnoreCase(errorsTolerance);

    // Check for legacy KC v3 config and warn if present (schematization enabled via task config)
    if (taskConfig.isEnableSchematization()) {
      LOGGER.warn(
          "Config 'snowflake.enable.schematization' is not supported in KC v4. "
              + "Schema evolution is now handled server-side via table property "
              + "'ENABLE_SCHEMA_EVOLUTION'. For pre-created tables, run: "
              + "ALTER TABLE ... SET ENABLE_SCHEMA_EVOLUTION = TRUE");
    } else {
      LOGGER.info(
          "Schematization is disabled — the connector wraps payloads into"
              + " RECORD_CONTENT/RECORD_METADATA.");
    }

    if (taskConfig.getValidation() != SnowflakeValidation.CLIENT_SIDE) {
      // Check each target table for ERROR_LOGGING.
      // Note: makes up to 3 network calls per table (tableExist + isIcebergTable +
      // hasErrorLoggingEnabled). Acceptable at startup; only runs once per task constructor.
      Set<String> uniqueTables = new HashSet<>(taskConfig.getTopicToTableMap().values());
      for (String tableName : uniqueTables) {
        if (!conn.tableExist(tableName)) {
          // Table doesn't exist yet — will be auto-created with ERROR_LOGGING = TRUE
          continue;
        }
        if (conn.isIcebergTable(tableName)) {
          LOGGER.warn(
              "Table '{}' is an Iceberg table. Iceberg tables do not support ERROR_LOGGING."
                  + " In v4 high-throughput mode, invalid records targeting this table will be"
                  + " silently dropped. Error table functionality is not available for Iceberg"
                  + " tables.",
              tableName);
          continue;
        }
        if (!conn.hasErrorLoggingEnabled(tableName)) {
          LOGGER.warn(
              "Table '{}' does not have ERROR_LOGGING enabled. In v4 high-throughput mode,"
                  + " invalid records will be silently dropped. Run: ALTER TABLE \"{}\" SET"
                  + " ERROR_LOGGING = TRUE",
              tableName,
              tableName);
        } else {
          LOGGER.info("Table '{}' has ERROR_LOGGING enabled — error table is active.", tableName);
        }
      }
      return;
    }

    // VALIDATION ENABLED
    // Verify safe error handling configuration
    if (tolerateAll) {
      if (dlqConfigured) {
        // SAFE: Validation errors route to DLQ
        LOGGER.info(
            "Client-side validation enabled with errors.tolerance=all. "
                + "Validation failures will route to DLQ topic: {}",
            dlqTopic);
      } else {
        // UNSAFE: Validation errors are silently dropped
        LOGGER.error(
            "UNSAFE CONFIGURATION: Client-side validation enabled with errors.tolerance=all but NO"
                + " DLQ configured. "
                + "Invalid records will be SILENTLY DROPPED causing data loss. "
                + "Configure '{}' to preserve failed records, or set errors.tolerance=none to abort"
                + " on errors.",
            KafkaConnectorConfigParams.ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_CONFIG);
        // Note: Not throwing exception to allow connector to start, but logging ERROR
        // Operators can decide if they want to fail fast by checking logs
      }
    } else {
      // SAFE: Task aborts on validation failure (errors.tolerance=none)
      LOGGER.info(
          "Client-side validation enabled with errors.tolerance=none. "
              + "Validation failures will abort the task (safe - prevents data loss){}.",
          dlqConfigured ? " DLQ configured but only used when errors.tolerance=all" : "");
    }
  }

  /**
   * Creates a table if it doesnt exist in Snowflake.
   *
   * <p>Initializes the Channel and partitionsToChannel map with new instance of {@link
   * TopicPartitionChannel}
   *
   * @param topicPartition TopicPartition passed from Kafka
   */
  @Override
  public void startPartition(TopicPartition topicPartition) {
    startPartitions(Set.of(topicPartition));
  }

  /**
   * Ensures tables and pipes exist in Snowflake, then delegates channel creation to the {@link
   * PartitionChannelManager}.
   *
   * @param partitions collection of topic partition
   */
  @Override
  public void startPartitions(Collection<TopicPartition> partitions) {
    final Map<String, String> tableToPipeMapping = new HashMap<>();

    final Collection<String> uniqueTopics =
        partitions.stream().map(TopicPartition::topic).collect(Collectors.toSet());

    for (String topic : uniqueTopics) {
      final String tableName =
          getTableName(topic, taskConfig.getTopicToTableMap(), taskConfig.isEnableSanitization());
      createTableIfNotExists(tableName);

      // Client-side validation only supports default pipes.
      // When validation is enabled, reject non-default pipes (pipes whose name equals the table
      // name) because validation assumptions may not hold for user-created pipes.
      final String targetPipeName;
      if (taskConfig.getValidation() == SnowflakeValidation.CLIENT_SIDE) {
        if (this.conn.pipeExist(tableName)) {
          throw SnowflakeErrors.ERROR_0032.getException("table: " + tableName);
        }
        targetPipeName = buildDefaultPipeName(tableName);
      } else {
        // When validation is disabled (high-performance mode), allow non-default pipes.
        final boolean pipeExists = this.conn.pipeExist(tableName);
        targetPipeName = pipeExists ? tableName : buildDefaultPipeName(tableName);
      }

      tableToPipeMapping.put(tableName, targetPipeName);
      LOGGER.info("Table: {}, using pipe: {}", tableName, targetPipeName);
    }

    channelManager.startPartitions(partitions, tableToPipeMapping);
  }

  private void createTableIfNotExists(final String tableName) {
    if (this.conn.tableExist(tableName)) {
      LOGGER.info("Using existing table {}.", tableName);
    } else {
      LOGGER.info("Creating new table {}.", tableName);
      this.conn.createTableWithOnlyMetadataColumn(tableName);
    }
  }

  private Set<TopicPartition> currentlyInitializing(Collection<TopicPartition> partitions) {
    return partitions.stream()
        .filter(
            tp -> {
              return channelManager
                  .getChannel(tp)
                  .map(TopicPartitionChannel::isInitializing)
                  .orElse(false);
            })
        .collect(Collectors.toSet());
  }

  /**
   * @param records records coming from Kafka. Please note, they are not just from single topic and
   *     partition. It depends on the kafka connect worker node which can consume from multiple
   *     Topic and multiple Partitions
   */
  @Override
  public void insert(final Collection<SinkRecord> records) {
    // Skip partitions for which the partition-channel bridge is currently being initialized.
    Set<TopicPartition> partitions =
        records.stream()
            .map(record -> new TopicPartition(record.topic(), record.kafkaPartition()))
            .collect(Collectors.toSet());

    Set<TopicPartition> initializingPartitions = currentlyInitializing(partitions);
    if (!initializingPartitions.isEmpty()) {
      LOGGER.debug(
          "Skipping put for {}/{} partitions that are currently being initialized: {}",
          initializingPartitions.size(),
          partitions.size(),
          initializingPartitions);
    }

    Map<TopicPartition, Long> offsetsToRewindTo = new HashMap<>();

    // Drain offset resets submitted by channel init / recovery on the IO thread.
    // These partitions are skipped in this batch and rewound at the end.
    // They will be processed normally in the next batch.
    // This is so that sinkTaskContext.offset() is only ever called from this (task) thread.
    Map<TopicPartition, Long> pendingResets = channelManager.drainPendingOffsetResets();
    if (!pendingResets.isEmpty()) {
      LOGGER.info("Draining {} pending offset resets: {}", pendingResets.size(), pendingResets);
      offsetsToRewindTo.putAll(pendingResets);
    }

    // If still in cooldown from a recent backpressure event, treat all partitions as
    // backpressured so we skip the entire batch and give the SDK time to drain.
    boolean skipAllPartitions = false;
    if (Instant.now().isBefore(backpressureUntil)) {
      LOGGER.debug(
          "Backpressure cooldown active until {}. Skipping entire batch.", backpressureUntil);
      skipAllPartitions = true;
    }

    boolean newBackpressure = false;
    for (SinkRecord record : records) {
      // check if it needs to handle null value records
      if (shouldSkipNullValue(record)) {
        continue;
      }

      TopicPartition tp = new TopicPartition(record.topic(), record.kafkaPartition());

      if (offsetsToRewindTo.containsKey(tp)) {
        // We've already skipped a record in this partition, so should also skip the remaining
        // records in this partition.
        continue;
      }
      if (skipAllPartitions || initializingPartitions.contains(tp)) {
        // Make sure we store the first record in each partition that we skipped so we can correctly
        // rewind the offset.
        offsetsToRewindTo.putIfAbsent(tp, record.kafkaOffset());
        continue;
      }

      try {
        if (!insert(record)) {
          offsetsToRewindTo.putIfAbsent(tp, record.kafkaOffset());
        }
      } catch (BackpressureException e) {
        LOGGER.warn(
            "Backpressure on partition {}. Skipping remaining records for this partition."
                + " Exception: {}",
            tp,
            e.getMessage());
        taskMetrics.incBackpressureRewindCount();
        offsetsToRewindTo.putIfAbsent(tp, record.kafkaOffset());
        skipAllPartitions = true;
        newBackpressure = true;
      }
    }

    if (newBackpressure) {
      backpressureUntil = Instant.now().plus(BACKPRESSURE_COOLDOWN);
      LOGGER.info("Backpressure cooldown set until {}", backpressureUntil);
    }

    if (!offsetsToRewindTo.isEmpty()) {
      LOGGER.info("Rewinding offsets for skipped partitions: {}", offsetsToRewindTo);
      sinkTaskContext.offset(offsetsToRewindTo);
    }
  }

  /**
   * Inserts individual records into buffer. It fetches the TopicPartitionChannel from the map and
   * then each partition(Streaming channel) calls its respective appendRows API
   */
  @Override
  public boolean insert(SinkRecord record) {
    LOGGER.trace("Inserting record: {}", record);

    TopicPartition topicPartition = new TopicPartition(record.topic(), record.kafkaPartition());

    // Initialize a new topic partition if it's not in the cache or if the channel is closed.
    if (channelManager
        .getChannel(topicPartition)
        .map(TopicPartitionChannel::isChannelClosed)
        .orElse(true)) {
      LOGGER.warn("Streaming channel doesn't exist or is closed for {}", topicPartition);
      startPartition(topicPartition);
      return false;
    }

    TopicPartitionChannel channel =
        channelManager
            .getChannel(topicPartition)
            .orElseThrow(
                () ->
                    new IllegalStateException(
                        "Channel for " + topicPartition + " not found after startPartition"));

    return channel.insertRecord(record);
  }

  private boolean shouldSkipNullValue(SinkRecord record) {
    if (taskConfig.getBehaviorOnNullValues() == ConnectorConfigTools.BehaviorOnNullValues.DEFAULT) {
      return false;
    }
    if (record.value() == null) {
      LOGGER.debug(
          "Null valued record from topic '{}', partition {} and offset {} was skipped.",
          record.topic(),
          record.kafkaPartition(),
          record.kafkaOffset());
      return true;
    }
    return false;
  }

  @Override
  public long getOffset(TopicPartition topicPartition) {
    return getCommittedOffsets(Collections.singleton(topicPartition))
        .getOrDefault(topicPartition, NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE);
  }

  @Override
  public Map<TopicPartition, Long> getCommittedOffsets(
      final Collection<TopicPartition> partitions) {

    // Skip partitions for which the partition-channel bridge is currently being initialized.
    Set<TopicPartition> initializingPartitions = currentlyInitializing(partitions);
    if (!initializingPartitions.isEmpty()) {
      LOGGER.info(
          "Skipping preCommit for {}/{} partitions that are currently being initialized: {}",
          initializingPartitions.size(),
          partitions.size(),
          initializingPartitions);
    }

    Set<TopicPartition> partitionsToFetchOffsetsFor =
        partitions.stream()
            .filter(tp -> !initializingPartitions.contains(tp))
            .collect(Collectors.toSet());

    return batchOffsetFetcher.getCommittedOffsets(
        partitionsToFetchOffsetsFor, channelManager::getChannel);
  }

  @Override
  public int getPartitionCount() {
    return channelManager.getPartitionChannels().size();
  }

  @Override
  public void closeAll() {
    channelManager.closeAll();
  }

  /**
   * This function is called during rebalance.
   *
   * <p>All the channels are closed. The client is still active. Upon rebalance, (inside {@link
   * com.snowflake.kafka.connector.SnowflakeSinkTask#open(Collection)} we will reopen the channel.
   *
   * <p>We will wipe the cache partitionsToChannel so that in {@link
   * com.snowflake.kafka.connector.SnowflakeSinkTask#open(Collection)} we reinstantiate and fetch
   * offsetToken
   *
   * @param partitions a list of topic partition
   */
  @Override
  public void close(Collection<TopicPartition> partitions) {
    channelManager.close(partitions);
  }

  @Override
  public void stop() {
    LOGGER.info(
        "Stopping SnowflakeSinkServiceV2 for connector: {}, task: {}",
        this.connectorName,
        taskConfig.getTaskId());

    channelManager.waitForAllChannelsToCommitData();

    // Release all streaming clients used by this service.
    // Clients will only be closed if no other tasks are using them.
    StreamingClientPools.closeTaskClients(connectorName, taskConfig.getTaskId());

    // Release this task's claim on the shared thread pool.
    // The pool is shut down when the last task for this connector unregisters.
    ThreadPools.closeForTask(connectorName);
  }

  /* Undefined */
  @Override
  public boolean isClosed() {
    return false;
  }

  @Override
  public Map<String, TopicPartitionChannel> getPartitionChannels() {
    return channelManager.getPartitionChannels();
  }

  @Override
  public Optional<MetricRegistry> getMetricRegistry(String partitionChannelKey) {
    if (channelManager.getChannel(partitionChannelKey).isEmpty()) {
      return Optional.empty();
    }
    return metricsJmxReporter.map(MetricsJmxReporter::getMetricRegistry);
  }

  /** Blocks until all partition channels have finished initialization. */
  @Override
  public void awaitInitialization() {
    channelManager.awaitAllPartitions();
  }

  @VisibleForTesting
  PartitionChannelManager getChannelManager() {
    return channelManager;
  }
}
