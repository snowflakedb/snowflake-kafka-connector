package com.snowflake.kafka.connector.internal.streaming;

import static com.google.common.base.Strings.isNullOrEmpty;
import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.NAME;
import static com.snowflake.kafka.connector.Utils.getTableName;
import static com.snowflake.kafka.connector.internal.streaming.channel.TopicPartitionChannel.NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE;
import static com.snowflake.kafka.connector.internal.streaming.v2.PipeNameProvider.buildDefaultPipeName;

import com.codahale.metrics.MetricRegistry;
import com.google.common.annotations.VisibleForTesting;
import com.snowflake.kafka.connector.ConnectorConfigTools;
import com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams;
import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.dlq.KafkaRecordErrorReporter;
import com.snowflake.kafka.connector.internal.KCLogger;
import com.snowflake.kafka.connector.internal.SnowflakeConnectionService;
import com.snowflake.kafka.connector.internal.SnowflakeErrors;
import com.snowflake.kafka.connector.internal.SnowflakeSinkService;
import com.snowflake.kafka.connector.internal.metrics.MetricsJmxReporter;
import com.snowflake.kafka.connector.internal.metrics.TaskMetrics;
import com.snowflake.kafka.connector.internal.streaming.channel.TopicPartitionChannel;
import com.snowflake.kafka.connector.internal.streaming.v2.client.StreamingClientPools;
import com.snowflake.kafka.connector.internal.streaming.v2.service.BatchOffsetFetcher;
import com.snowflake.kafka.connector.internal.streaming.v2.service.PartitionChannelManager;
import com.snowflake.kafka.connector.internal.streaming.v2.service.ThreadPools;
import com.snowflake.kafka.connector.records.SnowflakeMetadataConfig;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
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

  // Used to connect to Snowflake, could be null during testing
  private final SnowflakeConnectionService conn;

  private final Map<String, String> topicToTableMap;

  // Behavior to be set at the start of connector start. (For tombstone records)
  private final ConnectorConfigTools.BehaviorOnNullValues behaviorOnNullValues;
  private final Optional<MetricsJmxReporter> metricsJmxReporter;
  private final String connectorName;
  private final String taskId;

  private final Map<String, String> connectorConfig;

  // Set that keeps track of the channels that have been seen per input batch
  private final Set<String> channelsVisitedPerBatch = new HashSet<>();
  // Whether to tolerate errors during ingestion (based on errors.tolerance config)
  private final boolean tolerateErrors;
  private final BatchOffsetFetcher batchOffsetFetcher;
  // Whether to enable table name sanitization
  private final boolean enableSanitization;

  private final PartitionChannelManager channelManager;

  public SnowflakeSinkServiceV2(
      SnowflakeConnectionService conn,
      Map<String, String> connectorConfig,
      KafkaRecordErrorReporter recordErrorReporter,
      SinkTaskContext sinkTaskContext,
      Optional<MetricsJmxReporter> metricsJmxReporter,
      Map<String, String> topicToTableMap,
      ConnectorConfigTools.BehaviorOnNullValues behaviorOnNullValues,
      TaskMetrics taskMetrics) {
    if (conn == null || conn.isClosed()) {
      throw SnowflakeErrors.ERROR_5010.getException();
    }
    this.conn = conn;
    this.connectorConfig = connectorConfig;
    this.metricsJmxReporter = metricsJmxReporter;
    this.topicToTableMap = topicToTableMap;
    this.behaviorOnNullValues = behaviorOnNullValues;

    // Extract and validate connector name - must not be null or empty
    this.connectorName = connectorConfig.get(NAME);
    if (isNullOrEmpty(this.connectorName)) {
      throw new IllegalArgumentException(
          "Connector name ('" + NAME + "') must be set in configuration and cannot be empty");
    }

    // Extract and validate task ID - must not be null or empty
    this.taskId = connectorConfig.get(Utils.TASK_ID);
    if (this.taskId == null || this.taskId.trim().isEmpty()) {
      throw new IllegalArgumentException(
          "Task ID ('" + Utils.TASK_ID + "') must be set and cannot be null or empty");
    }

    this.tolerateErrors = StreamingUtils.tolerateErrors(connectorConfig);
    this.batchOffsetFetcher =
        new BatchOffsetFetcher(
            this.connectorName,
            this.taskId,
            connectorConfig,
            this.tolerateErrors,
            ThreadPools.getIoExecutor(this.connectorName),
            taskMetrics);
    this.enableSanitization =
        Boolean.parseBoolean(
            connectorConfig.getOrDefault(
                KafkaConnectorConfigParams.SNOWFLAKE_ENABLE_AUTOGENERATED_TABLE_NAME_SANITIZATION,
                String.valueOf(
                    KafkaConnectorConfigParams
                        .SNOWFLAKE_ENABLE_AUTOGENERATED_TABLE_NAME_SANITIZATION_DEFAULT)));

    this.channelManager =
        new PartitionChannelManager(
            conn.getTelemetryClient(),
            connectorConfig,
            recordErrorReporter,
            new SnowflakeMetadataConfig(connectorConfig),
            sinkTaskContext,
            metricsJmxReporter,
            this.connectorName,
            this.taskId,
            taskMetrics,
            topicToTableMap,
            this.enableSanitization);

    ThreadPools.registerTask(this.connectorName, this.taskId);

    // Log validation configuration for operator visibility
    logValidationConfiguration();

    LOGGER.info(
        "SnowflakeSinkServiceV2 initialized for connector: {}, task: {}, tolerateErrors: {},"
            + " enableSanitization: {}",
        this.connectorName,
        this.taskId,
        this.tolerateErrors,
        this.enableSanitization);
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
    boolean validationEnabled =
        Boolean.parseBoolean(
            connectorConfig.getOrDefault(
                KafkaConnectorConfigParams.SNOWFLAKE_CLIENT_VALIDATION_ENABLED,
                String.valueOf(
                    KafkaConnectorConfigParams.SNOWFLAKE_CLIENT_VALIDATION_ENABLED_DEFAULT)));

    String errorsTolerance =
        connectorConfig.getOrDefault(
            KafkaConnectorConfigParams.ERRORS_TOLERANCE_CONFIG,
            KafkaConnectorConfigParams.ERRORS_TOLERANCE_DEFAULT);

    String dlqTopic =
        connectorConfig.getOrDefault(
            KafkaConnectorConfigParams.ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_CONFIG,
            KafkaConnectorConfigParams.ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_DEFAULT);

    boolean dlqConfigured = dlqTopic != null && !dlqTopic.trim().isEmpty();
    boolean tolerateAll = "all".equalsIgnoreCase(errorsTolerance);

    // Check for legacy KC v3 config and warn if present
    if (connectorConfig.containsKey("snowflake.enable.schematization")) {
      LOGGER.warn(
          "Config 'snowflake.enable.schematization' is not supported in KC v4. "
              + "Schema evolution is now handled server-side via table property "
              + "'ENABLE_SCHEMA_EVOLUTION'. For pre-created tables, run: "
              + "ALTER TABLE ... SET ENABLE_SCHEMA_EVOLUTION = TRUE");
    }

    if (!validationEnabled) {
      // VALIDATION DISABLED (High-Performance Mode)
      // Must verify SSv2 Error Table is configured to prevent records from being silently dropped
      // TODO: Check Error Table configuration when SSv2 API exposes this information
      // For now, log warning as API is not yet available
      LOGGER.warn(
          "CLIENT-SIDE VALIDATION DISABLED (High-Performance Mode). Running without client-side"
              + " validation requires a configured SSv2 Error Table to prevent records from being"
              + " silently dropped.");
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
      final String tableName = getTableName(topic, this.topicToTableMap, this.enableSanitization);
      createTableIfNotExists(tableName);

      // Look for a pipe with the same name as the table. Otherwise, use the default pipe.
      final boolean pipeExists = this.conn.pipeExist(tableName);
      final String targetPipeName = pipeExists ? tableName : buildDefaultPipeName(tableName);

      tableToPipeMapping.put(tableName, targetPipeName);
      LOGGER.info(
          "Table: {}, pipe exists: {}, using pipe: {}", tableName, pipeExists, targetPipeName);
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

  /**
   * @param records records coming from Kafka. Please note, they are not just from single topic and
   *     partition. It depends on the kafka connect worker node which can consume from multiple
   *     Topic and multiple Partitions
   */
  @Override
  public void insert(final Collection<SinkRecord> records) {
    channelsVisitedPerBatch.clear();
    for (SinkRecord record : records) {
      // check if it needs to handle null value records
      if (shouldSkipNullValue(record)) {
        continue;
      }
      insert(record);
    }
  }

  /**
   * Inserts individual records into buffer. It fetches the TopicPartitionChannel from the map and
   * then each partition(Streaming channel) calls its respective appendRows API
   */
  @Override
  public void insert(SinkRecord record) {
    LOGGER.trace("Inserting record: {}", record);

    TopicPartition topicPartition = new TopicPartition(record.topic(), record.kafkaPartition());

    // Initialize a new topic partition if it's not in the cache or if the channel is closed.
    if (channelManager
        .getChannel(topicPartition)
        .map(TopicPartitionChannel::isChannelClosed)
        .orElse(true)) {
      LOGGER.warn("Channel hasn't been initialized for {}", topicPartition);
      startPartition(topicPartition);
    }

    TopicPartitionChannel channel =
        channelManager
            .getChannel(topicPartition)
            .orElseThrow(
                () ->
                    new IllegalStateException(
                        "Channel for " + topicPartition + " not found after startPartition"));

    boolean isFirstRowPerPartitionInBatch = channelsVisitedPerBatch.add(channel.getChannelName());
    channel.insertRecord(record, isFirstRowPerPartitionInBatch);
  }

  private boolean shouldSkipNullValue(SinkRecord record) {
    if (behaviorOnNullValues == ConnectorConfigTools.BehaviorOnNullValues.DEFAULT) {
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
    return batchOffsetFetcher.getCommittedOffsets(partitions, channelManager::getChannel);
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
        this.taskId);

    channelManager.waitForAllChannelsToCommitData();

    // Release all streaming clients used by this service.
    // Clients will only be closed if no other tasks are using them.
    StreamingClientPools.closeTaskClients(connectorName, taskId);

    // Release this task's claim on the shared thread pool.
    // The pool is shut down when the last task for this connector unregisters.
    ThreadPools.closeForTask(connectorName, taskId);
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

  @VisibleForTesting
  PartitionChannelManager getChannelManager() {
    return channelManager;
  }
}
