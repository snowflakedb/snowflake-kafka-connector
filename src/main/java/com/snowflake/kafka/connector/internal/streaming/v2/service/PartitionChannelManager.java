package com.snowflake.kafka.connector.internal.streaming.v2.service;

import com.google.common.annotations.VisibleForTesting;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.config.SinkTaskConfig;
import com.snowflake.kafka.connector.config.SnowflakeValidation;
import com.snowflake.kafka.connector.dlq.KafkaRecordErrorReporter;
import com.snowflake.kafka.connector.internal.KCLogger;
import com.snowflake.kafka.connector.internal.SnowflakeConnectionService;
import com.snowflake.kafka.connector.internal.metrics.MetricsJmxReporter;
import com.snowflake.kafka.connector.internal.metrics.TaskMetrics;
import com.snowflake.kafka.connector.internal.streaming.StreamingClientProperties;
import com.snowflake.kafka.connector.internal.streaming.StreamingErrorHandler;
import com.snowflake.kafka.connector.internal.streaming.channel.TopicPartitionChannel;
import com.snowflake.kafka.connector.internal.streaming.telemetry.SnowflakeTelemetryChannelStatus;
import com.snowflake.kafka.connector.internal.streaming.v2.SnowpipeStreamingPartitionChannel;
import com.snowflake.kafka.connector.internal.streaming.v2.channel.PartitionOffsetTracker;
import com.snowflake.kafka.connector.internal.streaming.v2.client.StreamingClientPools;
import com.snowflake.kafka.connector.internal.streaming.v2.migration.Ssv1MigrationMode;
import com.snowflake.kafka.connector.internal.telemetry.SnowflakeTelemetryService;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkTaskContext;

/**
 * Manages the lifecycle of {@link TopicPartitionChannel} instances for a single Kafka Connect task.
 * Handles channel creation, opening, closing, and lookup.
 */
public class PartitionChannelManager {

  private static final KCLogger LOGGER = new KCLogger(PartitionChannelManager.class.getName());

  /**
   * Creates a {@link TopicPartitionChannel} for a single partition during {@link #startPartitions}.
   * Production code uses {@link #buildChannel}; tests inject a lambda.
   */
  @FunctionalInterface
  interface PartitionChannelBuilder {
    TopicPartitionChannel build(
        TopicPartition topicPartition, String tableName, String channelName, String pipeName);
  }

  private final SnowflakeTelemetryService telemetryService;
  private final KafkaRecordErrorReporter kafkaRecordErrorReporter;
  private final Optional<MetricsJmxReporter> metricsJmxReporter;
  private final TaskMetrics taskMetrics;

  private final SinkTaskContext sinkTaskContext;

  private final SinkTaskConfig taskConfig;
  private final SnowflakeConnectionService conn;

  private final PartitionChannelBuilder partitionChannelBuilder;
  private final Map<String, TopicPartitionChannel> partitionChannels;
  private final Map<String, Boolean> shouldEvolveSchemaCache = new ConcurrentHashMap<>();

  public PartitionChannelManager(
      SnowflakeTelemetryService telemetryService,
      SinkTaskConfig taskConfig,
      KafkaRecordErrorReporter kafkaRecordErrorReporter,
      SinkTaskContext sinkTaskContext,
      Optional<MetricsJmxReporter> metricsJmxReporter,
      TaskMetrics taskMetrics,
      SnowflakeConnectionService conn) {
    this.telemetryService = telemetryService;
    this.taskConfig = taskConfig;
    this.kafkaRecordErrorReporter = kafkaRecordErrorReporter;
    this.sinkTaskContext = sinkTaskContext;
    this.metricsJmxReporter = metricsJmxReporter;
    this.taskMetrics = taskMetrics;
    this.conn = conn;
    this.partitionChannelBuilder = this::buildChannel;
    this.partitionChannels = new ConcurrentHashMap<>();
  }

  @VisibleForTesting
  PartitionChannelManager(
      SinkTaskConfig taskConfig, PartitionChannelBuilder partitionChannelBuilder) {
    this.taskConfig = taskConfig;
    this.partitionChannelBuilder = partitionChannelBuilder;
    this.partitionChannels = new ConcurrentHashMap<>();
    this.telemetryService = null;
    this.kafkaRecordErrorReporter = null;
    this.sinkTaskContext = null;
    this.metricsJmxReporter = Optional.empty();
    this.taskMetrics = null;
    this.conn = null;
  }

  /** Gets a unique identifier consisting of connector name, topic name and partition number. */
  @VisibleForTesting
  public static String makeChannelName(
      final String connectorName, final String topic, final int partition) {
    final String separator = "_";
    return connectorName + separator + topic + separator + partition;
  }

  private String getChannelName(TopicPartition topicPartition) {
    return makeChannelName(
        taskConfig.getConnectorName(), topicPartition.topic(), topicPartition.partition());
  }

  private String getTableName(TopicPartition topicPartition) {
    return Utils.getTableName(
        topicPartition.topic(), taskConfig.getTopicToTableMap(), taskConfig.isEnableSanitization());
  }

  /**
   * Creates and registers channels for the given partitions.
   *
   * @param partitions collection of topic partitions to open channels for
   * @param tableToPipeMapping pre-resolved mapping of table name to pipe name; the caller is
   *     responsible for ensuring tables exist and resolving the correct pipe for each table
   */
  public void startPartitions(
      Collection<TopicPartition> partitions, Map<String, String> tableToPipeMapping) {
    LOGGER.info(
        "Starting {} partitions for connector: {}, task: {}",
        partitions.size(),
        taskConfig.getConnectorName(),
        taskConfig.getTaskId());

    warmUpStreamingClients(tableToPipeMapping);

    for (TopicPartition topicPartition : partitions) {
      final String tableName = getTableName(topicPartition);
      final String pipeName = tableToPipeMapping.get(tableName);
      final String channelName = getChannelName(topicPartition);

      LOGGER.info(
          "Creating streaming channel {} for {}, table: {}, pipe: {}",
          channelName,
          topicPartition,
          tableName,
          pipeName);

      final TopicPartitionChannel partitionChannel =
          partitionChannelBuilder.build(topicPartition, tableName, channelName, pipeName);

      partitionChannels.put(channelName, partitionChannel);
      LOGGER.info("Successfully created streaming channel: {}", channelName);
    }
  }

  private TopicPartitionChannel buildChannel(
      TopicPartition topicPartition, String tableName, String channelName, String pipeName) {

    final StreamingErrorHandler streamingErrorHandler =
        new StreamingErrorHandler(taskConfig, kafkaRecordErrorReporter, telemetryService);
    final boolean enableSchematization = taskConfig.isEnableSchematization();
    final StreamingClientProperties streamingClientProperties =
        StreamingClientProperties.from(taskConfig);
    final SnowflakeStreamingIngestClient streamingClient =
        StreamingClientPools.getClient(
            taskConfig.getConnectorName(),
            taskConfig.getTaskId(),
            pipeName,
            taskConfig,
            streamingClientProperties,
            taskMetrics);
    final boolean clientValidationEnabled =
        taskConfig.getValidation() == SnowflakeValidation.CLIENT_SIDE;

    final PartitionOffsetTracker offsetTracker =
        new PartitionOffsetTracker(topicPartition, this.sinkTaskContext, channelName);

    final SnowflakeTelemetryChannelStatus telemetryChannelStatus =
        new SnowflakeTelemetryChannelStatus(
            tableName,
            taskConfig.getConnectorName(),
            channelName,
            System.currentTimeMillis(),
            this.metricsJmxReporter,
            offsetTracker.persistedOffsetRef(),
            offsetTracker.processedOffsetRef(),
            offsetTracker.consumerGroupOffsetRef());

    final ExecutorService openChannelIoExecutor =
        ThreadPools.getOpenChannelIoExecutor(taskConfig.getConnectorName());

    final boolean shouldEvolveSchema =
        (taskConfig.getValidation() == SnowflakeValidation.CLIENT_SIDE)
            && shouldEvolveSchemaCache.computeIfAbsent(
                tableName, t -> conn.shouldEvolveSchema(t, taskConfig.getSnowflakeRole()));

    // KC v3 defaulted to V1 channel naming: {topic}_{partition}.
    // Customers who set snowflake.streaming.channel.name.include.connector.name=true
    // in KC v3 used V2 naming: {connectorName}_{topic}_{partition} (same as KC v4).
    final Ssv1MigrationMode ssv1MigrationMode = taskConfig.getSsv1MigrationMode();
    final Optional<String> ssv1ChannelName;
    if (ssv1MigrationMode != Ssv1MigrationMode.SKIP) {
      String topic = topicPartition.topic();
      int partition = topicPartition.partition();
      ssv1ChannelName =
          Optional.of(
              taskConfig.isSsv1MigrationIncludeConnectorName()
                  ? connectorName + "_" + topic + "_" + partition
                  : topic + "_" + partition);
    } else {
      ssv1ChannelName = Optional.empty();
    }

    return new SnowpipeStreamingPartitionChannel(
        tableName,
        channelName,
        pipeName,
        streamingClient,
        openChannelIoExecutor,
        this.telemetryService,
        telemetryChannelStatus,
        offsetTracker,
        taskConfig.getMetadataConfig(),
        enableSchematization,
        taskConfig.isEnableColumnIdentifierNormalization(),
        streamingErrorHandler,
        this.taskMetrics,
        clientValidationEnabled,
        shouldEvolveSchema,
        this.conn,
        ssv1MigrationMode,
        ssv1ChannelName);
  }

  /**
   * Pre-warms the {@link StreamingClientPools} cache by creating clients for all distinct pipes in
   * parallel. Subsequent per-partition calls to {@link StreamingClientPools#getClient} in {@link
   * #buildChannel} will return the cached clients immediately.
   *
   * <p>Skipped when using the test constructor (conn is null).
   */
  private void warmUpStreamingClients(Map<String, String> tableToPipeMapping) {
    if (conn == null) {
      return;
    }

    final StreamingClientProperties streamingClientProperties =
        StreamingClientProperties.from(taskConfig);

    CompletableFuture<?>[] clientFutures =
        tableToPipeMapping.values().stream()
            .distinct()
            .map(
                pipeName ->
                    StreamingClientPools.getClientAsync(
                        taskConfig.getConnectorName(),
                        taskConfig.getTaskId(),
                        pipeName,
                        taskConfig,
                        streamingClientProperties,
                        taskMetrics))
            .toArray(CompletableFuture[]::new);
    try {
      CompletableFuture.allOf(clientFutures).join();
    } catch (CompletionException e) {
      if (e.getCause() instanceof RuntimeException) {
        throw (RuntimeException) e.getCause();
      }
      throw e;
    }
  }

  public void waitForAllChannelsToCommitData() {
    int channelCount = partitionChannels.size();
    if (channelCount == 0) {
      return;
    }

    LOGGER.info("Starting parallel flush for {} channels", channelCount);

    CompletableFuture<?>[] futures =
        partitionChannels.values().stream()
            .map(TopicPartitionChannel::waitForLastProcessedRecordCommitted)
            .toArray(CompletableFuture[]::new);

    CompletableFuture.allOf(futures).join();

    LOGGER.info("Completed parallel flush for {} channels", channelCount);
  }

  public void closeAll() {
    LOGGER.info(
        "Closing all {} partition channels for connector: {}, task: {}",
        partitionChannels.size(),
        taskConfig.getConnectorName(),
        taskConfig.getTaskId());

    CompletableFuture<?>[] futures =
        partitionChannels.values().stream()
            .map(TopicPartitionChannel::closeChannelAsync)
            .toArray(CompletableFuture[]::new);
    CompletableFuture.allOf(futures).join();

    partitionChannels.clear();

    LOGGER.info(
        "Completed closing all partition channels for connector: {}, task: {}",
        taskConfig.getConnectorName(),
        taskConfig.getTaskId());
  }

  /**
   * This function is called during rebalance.
   *
   * <p>All the channels are closed. The client is still active. Upon rebalance, (inside {@link
   * com.snowflake.kafka.connector.SnowflakeSinkTask#open(Collection)} we will reopen the channel.
   *
   * <p>We will wipe the cache partitionChannels so that in {@link
   * com.snowflake.kafka.connector.SnowflakeSinkTask#open(Collection)} we reinstantiate and fetch
   * offsetToken
   *
   * @param partitions a list of topic partition
   */
  public void close(Collection<TopicPartition> partitions) {
    LOGGER.info(
        "Closing {} partitions for connector: {}, task: {}",
        partitions.size(),
        taskConfig.getConnectorName(),
        taskConfig.getTaskId());

    CompletableFuture<?>[] futures =
        partitions.stream()
            .map(this::getChannel)
            .filter(Optional::isPresent)
            .map(Optional::get)
            .map(
                channel ->
                    channel
                        .closeChannelAsync()
                        .thenAccept(__ -> partitionChannels.remove(channel.getChannelName())))
            .toArray(CompletableFuture[]::new);
    CompletableFuture.allOf(futures).join();

    LOGGER.info(
        "Closed {} partitions, remaining {} open partitions are: {}",
        partitions.size(),
        partitionChannels.size(),
        partitionChannels.keySet().toString());
  }

  /** Returns the channel for the given name, or empty if not found. */
  public Optional<TopicPartitionChannel> getChannel(String channelName) {
    return Optional.ofNullable(partitionChannels.get(channelName));
  }

  /** Returns the channel for the given TopicPartition, or empty if not found. */
  public Optional<TopicPartitionChannel> getChannel(TopicPartition topicPartition) {
    String channelName =
        makeChannelName(
            taskConfig.getConnectorName(), topicPartition.topic(), topicPartition.partition());
    return getChannel(channelName);
  }

  public Map<String, TopicPartitionChannel> getPartitionChannels() {
    return partitionChannels;
  }

  /** Blocks until all partition channels have finished initialization. */
  @VisibleForTesting
  public void awaitAllPartitions() {
    partitionChannels.values().forEach(TopicPartitionChannel::awaitInitialization);
  }
}
