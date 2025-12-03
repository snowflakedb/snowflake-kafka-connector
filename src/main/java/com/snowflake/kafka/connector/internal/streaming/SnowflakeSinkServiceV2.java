package com.snowflake.kafka.connector.internal.streaming;

import static com.google.common.base.Strings.isNullOrEmpty;
import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.NAME;
import static com.snowflake.kafka.connector.Utils.getTableName;
import static com.snowflake.kafka.connector.internal.streaming.channel.TopicPartitionChannel.NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE;
import static com.snowflake.kafka.connector.internal.streaming.v2.PipeNameProvider.buildDefaultPipeName;
import static com.snowflake.kafka.connector.internal.streaming.v2.PipeNameProvider.buildPipeName;

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
import com.snowflake.kafka.connector.internal.streaming.channel.TopicPartitionChannel;
import com.snowflake.kafka.connector.internal.streaming.v2.SnowpipeStreamingPartitionChannel;
import com.snowflake.kafka.connector.internal.streaming.v2.StreamingClientManager;
import com.snowflake.kafka.connector.records.SnowflakeMetadataConfig;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
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

  private final SnowflakeMetadataConfig metadataConfig;

  private final Map<String, String> topicToTableMap;

  // Behavior to be set at the start of connector start. (For tombstone records)
  private final ConnectorConfigTools.BehaviorOnNullValues behaviorOnNullValues;
  private final MetricsJmxReporter metricsJmxReporter;
  private final String connectorName;
  private final String taskId;
  /**
   * Fetching this from {@link org.apache.kafka.connect.sink.SinkTaskContext}'s {@link
   * org.apache.kafka.connect.sink.ErrantRecordReporter}
   */
  private final KafkaRecordErrorReporter kafkaRecordErrorReporter;
  /* SinkTaskContext has access to all methods/APIs available to talk to Kafka Connect runtime*/
  private final SinkTaskContext sinkTaskContext;
  // Config set in JSON
  private final Map<String, String> connectorConfig;

  private final Map<String, TopicPartitionChannel> partitionsToChannel;
  // Set that keeps track of the channels that have been seen per input batch
  private final Set<String> channelsVisitedPerBatch = new HashSet<>();
  // default is true unless the configuration provided is false;
  // If this is true, we will enable Mbean for required classes and emit JMX metrics for monitoring
  private boolean enableCustomJMXMonitoring = KafkaConnectorConfigParams.JMX_OPT_DEFAULT;

  public SnowflakeSinkServiceV2(
      SnowflakeConnectionService conn,
      Map<String, String> connectorConfig,
      KafkaRecordErrorReporter recordErrorReporter,
      SinkTaskContext sinkTaskContext,
      boolean enableCustomJMXMonitoring,
      Map<String, String> topicToTableMap,
      ConnectorConfigTools.BehaviorOnNullValues behaviorOnNullValues) {
    if (conn == null || conn.isClosed()) {
      throw SnowflakeErrors.ERROR_5010.getException();
    }
    this.conn = conn;
    this.connectorConfig = connectorConfig;
    this.kafkaRecordErrorReporter = recordErrorReporter;
    this.sinkTaskContext = sinkTaskContext;
    this.enableCustomJMXMonitoring = enableCustomJMXMonitoring;
    this.topicToTableMap = topicToTableMap;
    this.metadataConfig = new SnowflakeMetadataConfig(connectorConfig);
    this.behaviorOnNullValues = behaviorOnNullValues;
    this.partitionsToChannel = new HashMap<>();

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

    this.metricsJmxReporter = new MetricsJmxReporter(new MetricRegistry(), this.connectorName);

    LOGGER.info(
        "SnowflakeSinkServiceV2 initialized for connector: {}, task: {}",
        this.connectorName,
        this.taskId);
  }

  /** Gets a unique identifier consisting of connector name, topic name and partition number. */
  @VisibleForTesting
  public static String makeChannelName(
      final String connectorName, final String topic, final int partition) {
    final String separator = "_";
    return connectorName + separator + topic + separator + partition;
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
   * Initializes multiple Channels and partitionsToChannel maps with new instances of {@link
   * TopicPartitionChannel}
   *
   * @param partitions collection of topic partition
   */
  @Override
  public void startPartitions(Collection<TopicPartition> partitions) {
    LOGGER.info(
        "Starting {} partitions for connector: {}, task: {}",
        partitions.size(),
        this.connectorName,
        this.taskId);

    final Map<String, String> tableToPipeMapping = new HashMap<>();

    final Collection<String> topics =
        partitions.stream().map(TopicPartition::topic).collect(Collectors.toSet());

    for (String topic : topics) {
      final String tableName = getTableName(topic, this.topicToTableMap);

      final boolean tableExists = this.conn.tableExist(tableName);
      if (!tableExists) {
        throw SnowflakeErrors.ERROR_5029.getException(
            "Table name: " + tableName, this.conn.getTelemetryClient());
      }

      // not an error, by convention we're looking for the same name as table
      final boolean pipeExists = this.conn.pipeExist(tableName);
      // use pipe created by the user instead of the default pipe
      final String targetPipeName =
          pipeExists ? buildPipeName(tableName) : buildDefaultPipeName(tableName);
      tableToPipeMapping.put(tableName, targetPipeName);
      LOGGER.info(
          "Table: {}, pipe exists: {}, using pipe: {}", tableName, pipeExists, targetPipeName);
    }

    for (TopicPartition topicPartition : partitions) {
      final String tableName = getTableName(topicPartition.topic(), this.topicToTableMap);
      final String targetPipeName = tableToPipeMapping.get(tableName);
      createStreamingChannelForTopicPartition(tableName, targetPipeName, topicPartition);
    }
  }

  /**
   * Always opens a new channel and creates a new instance of TopicPartitionChannel.
   *
   * <p>This is essentially a blind write to partitionsToChannel. i.e. we do not check if it is
   * presented or not.
   */
  private void createStreamingChannelForTopicPartition(
      final String tableName, final String pipeName, final TopicPartition topicPartition) {
    final String channelName =
        makeChannelName(this.connectorName, topicPartition.topic(), topicPartition.partition());

    LOGGER.info(
        "Creating streaming channel for topic: {}, partition: {}, table: {}, pipe: {}, channel: {}",
        topicPartition.topic(),
        topicPartition.partition(),
        tableName,
        pipeName,
        channelName);

    StreamingErrorHandler streamingErrorHandler =
        new StreamingErrorHandler(
            connectorConfig, kafkaRecordErrorReporter, this.conn.getTelemetryClient());

    final SnowpipeStreamingPartitionChannel partitionChannel =
        new SnowpipeStreamingPartitionChannel(
            tableName,
            channelName,
            pipeName,
            topicPartition,
            this.conn,
            this.connectorConfig,
            this.kafkaRecordErrorReporter,
            this.metadataConfig,
            this.sinkTaskContext,
            this.enableCustomJMXMonitoring,
            this.metricsJmxReporter,
            this.connectorName,
            this.taskId,
            streamingErrorHandler);

    partitionsToChannel.put(channelName, partitionChannel);
    LOGGER.info("Successfully created streaming channel: {}", channelName);
  }

  private void waitForAllChannelsToCommitData() {
    int channelCount = partitionsToChannel.size();
    if (channelCount == 0) {
      return;
    }

    LOGGER.info("Starting parallel flush for {} channels", channelCount);

    CompletableFuture<?>[] futures =
        partitionsToChannel.values().stream()
            .map(TopicPartitionChannel::waitForLastProcessedRecordCommitted)
            .toArray(CompletableFuture[]::new);

    CompletableFuture.allOf(futures).join();

    LOGGER.info("Completed parallel flush for {} channels", channelCount);
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
    String channelName =
        makeChannelName(this.connectorName, record.topic(), record.kafkaPartition());
    // init a new topic partition if it's not presented in cache or if channel is closed
    if (!partitionsToChannel.containsKey(channelName)
        || partitionsToChannel.get(channelName).isChannelClosed()) {
      LOGGER.warn(
          "Topic: {} Partition: {} hasn't been initialized by OPEN function",
          record.topic(),
          record.kafkaPartition());
      startPartition(new TopicPartition(record.topic(), record.kafkaPartition()));
    }

    TopicPartitionChannel channelPartition = partitionsToChannel.get(channelName);
    boolean isFirstRowPerPartitionInBatch = channelsVisitedPerBatch.add(channelName);
    channelPartition.insertRecord(record, isFirstRowPerPartitionInBatch);
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
    String partitionChannelKey =
        makeChannelName(this.connectorName, topicPartition.topic(), topicPartition.partition());
    if (partitionsToChannel.containsKey(partitionChannelKey)) {
      long offset = partitionsToChannel.get(partitionChannelKey).getOffsetSafeToCommitToKafka();
      partitionsToChannel.get(partitionChannelKey).setLatestConsumerGroupOffset(offset);
      LOGGER.info(
          "Fetched snowflake commited offset: [{}] for channel [{}]", offset, partitionChannelKey);
      return offset;
    } else {
      LOGGER.warn(
          "Topic: {} Partition: {} hasn't been initialized to get offset",
          topicPartition.topic(),
          topicPartition.partition());
      return NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE;
    }
  }

  @Override
  public int getPartitionCount() {
    return partitionsToChannel.size();
  }

  @Override
  public void closeAll() {
    LOGGER.info(
        "Closing all {} partition channels for connector: {}, task: {}",
        partitionsToChannel.size(),
        this.connectorName,
        this.taskId);
    closeAllInParallel();
    partitionsToChannel.clear();
    LOGGER.info(
        "Completed closing all partition channels for connector: {}, task: {}",
        this.connectorName,
        this.taskId);
  }

  private void closeAllInParallel() {
    CompletableFuture<?>[] futures =
        partitionsToChannel.entrySet().stream()
            .map(
                entry -> {
                  String channelKey = entry.getKey();
                  TopicPartitionChannel topicPartitionChannel = entry.getValue();

                  LOGGER.info("Closing partition channel:{}", channelKey);
                  return topicPartitionChannel.closeChannelAsync();
                })
            .toArray(CompletableFuture[]::new);

    CompletableFuture.allOf(futures).join();
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
    LOGGER.info(
        "Closing {} partitions for connector: {}, task: {}",
        partitions.size(),
        this.connectorName,
        this.taskId);

    CompletableFuture<?>[] futures =
        partitions.stream().map(this::closeTopicPartition).toArray(CompletableFuture[]::new);

    CompletableFuture.allOf(futures).join();
    LOGGER.info(
        "Closed {} partitions, remaining partitions which are not closed are:{}, with size:{}",
        partitions.size(),
        partitionsToChannel.keySet().toString(),
        partitionsToChannel.size());
  }

  private CompletableFuture<Void> closeTopicPartition(TopicPartition topicPartition) {
    String key =
        makeChannelName(this.connectorName, topicPartition.topic(), topicPartition.partition());

    TopicPartitionChannel topicPartitionChannel = partitionsToChannel.get(key);

    LOGGER.info(
        "Closing partitionChannel:{}, partition:{}, topic:{}",
        topicPartitionChannel == null ? null : topicPartitionChannel.getChannelNameFormatV1(),
        topicPartition.partition(),
        topicPartition.topic());

    // It's possible that some partitions can be unassigned before their respective channels are
    // even created.
    return topicPartitionChannel == null
        ? CompletableFuture.completedFuture(null) // All is good, nothing needs to be done.
        : topicPartitionChannel
            .closeChannelAsync()
            .thenAccept(__ -> partitionsToChannel.remove(key));
  }

  @Override
  public void stop() {
    LOGGER.info(
        "Stopping SnowflakeSinkServiceV2 for connector: {}, task: {}",
        this.connectorName,
        this.taskId);
    waitForAllChannelsToCommitData();

    // Release all streaming clients used by this service
    // Clients will only be closed if no other tasks are using them
    StreamingClientManager.closeTaskClients(connectorName, taskId);
  }

  /* Undefined */
  @Override
  public boolean isClosed() {
    return false;
  }

  @Override
  public Optional<MetricRegistry> getMetricRegistry(String partitionChannelKey) {
    return this.partitionsToChannel.containsKey(partitionChannelKey)
        ? Optional.of(
            this.partitionsToChannel
                .get(partitionChannelKey)
                .getSnowflakeTelemetryChannelStatus()
                .getMetricsJmxReporter()
                .getMetricRegistry())
        : Optional.empty();
  }

  // ------ Streaming Ingest Related Functions ------ //
  private void createTableIfNotExists(final String tableName) {
    if (this.conn.tableExist(tableName)) {
      LOGGER.info("Using existing table {}.", tableName);
      this.conn.appendMetaColIfNotExist(tableName);
    } else {
      LOGGER.info("Creating new table {}.", tableName);
      this.conn.createTableWithOnlyMetadataColumn(tableName);
    }
  }
}
