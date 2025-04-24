package com.snowflake.kafka.connector.internal.streaming;

import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.ENABLE_STREAMING_CLIENT_OPTIMIZATION_DEFAULT;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.SNOWFLAKE_ROLE;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.SNOWPIPE_STREAMING_CLOSE_CHANNELS_IN_PARALLEL;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.SNOWPIPE_STREAMING_CLOSE_CHANNELS_IN_PARALLEL_DEFAULT;
import static com.snowflake.kafka.connector.internal.streaming.channel.TopicPartitionChannel.NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE;

import com.codahale.metrics.MetricRegistry;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig;
import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.dlq.KafkaRecordErrorReporter;
import com.snowflake.kafka.connector.internal.KCLogger;
import com.snowflake.kafka.connector.internal.SnowflakeConnectionService;
import com.snowflake.kafka.connector.internal.SnowflakeErrors;
import com.snowflake.kafka.connector.internal.SnowflakeSinkService;
import com.snowflake.kafka.connector.internal.metrics.MetricsJmxReporter;
import com.snowflake.kafka.connector.internal.streaming.channel.TopicPartitionChannel;
import com.snowflake.kafka.connector.internal.streaming.schemaevolution.InsertErrorMapper;
import com.snowflake.kafka.connector.internal.streaming.schemaevolution.SchemaEvolutionService;
import com.snowflake.kafka.connector.internal.streaming.schemaevolution.iceberg.IcebergSchemaEvolutionService;
import com.snowflake.kafka.connector.internal.streaming.schemaevolution.snowflake.SnowflakeSchemaEvolutionService;
import com.snowflake.kafka.connector.internal.telemetry.SnowflakeTelemetryService;
import com.snowflake.kafka.connector.records.RecordService;
import com.snowflake.kafka.connector.records.RecordServiceFactory;
import com.snowflake.kafka.connector.records.SnowflakeMetadataConfig;
import com.snowflake.kafka.connector.streaming.iceberg.IcebergInitService;
import com.snowflake.kafka.connector.streaming.iceberg.IcebergTableSchemaValidator;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
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

  private final RecordService recordService;
  private final SnowflakeTelemetryService telemetryService;

  private final IcebergTableSchemaValidator icebergTableSchemaValidator;
  private final IcebergInitService icebergInitService;

  private SchemaEvolutionService schemaEvolutionService;

  private Map<String, String> topicToTableMap;

  // Behavior to be set at the start of connector start. (For tombstone records)
  private SnowflakeSinkConnectorConfig.BehaviorOnNullValues behaviorOnNullValues;

  // default is true unless the configuration provided is false;
  // If this is true, we will enable Mbean for required classes and emit JMX metrics for monitoring
  private boolean enableCustomJMXMonitoring = SnowflakeSinkConnectorConfig.JMX_OPT_DEFAULT;
  private MetricsJmxReporter metricsJmxReporter;

  /**
   * Fetching this from {@link org.apache.kafka.connect.sink.SinkTaskContext}'s {@link
   * org.apache.kafka.connect.sink.ErrantRecordReporter}
   */
  private KafkaRecordErrorReporter kafkaRecordErrorReporter;

  /* SinkTaskContext has access to all methods/APIs available to talk to Kafka Connect runtime*/
  private SinkTaskContext sinkTaskContext;

  // ------ Streaming Ingest ------ //
  // needs url, username. p8 key, role name
  private SnowflakeStreamingIngestClient streamingIngestClient;

  // Config set in JSON
  private final Map<String, String> connectorConfig;

  private final boolean enableSchematization;

  private final boolean closeChannelsInParallel;

  /**
   * Key is formulated in {@link #partitionChannelKey(String, int)} }
   *
   * <p>value is the Streaming Ingest Channel implementation (Wrapped around TopicPartitionChannel)
   */
  private final Map<String, TopicPartitionChannel> partitionsToChannel;

  // Cache for schema evolution
  private final Map<String, Boolean> tableName2SchemaEvolutionPermission;

  // Set that keeps track of the channels that have been seen per input batch
  private final Set<String> channelsVisitedPerBatch = new HashSet<>();

  public SnowflakeSinkServiceV2(
      SnowflakeConnectionService conn,
      Map<String, String> connectorConfig,
      KafkaRecordErrorReporter recordErrorReporter,
      SinkTaskContext sinkTaskContext,
      boolean enableCustomJMXMonitoring,
      Map<String, String> topicToTableMap,
      SchemaEvolutionService schemaEvolutionService) {
    this(conn, connectorConfig);
    this.kafkaRecordErrorReporter = recordErrorReporter;
    this.sinkTaskContext = sinkTaskContext;
    this.enableCustomJMXMonitoring = enableCustomJMXMonitoring;
    this.topicToTableMap = topicToTableMap;
    this.schemaEvolutionService = schemaEvolutionService;
  }

  @Deprecated
  public SnowflakeSinkServiceV2(
      SnowflakeConnectionService conn, Map<String, String> connectorConfig) {
    if (conn == null || conn.isClosed()) {
      throw SnowflakeErrors.ERROR_5010.getException();
    }

    this.conn = conn;
    this.telemetryService = conn.getTelemetryClient();
    boolean schematizationEnabled = Utils.isSchematizationEnabled(connectorConfig);
    this.recordService =
        RecordServiceFactory.createRecordService(
            Utils.isIcebergEnabled(connectorConfig), schematizationEnabled);
    this.icebergTableSchemaValidator = new IcebergTableSchemaValidator(conn);
    this.icebergInitService = new IcebergInitService(conn);
    this.schemaEvolutionService =
        Utils.isIcebergEnabled(connectorConfig)
            ? new IcebergSchemaEvolutionService(conn)
            : new SnowflakeSchemaEvolutionService(conn);

    this.topicToTableMap = new HashMap<>();

    // Setting the default value in constructor
    // meaning it will not ignore the null values (Tombstone records wont be ignored/filtered)
    this.behaviorOnNullValues = SnowflakeSinkConnectorConfig.BehaviorOnNullValues.DEFAULT;

    this.connectorConfig = connectorConfig;

    this.enableSchematization = schematizationEnabled;

    this.closeChannelsInParallel =
        Optional.ofNullable(connectorConfig.get(SNOWPIPE_STREAMING_CLOSE_CHANNELS_IN_PARALLEL))
            .map(Boolean::parseBoolean)
            .orElse(SNOWPIPE_STREAMING_CLOSE_CHANNELS_IN_PARALLEL_DEFAULT);

    this.streamingIngestClient =
        StreamingClientProvider.getStreamingClientProviderInstance()
            .getClient(this.connectorConfig);

    this.partitionsToChannel = new HashMap<>();

    this.tableName2SchemaEvolutionPermission = new HashMap<>();

    // jmx
    String connectorName =
        conn == null || Strings.isNullOrEmpty(this.conn.getConnectorName())
            ? "default_connector"
            : this.conn.getConnectorName();
    this.metricsJmxReporter = new MetricsJmxReporter(new MetricRegistry(), connectorName);
  }

  /**
   * Creates a table if it doesnt exist in Snowflake.
   *
   * <p>Initializes the Channel and partitionsToChannel map with new instance of {@link
   * TopicPartitionChannel}
   *
   * @param tableName destination table name
   * @param topicPartition TopicPartition passed from Kafka
   */
  @Override
  public void startPartition(String tableName, TopicPartition topicPartition) {
    // the table should be present before opening a channel so let's do a table existence check here
    tableActionsOnStartPartition(tableName);

    // Create channel for the given partition
    createStreamingChannelForTopicPartition(
        tableName, topicPartition, tableName2SchemaEvolutionPermission.get(tableName));
  }

  /**
   * Initializes multiple Channels and partitionsToChannel maps with new instances of {@link
   * TopicPartitionChannel}
   *
   * @param partitions collection of topic partition
   * @param topic2Table map of topic to table name
   */
  @Override
  public void startPartitions(
      Collection<TopicPartition> partitions, Map<String, String> topic2Table) {
    partitions.stream()
        .map(TopicPartition::topic)
        .distinct()
        .forEach(topic -> perTopicActionsOnStartPartitions(topic, topic2Table));
    partitions.forEach(
        tp -> {
          String tableName = Utils.tableName(tp.topic(), topic2Table);
          createStreamingChannelForTopicPartition(
              tableName, tp, tableName2SchemaEvolutionPermission.get(tableName));
        });
  }

  private void perTopicActionsOnStartPartitions(String topic, Map<String, String> topic2Table) {
    String tableName = Utils.tableName(topic, topic2Table);
    tableActionsOnStartPartition(tableName);
  }

  private void tableActionsOnStartPartition(String tableName) {
    if (Utils.isIcebergEnabled(connectorConfig)) {
      icebergTableSchemaValidator.validateTable(
          tableName, Utils.role(connectorConfig), enableSchematization);
      icebergInitService.initializeIcebergTableProperties(tableName);
      populateSchemaEvolutionPermissions(tableName);
    } else {
      createTableIfNotExists(tableName);
    }
  }

  /**
   * Always opens a new channel and creates a new instance of TopicPartitionChannel.
   *
   * <p>This is essentially a blind write to partitionsToChannel. i.e. we do not check if it is
   * presented or not.
   */
  private void createStreamingChannelForTopicPartition(
      final String tableName,
      final TopicPartition topicPartition,
      boolean hasSchemaEvolutionPermission) {
    final String partitionChannelKey =
        partitionChannelKey(topicPartition.topic(), topicPartition.partition());
    // Create new instance of TopicPartitionChannel which will always open the channel.
    partitionsToChannel.put(
        partitionChannelKey,
        createTopicPartitionChannel(
            tableName, topicPartition, hasSchemaEvolutionPermission, partitionChannelKey));
  }

  private TopicPartitionChannel createTopicPartitionChannel(
      String tableName,
      TopicPartition topicPartition,
      boolean hasSchemaEvolutionPermission,
      String partitionChannelKey) {

    return new DirectTopicPartitionChannel(
        this.streamingIngestClient,
        topicPartition,
        partitionChannelKey, // Streaming channel name
        tableName,
        hasSchemaEvolutionPermission,
        this.connectorConfig,
        this.kafkaRecordErrorReporter,
        this.sinkTaskContext,
        this.conn,
        this.recordService,
        this.conn.getTelemetryClient(),
        this.enableCustomJMXMonitoring,
        this.metricsJmxReporter,
        this.schemaEvolutionService,
        new InsertErrorMapper());
  }

  /**
   * Inserts the given record into buffer and then eventually calls insertRows API if buffer
   * threshold has reached.
   *
   * <p>TODO: SNOW-473896 - Please note we will get away with Buffering logic in future commits.
   *
   * @param records records coming from Kafka. Please note, they are not just from single topic and
   *     partition. It depends on the kafka connect worker node which can consume from multiple
   *     Topic and multiple Partitions
   */
  @Override
  public void insert(final Collection<SinkRecord> records) {
    // note that records can be empty but, we will still need to check for time based flush
    channelsVisitedPerBatch.clear();
    for (SinkRecord record : records) {
      // check if it needs to handle null value records
      if (recordService.shouldSkipNullValue(record, behaviorOnNullValues)) {
        continue;
      }

      // While inserting into buffer, we will check for count threshold and buffered bytes
      // threshold.
      insert(record);
    }
  }

  /**
   * Inserts individual records into buffer. It fetches the TopicPartitionChannel from the map and
   * then each partition(Streaming channel) calls its respective insertRows API
   *
   * @param record record content
   */
  @Override
  public void insert(SinkRecord record) {
    String partitionChannelKey = partitionChannelKey(record.topic(), record.kafkaPartition());
    // init a new topic partition if it's not presented in cache or if channel is closed
    if (!partitionsToChannel.containsKey(partitionChannelKey)
        || partitionsToChannel.get(partitionChannelKey).isChannelClosed()) {
      LOGGER.warn(
          "Topic: {} Partition: {} hasn't been initialized by OPEN function",
          record.topic(),
          record.kafkaPartition());
      startPartition(
          Utils.tableName(record.topic(), this.topicToTableMap),
          new TopicPartition(record.topic(), record.kafkaPartition()));
    }

    TopicPartitionChannel channelPartition = partitionsToChannel.get(partitionChannelKey);
    boolean isFirstRowPerPartitionInBatch = channelsVisitedPerBatch.add(partitionChannelKey);
    channelPartition.insertRecord(record, isFirstRowPerPartitionInBatch);
  }

  @Override
  public long getOffset(TopicPartition topicPartition) {
    String partitionChannelKey =
        partitionChannelKey(topicPartition.topic(), topicPartition.partition());
    if (partitionsToChannel.containsKey(partitionChannelKey)) {
      long offset = partitionsToChannel.get(partitionChannelKey).getOffsetSafeToCommitToKafka();
      partitionsToChannel.get(partitionChannelKey).setLatestConsumerGroupOffset(offset);

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
  public void callAllGetOffset() {
    // undefined
  }

  @Override
  public void closeAll() {
    if (closeChannelsInParallel) {
      closeAllInParallel();
    } else {
      closeAllSequentially();
    }

    partitionsToChannel.clear();

    StreamingClientProvider.getStreamingClientProviderInstance()
        .closeClient(this.connectorConfig, this.streamingIngestClient);
  }

  private void closeAllSequentially() {
    partitionsToChannel.forEach(
        (partitionChannelKey, topicPartitionChannel) -> {
          LOGGER.info("Closing partition channel:{}", partitionChannelKey);
          topicPartitionChannel.closeChannel();
        });
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
    if (closeChannelsInParallel) {
      closeInParallel(partitions);
    } else {
      closeSequentially(partitions);
    }

    LOGGER.info(
        "Closing {} partitions and remaining partitions which are not closed are:{}, with size:{}",
        partitions.size(),
        partitionsToChannel.keySet().toString(),
        partitionsToChannel.size());
  }

  private void closeSequentially(Collection<TopicPartition> partitions) {
    partitions.forEach(
        topicPartition -> {
          final String partitionChannelKey =
              partitionChannelKey(topicPartition.topic(), topicPartition.partition());
          TopicPartitionChannel topicPartitionChannel =
              partitionsToChannel.get(partitionChannelKey);
          // Check for null since it's possible that the something goes wrong even before the
          // channels are created
          if (topicPartitionChannel != null) {
            topicPartitionChannel.closeChannel();
          }
          LOGGER.info(
              "Closing partitionChannel:{}, partition:{}, topic:{}",
              topicPartitionChannel == null ? null : topicPartitionChannel.getChannelNameFormatV1(),
              topicPartition.partition(),
              topicPartition.topic());
          partitionsToChannel.remove(partitionChannelKey);
        });
  }

  private void closeInParallel(Collection<TopicPartition> partitions) {
    CompletableFuture<?>[] futures =
        partitions.stream().map(this::closeTopicPartition).toArray(CompletableFuture[]::new);

    CompletableFuture.allOf(futures).join();
  }

  private CompletableFuture<Void> closeTopicPartition(TopicPartition topicPartition) {
    String key = partitionChannelKey(topicPartition.topic(), topicPartition.partition());

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
    final boolean isOptimizationEnabled =
        Boolean.parseBoolean(
            connectorConfig.getOrDefault(
                SnowflakeSinkConnectorConfig.ENABLE_STREAMING_CLIENT_OPTIMIZATION_CONFIG,
                Boolean.toString(ENABLE_STREAMING_CLIENT_OPTIMIZATION_DEFAULT)));
    // when optimization is enabled single streamingIngestClient instance may be used by many
    // SinkService instances
    // stopping the client may cause unexpected behaviour
    if (!isOptimizationEnabled) {
      try {
        StreamingClientProvider.getStreamingClientProviderInstance()
            .closeClient(connectorConfig, this.streamingIngestClient);
      } catch (Exception e) {
        LOGGER.warn(
            "Could not close streaming ingest client {}. Reason: {}",
            streamingIngestClient.getName(),
            e.getMessage());
      }
    }
  }

  /* Undefined */
  @Override
  public boolean isClosed() {
    return false;
  }

  @Override
  public void setRecordNumber(long num) {
    // TODO - remove from this class
  }

  /**
   * Assume this is buffer size in bytes, since this is streaming ingestion
   *
   * @param size in bytes - a non negative long number representing size of internal buffer for
   *     flush.
   */
  @Override
  public void setFileSize(long size) {
    // TODO - remove from this class
  }

  @Override
  public void setTopic2TableMap(Map<String, String> topicToTableMap) {
    this.topicToTableMap = topicToTableMap;
  }

  @Override
  public void setFlushTime(long time) {
    // TODO - remove from this class
  }

  @Override
  public void setMetadataConfig(SnowflakeMetadataConfig configMap) {
    this.recordService.setMetadataConfig(configMap);
  }

  @Override
  public void setBehaviorOnNullValuesConfig(
      SnowflakeSinkConnectorConfig.BehaviorOnNullValues behavior) {
    this.behaviorOnNullValues = behavior;
  }

  @Override
  public void setCustomJMXMetrics(boolean enableJMX) {
    this.enableCustomJMXMonitoring = enableJMX;
  }

  @Override
  public SnowflakeSinkConnectorConfig.BehaviorOnNullValues getBehaviorOnNullValuesConfig() {
    return this.behaviorOnNullValues;
  }

  /* Set this to send records to DLQ. */
  @Override
  public void setErrorReporter(KafkaRecordErrorReporter kafkaRecordErrorReporter) {
    this.kafkaRecordErrorReporter = kafkaRecordErrorReporter;
  }

  @Override
  public void setSinkTaskContext(SinkTaskContext sinkTaskContext) {
    this.sinkTaskContext = sinkTaskContext;
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

  /**
   * Gets a unique identifier consisting of connector name, topic name and partition number.
   *
   * @param topic topic name
   * @param partition partition number
   * @return combinartion of topic and partition
   */
  @VisibleForTesting
  public static String partitionChannelKey(String topic, int partition) {
    return topic + "_" + partition;
  }

  /* Used for testing */
  @VisibleForTesting
  public SnowflakeStreamingIngestClient getStreamingIngestClient() {
    return this.streamingIngestClient;
  }

  /**
   * Used for testing Only
   *
   * @param topicPartitionChannelKey look {@link #partitionChannelKey(String, int)} for key format
   * @return TopicPartitionChannel if present in partitionsToChannel Map else null
   */
  @VisibleForTesting
  protected Optional<TopicPartitionChannel> getTopicPartitionChannelFromCacheKey(
      final String topicPartitionChannelKey) {
    return Optional.ofNullable(
        this.partitionsToChannel.getOrDefault(topicPartitionChannelKey, null));
  }

  // ------ Streaming Ingest Related Functions ------ //
  private void createTableIfNotExists(final String tableName) {
    if (this.conn.tableExist(tableName)) {
      if (!this.enableSchematization) {
        if (this.conn.isTableCompatible(tableName)) {
          LOGGER.info("Using existing table {}.", tableName);
        } else {
          throw SnowflakeErrors.ERROR_5003.getException(
              "table name: " + tableName, this.telemetryService);
        }
      } else {
        this.conn.appendMetaColIfNotExist(tableName);
      }
    } else {
      LOGGER.info("Creating new table {}.", tableName);
      if (this.enableSchematization) {
        // Always create the table with RECORD_METADATA only and rely on schema evolution to update
        // the schema
        this.conn.createTableWithOnlyMetadataColumn(tableName);
      } else {
        this.conn.createTable(tableName);
      }
    }

    // Populate schema evolution cache if needed
    populateSchemaEvolutionPermissions(tableName);
  }

  private void populateSchemaEvolutionPermissions(String tableName) {
    if (!tableName2SchemaEvolutionPermission.containsKey(tableName)) {
      if (enableSchematization) {
        boolean hasSchemaEvolutionPermission =
            conn != null
                && conn.hasSchemaEvolutionPermission(
                    tableName, connectorConfig.get(SNOWFLAKE_ROLE));
        LOGGER.info(
            "[SCHEMA_EVOLUTION_CACHE] Setting {} for table {}",
            hasSchemaEvolutionPermission,
            tableName);
        tableName2SchemaEvolutionPermission.put(tableName, hasSchemaEvolutionPermission);
      } else {
        LOGGER.info(
            "[SCHEMA_EVOLUTION_CACHE] Schematization disabled. Setting false for table {}",
            tableName);
        tableName2SchemaEvolutionPermission.put(tableName, false);
      }
    }
  }
}
