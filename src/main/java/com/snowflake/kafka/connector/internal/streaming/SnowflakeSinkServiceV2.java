package com.snowflake.kafka.connector.internal.streaming;

import com.codahale.metrics.MetricRegistry;
import com.google.common.annotations.VisibleForTesting;
import com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig;
import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.internal.Logging;
import com.snowflake.kafka.connector.internal.SnowflakeConnectionService;
import com.snowflake.kafka.connector.internal.SnowflakeErrors;
import com.snowflake.kafka.connector.internal.SnowflakeSinkService;
import com.snowflake.kafka.connector.internal.SnowflakeTelemetryService;
import com.snowflake.kafka.connector.records.RecordService;
import com.snowflake.kafka.connector.records.SnowflakeMetadataConfig;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import net.snowflake.ingest.streaming.OpenChannelRequest;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClientFactory;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  private static final Logger LOGGER = LoggerFactory.getLogger(SnowflakeSinkServiceV2.class);

  private static String STREAMING_CLIENT_PREFIX_NAME = "KC_CLIENT_";

  // Assume next three values are a threshold after which we will call insertRows API
  // Set in config (Time based flush) in seconds
  private long flushTimeSeconds;
  // Set in config (buffer size based flush) in bytes
  private long fileSizeBytes;

  // Set in config (Threshold before we call insertRows API) corresponds to # of
  // records in kafka
  private long recordNum;

  // Used to connect to Snowflake
  private final SnowflakeConnectionService conn;

  private final RecordService recordService;
  private final SnowflakeTelemetryService telemetryService;
  private Map<String, String> topicToTableMap;

  // Behavior to be set at the start of connector start. (For tombstone records)
  private SnowflakeSinkConnectorConfig.BehaviorOnNullValues behaviorOnNullValues;

  // default is true unless the configuration provided is false;
  // If this is true, we will enable Mbean for required classes and emit JMX metrics for monitoring
  private boolean enableCustomJMXMonitoring = SnowflakeSinkConnectorConfig.JMX_OPT_DEFAULT;

  // default is at_least_once semantic (To begin with)
  // TODO: SNOW-526435
  private SnowflakeSinkConnectorConfig.IngestionDeliveryGuarantee ingestionDeliveryGuarantee =
      SnowflakeSinkConnectorConfig.IngestionDeliveryGuarantee.AT_LEAST_ONCE;

  // ------ Streaming Ingest ------ //
  // needs url, username. p8 key, role name
  private SnowflakeStreamingIngestClient streamingIngestClient;

  // Config set in JSON
  private final Map<String, String> connectorConfig;

  private final String taskId;

  private final String streamingIngestClientName;

  /**
   * Key is formulated in {@link #partitionChannelKey(String, int)} }
   *
   * <p>value is the Streaming Ingest Channel implementation (Wrapped around TopicPartitionChannel)
   */
  private final Map<String, TopicPartitionChannel> partitionsToChannel;

  /**
   * Public ctor used from SnowflakeSinkTask
   *
   * @param conn Used for Connecting to Snowflake (JDBC)
   * @param connectorConfig all user defined properties
   */
  public SnowflakeSinkServiceV2(
      SnowflakeConnectionService conn, Map<String, String> connectorConfig) {
    if (conn == null || conn.isClosed()) {
      throw SnowflakeErrors.ERROR_5010.getException();
    }

    this.fileSizeBytes = SnowflakeSinkConnectorConfig.BUFFER_SIZE_BYTES_DEFAULT;
    this.recordNum = SnowflakeSinkConnectorConfig.BUFFER_COUNT_RECORDS_DEFAULT;
    this.flushTimeSeconds = SnowflakeSinkConnectorConfig.BUFFER_FLUSH_TIME_SEC_DEFAULT;
    this.conn = conn;
    this.recordService = new RecordService();
    this.telemetryService = conn.getTelemetryClient();
    this.topicToTableMap = new HashMap<>();

    // Setting the default value in constructor
    // meaning it will not ignore the null values (Tombstone records wont be ignored/filtered)
    this.behaviorOnNullValues = SnowflakeSinkConnectorConfig.BehaviorOnNullValues.DEFAULT;

    this.connectorConfig = connectorConfig;
    this.taskId = connectorConfig.getOrDefault(Utils.TASK_ID, "-1");
    this.streamingIngestClientName =
        STREAMING_CLIENT_PREFIX_NAME + conn.getConnectorName() + "_" + taskId;
    initStreamingClient();
    this.partitionsToChannel = new HashMap<>();
  }

  @Override
  public void startTask(String tableName, String topic, int partition) {
    String partitionChannelKey = partitionChannelKey(topic, partition);
    // the table should be present before opening a channel so lets do a table existence check here
    createTableIfNotExists(tableName);
    SnowflakeStreamingIngestChannel partitionChannel =
        openChannelForTable(partitionChannelKey, tableName);
    TopicPartitionChannel topicPartitionChannel =
        new TopicPartitionChannel(partitionChannel, this.conn, tableName);
    partitionsToChannel.putIfAbsent(partitionChannelKey, topicPartitionChannel);
  }

  /** @param records record content */
  @Override
  public void insert(Collection<SinkRecord> records) {
    Map<String, List<SinkRecord>> topicPartitionsToRecords = new HashMap<>();
    for (SinkRecord record : records) {
      // check if need to handle null value records
      if (recordService.shouldSkipNullValue(record, behaviorOnNullValues)) {
        continue;
      }
      String topicPartitionChannelName = getOrInitTopicPartitionChannelName(record);
      // if present, just add.
      // if not present, create a new arraylist and then add it to the list
      topicPartitionsToRecords
          .computeIfAbsent(topicPartitionChannelName, mapKey -> new ArrayList<>())
          .add(record);
    }

    // we will not worry about any flushing logic, once we know which records corresponds to which
    // topicPartitions, we will start processing those records

    // once they are processed, we will immediately call insertRows API and not buffer
    // TODO: SNOW-536611 Optimize processing and calling insertRows in its own thread
    // Take a look at
    // https://github.com/confluentinc/kafka-connect-bigquery/blob/master/kcbq-connector/src/main/java/com/wepay/kafka/connect/bigquery/write/batch/GCSBatchTableWriter.java
    // they do similar thing and run it in multiple threads as runnable task
    topicPartitionsToRecords.forEach(
        (topicPartitionChannelName, sinkRecords) -> {
          TopicPartitionChannel partitionChannel =
              partitionsToChannel.get(topicPartitionChannelName);
          partitionChannel.processAndInsertSinkRecords(sinkRecords);
        });
  }

  /**
   * Return a unique string consisting topic and partition number and insert it into a
   * partitionsToChannel Map if that key was not present before.
   *
   * If unique key is not present in {@link #partitionsToChannel} map, start the task too.
   *
   * @param record record to fetch Topic name and partition no.
   * @return unique
   */
  private String getOrInitTopicPartitionChannelName(SinkRecord record) {
    String partitionChannelKey = partitionChannelKey(record.topic(), record.kafkaPartition());
    // init a new topic partition
    if (!partitionsToChannel.containsKey(partitionChannelKey)) {
      LOGGER.warn(
          "Topic: {} Partition: {} hasn't been initialized by OPEN " + "function",
          record.topic(),
          record.kafkaPartition());
      startTask(
          Utils.tableName(record.topic(), this.topicToTableMap),
          record.topic(),
          record.kafkaPartition());
    }
    return partitionChannelKey;
  }

  /**
   * We use {@link #insert(Collection)} in production. So this single record internally calls
   * mentioned function wrapped around a list.
   *
   * @param record record content
   */
  @Override
  public void insert(SinkRecord record) {
    insert(Collections.singletonList(record));
  }

  @Override
  public long getOffset(TopicPartition topicPartition) {
    String partitionChannelKey =
        partitionChannelKey(topicPartition.topic(), topicPartition.partition());
    if (partitionsToChannel.containsKey(partitionChannelKey)) {
      return partitionsToChannel.get(partitionChannelKey).getCommittedOffset();
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
    return partitionsToChannel.size();
  }

  @Override
  public void callAllGetOffset() {
    // undefined
  }

  @Override
  public void closeAll() {
    partitionsToChannel.forEach(
        (partitionChannelKey, topicPartitionChannel) -> {
          LOGGER.info("Closing partition channel:{}", partitionChannelKey);
          topicPartitionChannel.closeChannel();
        });
    partitionsToChannel.clear();
    closeStreamingClient();
  }

  @Override
  public void close(Collection<TopicPartition> partitions) {
    partitions.forEach(
        topicPartition -> {
          String partitionChannelKey =
              partitionChannelKey(topicPartition.topic(), topicPartition.partition());
          LOGGER.info("Closing partition channel:{}", partitionChannelKey);
          partitionsToChannel.get(partitionChannelKey).closeChannel();
          partitionsToChannel.remove(partitionChannelKey);
        });

    closeStreamingClient();
  }

  @Override
  public void setIsStoppedToTrue() {}

  /* Undefined */
  @Override
  public boolean isClosed() {
    return false;
  }

  @Override
  public void setRecordNumber(long num) {
    if (num < 0) {
      LOGGER.error("number of record in each file is {}, it is negative, reset to 0", num);
      this.recordNum = 0;
    } else {
      this.recordNum = num;
      LOGGER.info("set number of record limitation to {}", num);
    }
  }

  /**
   * Assume this is buffer size in bytes, since this is streaming ingestion
   *
   * <p>Copying this from SinkServiceV1 and we will get away from this in future
   *
   * @param size a non negative long number represents data size limitation
   */
  @Override
  public void setFileSize(long size) {
    if (size < SnowflakeSinkConnectorConfig.BUFFER_SIZE_BYTES_MIN) {
      LOGGER.error(
          "Buffer size is {} bytes, it is smaller than the minimum buffer "
              + "size {} bytes, reset to the default file size",
          size,
          SnowflakeSinkConnectorConfig.BUFFER_SIZE_BYTES_DEFAULT);
      this.fileSizeBytes = SnowflakeSinkConnectorConfig.BUFFER_SIZE_BYTES_DEFAULT;
    } else {
      this.fileSizeBytes = size;
      LOGGER.info("set buffer size limitation to {} bytes", size);
    }
  }

  @Override
  public void setTopic2TableMap(Map<String, String> topicToTableMap) {
    this.topicToTableMap = topicToTableMap;
  }

  @Override
  public void setFlushTime(long time) {
    if (time < SnowflakeSinkConnectorConfig.BUFFER_FLUSH_TIME_SEC_MIN) {
      LOGGER.error(
          "flush time is {} seconds, it is smaller than the minimum "
              + "flush time {} seconds, reset to the minimum flush time",
          time,
          SnowflakeSinkConnectorConfig.BUFFER_FLUSH_TIME_SEC_MIN);
      this.flushTimeSeconds = SnowflakeSinkConnectorConfig.BUFFER_FLUSH_TIME_SEC_MIN;
    } else {
      this.flushTimeSeconds = time;
      LOGGER.info("set flush time to {} seconds", time);
    }
  }

  @Override
  public void setMetadataConfig(SnowflakeMetadataConfig configMap) {
    this.recordService.setMetadataConfig(configMap);
  }

  @Override
  public long getRecordNumber() {
    return this.recordNum;
  }

  @Override
  public long getFlushTime() {
    return this.flushTimeSeconds;
  }

  @Override
  public long getFileSize() {
    return this.fileSizeBytes;
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

  @Override
  public void setDeliveryGuarantee(
      SnowflakeSinkConnectorConfig.IngestionDeliveryGuarantee ingestionDeliveryGuarantee) {
    this.ingestionDeliveryGuarantee = ingestionDeliveryGuarantee;
  }

  @Override
  public Optional<MetricRegistry> getMetricRegistry(String pipeName) {
    return Optional.empty();
  }

  /**
   * Gets a unique identifier consisting of topic name and partition number.
   *
   * @param topic topic name
   * @param partition partition number
   * @return combinartion of topic and partition
   */
  @VisibleForTesting
  protected static String partitionChannelKey(String topic, int partition) {
    return topic + "_" + partition;
  }

  // ------ Streaming Ingest Related Functions ------ //

  /* Open a channel for Table with given channel name and tableName */
  private SnowflakeStreamingIngestChannel openChannelForTable(
      final String channelName, final String tableName) {
    OpenChannelRequest channelRequest =
        OpenChannelRequest.builder(channelName)
            .setDBName(this.connectorConfig.get(Utils.SF_DATABASE))
            .setSchemaName(this.connectorConfig.get(Utils.SF_SCHEMA))
            .setTableName(tableName)
            .setOnErrorOption(OpenChannelRequest.OnErrorOption.CONTINUE)
            .build();
    if (streamingIngestClient.isClosed()) {
      initStreamingClient();
    }
    LOGGER.info("Opening a channel with name:{} for table name:{}", channelName, tableName);
    return streamingIngestClient.openChannel(channelRequest);
  }

  /* Init Streaming client. If is also used to re-init the client if client was closed before. */
  private void initStreamingClient() {
    Properties streamingClientProps = new Properties();
    streamingClientProps.putAll(connectorConfig);
    if (this.streamingIngestClient == null || this.streamingIngestClient.isClosed()) {
      LOGGER.debug("Initializing Streaming Client. ClientName:{}", this.streamingIngestClientName);
      this.streamingIngestClient =
          SnowflakeStreamingIngestClientFactory.builder(this.streamingIngestClientName)
              .setProperties(streamingClientProps)
              .build();
    }
  }

  private void closeStreamingClient() {
    // do we need to close the client? If I close, I will have to re init the client upon rebalance
    // in open()
    LOGGER.info("Closing Streaming Client:{}", this.streamingIngestClientName);
    try {
      streamingIngestClient.close().get();
    } catch (InterruptedException | ExecutionException e) {
      LOGGER.error(
          Logging.logMessage(
              "Failure closing Streaming client msg:{}, cause:{}",
              e.getMessage(),
              Arrays.toString(e.getCause().getStackTrace())));
    }
  }

  private void createTableIfNotExists(final String tableName) {
    if (this.conn.tableExist(tableName)) {
      if (this.conn.isTableCompatible(tableName)) {
        LOGGER.info("Using existing table {}.", tableName);
      } else {
        throw SnowflakeErrors.ERROR_5003.getException("table name: " + tableName);
      }
    } else {
      LOGGER.info("Creating new table {}.", tableName);
      this.conn.createTable(tableName);
    }
  }
}
