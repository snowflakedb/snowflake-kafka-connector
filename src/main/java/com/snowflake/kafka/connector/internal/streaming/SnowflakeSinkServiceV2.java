package com.snowflake.kafka.connector.internal.streaming;

import com.codahale.metrics.MetricRegistry;
import com.google.common.annotations.VisibleForTesting;
import com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig;
import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.dlq.KafkaRecordErrorReporter;
import com.snowflake.kafka.connector.internal.Logging;
import com.snowflake.kafka.connector.internal.SnowflakeConnectionService;
import com.snowflake.kafka.connector.internal.SnowflakeErrors;
import com.snowflake.kafka.connector.internal.SnowflakeSinkService;
import com.snowflake.kafka.connector.internal.telemetry.SnowflakeTelemetryService;
import com.snowflake.kafka.connector.records.RecordService;
import com.snowflake.kafka.connector.records.SnowflakeMetadataConfig;
import io.confluent.connect.avro.AvroConverterConfig;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClientFactory;
import net.snowflake.ingest.utils.SFException;
import org.apache.avro.Schema;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.BUFFER_SIZE_BYTES_DEFAULT;
import static com.snowflake.kafka.connector.internal.streaming.StreamingUtils.STREAMING_BUFFER_COUNT_RECORDS_DEFAULT;
import static com.snowflake.kafka.connector.internal.streaming.StreamingUtils.STREAMING_BUFFER_FLUSH_TIME_DEFAULT_SEC;

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

  // We will make this non configurable if ingestion method is SNOWPIPE_STREAMING
  private SnowflakeSinkConnectorConfig.IngestionDeliveryGuarantee ingestionDeliveryGuarantee =
      SnowflakeSinkConnectorConfig.IngestionDeliveryGuarantee.EXACTLY_ONCE;

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

  private final String taskId;

  private final String streamingIngestClientName;

  private boolean enableSchematization = false;

  /**
   * Key is formulated in {@link #partitionChannelKey(String, int)} }
   *
   * <p>value is the Streaming Ingest Channel implementation (Wrapped around TopicPartitionChannel)
   */
  private final Map<String, TopicPartitionChannel> partitionsToChannel;

  public SnowflakeSinkServiceV2(
      SnowflakeConnectionService conn, Map<String, String> connectorConfig) {
    if (conn == null || conn.isClosed()) {
      throw SnowflakeErrors.ERROR_5010.getException();
    }

    this.fileSizeBytes = StreamingUtils.STREAMING_BUFFER_BYTES_DEFAULT;
    this.recordNum = StreamingUtils.STREAMING_BUFFER_COUNT_RECORDS_DEFAULT;
    this.flushTimeSeconds = StreamingUtils.STREAMING_BUFFER_FLUSH_TIME_DEFAULT_SEC;
    this.conn = conn;
    this.recordService = new RecordService();
    this.telemetryService = conn.getTelemetryClient();
    this.topicToTableMap = new HashMap<>();

    // Setting the default value in constructor
    // meaning it will not ignore the null values (Tombstone records wont be ignored/filtered)
    this.behaviorOnNullValues = SnowflakeSinkConnectorConfig.BehaviorOnNullValues.DEFAULT;

    this.connectorConfig = connectorConfig;

    if (connectorConfig.containsKey(SnowflakeSinkConnectorConfig.SCHEMATIZATION_ENABLE_CONFIG)) {
      this.enableSchematization =
          Boolean.parseBoolean(
              connectorConfig.get(SnowflakeSinkConnectorConfig.SCHEMATIZATION_ENABLE_CONFIG));
    }

    this.recordService.setSchematizationEnable(this.enableSchematization);

    this.taskId = connectorConfig.getOrDefault(Utils.TASK_ID, "-1");
    this.streamingIngestClientName =
        STREAMING_CLIENT_PREFIX_NAME + conn.getConnectorName() + "_" + taskId;
    initStreamingClient();
    this.partitionsToChannel = new HashMap<>();
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
  public void startTask(String tableName, TopicPartition topicPartition) {
    // the table should be present before opening a channel so lets do a table existence check here
    createTableIfNotExists(tableName);

    createStreamingChannelForTopicPartition(tableName, topicPartition);
  }

  /**
   * Always opens a new channel and creates a new instance of TopicPartitionChannel.
   *
   * <p>This is essentially a blind write to partitionsToChannel. i.e we dont check if it is present
   * or not.
   */
  private void createStreamingChannelForTopicPartition(
      final String tableName, final TopicPartition topicPartition) {
    final String partitionChannelKey =
        partitionChannelKey(topicPartition.topic(), topicPartition.partition());
    // Create new instance of TopicPartitionChannel which will always open the channel.
    partitionsToChannel.put(
        partitionChannelKey,
        new TopicPartitionChannel(
            this.streamingIngestClient,
            topicPartition,
            partitionChannelKey, // Streaming channel name
            tableName,
            new StreamingBufferThreshold(this.flushTimeSeconds, this.fileSizeBytes, this.recordNum),
            this.connectorConfig,
            this.kafkaRecordErrorReporter,
            this.sinkTaskContext));
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
  public void insert(Collection<SinkRecord> records) {
    // note that records can be empty but, we will still need to check for time based flush
    for (SinkRecord record : records) {
      // check if need to handle null value records
      if (recordService.shouldSkipNullValue(record, behaviorOnNullValues)) {
        continue;
      }
      // While inserting into buffer, we will check for count threshold and buffered bytes
      // threshold.
      insert(record);
    }
    // check all partitions to see if they need to be flushed based on time
    for (TopicPartitionChannel partitionChannel : partitionsToChannel.values()) {
      // Time based flushing
      partitionChannel.insertBufferedRecordsIfFlushTimeThresholdReached();
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
    // init a new topic partition if it not present in cache or if channel is closed
    if (!partitionsToChannel.containsKey(partitionChannelKey)
        || partitionsToChannel.get(partitionChannelKey).isChannelClosed()) {
      LOGGER.warn(
          "Topic: {} Partition: {} hasn't been initialized by OPEN function",
          record.topic(),
          record.kafkaPartition());
      startTask(
          Utils.tableName(record.topic(), this.topicToTableMap),
          new TopicPartition(record.topic(), record.kafkaPartition()));
    }

    TopicPartitionChannel channelPartition = partitionsToChannel.get(partitionChannelKey);
    channelPartition.insertRecordToBuffer(record);
  }

  @Override
  public long getOffset(TopicPartition topicPartition) {
    String partitionChannelKey =
        partitionChannelKey(topicPartition.topic(), topicPartition.partition());
    if (partitionsToChannel.containsKey(partitionChannelKey)) {
      return partitionsToChannel.get(partitionChannelKey).getOffsetSafeToCommitToKafka();
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
    partitions.forEach(
        topicPartition -> {
          final String partitionChannelKey =
              partitionChannelKey(topicPartition.topic(), topicPartition.partition());
          TopicPartitionChannel topicPartitionChannel =
              partitionsToChannel.get(partitionChannelKey);
          topicPartitionChannel.closeChannel();
          LOGGER.info(
              "Closing partitionChannel:{}, partition:{}, topic:{}",
              topicPartitionChannel.getChannelName(),
              topicPartition.topic(),
              topicPartition.partition());
        });
    partitionsToChannel.clear();
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
      this.recordNum = STREAMING_BUFFER_COUNT_RECORDS_DEFAULT;
    } else {
      this.recordNum = num;
      LOGGER.info("Set number of records for buffer threshold to {}", num);
    }
  }

  /**
   * Assume this is buffer size in bytes, since this is streaming ingestion
   *
   * @param size in bytes - a non negative long number representing size of internal buffer for
   *     flush.
   */
  @Override
  public void setFileSize(long size) {
    if (size < SnowflakeSinkConnectorConfig.BUFFER_SIZE_BYTES_MIN) {
      LOGGER.error(
          "Buffer size is {} bytes, it is smaller than the minimum buffer "
              + "size {} bytes, reset to the default buffer size",
          size,
          BUFFER_SIZE_BYTES_DEFAULT);
      this.fileSizeBytes = BUFFER_SIZE_BYTES_DEFAULT;
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
    if (time < StreamingUtils.STREAMING_BUFFER_FLUSH_TIME_MINIMUM_SEC) {
      LOGGER.error(
          "flush time is {} seconds, it is smaller than the minimum "
              + "flush time {} seconds, reset to the default flush time",
          time,
          STREAMING_BUFFER_FLUSH_TIME_DEFAULT_SEC);
      this.flushTimeSeconds = STREAMING_BUFFER_FLUSH_TIME_DEFAULT_SEC;
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

  /**
   * This is more of size in bytes of buffered records. This necessarily doesnt translates to files
   * created by Streaming Ingest since they are compressed. So there is no 1:1 mapping.
   */
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
    assert ingestionDeliveryGuarantee
        == SnowflakeSinkConnectorConfig.IngestionDeliveryGuarantee.EXACTLY_ONCE;
    this.ingestionDeliveryGuarantee = ingestionDeliveryGuarantee;
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

  /* Used for testing */
  @VisibleForTesting
  SnowflakeStreamingIngestClient getStreamingIngestClient() {
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

  /* Init Streaming client. If is also used to re-init the client if client was closed before. */
  private void initStreamingClient() {
    Map<String, String> streamingPropertiesMap =
        StreamingUtils.convertConfigForStreamingClient(new HashMap<>(this.connectorConfig));
    Properties streamingClientProps = new Properties();
    streamingClientProps.putAll(streamingPropertiesMap);
    if (this.streamingIngestClient == null || this.streamingIngestClient.isClosed()) {
      try {
        LOGGER.info("Initializing Streaming Client. ClientName:{}", this.streamingIngestClientName);
        this.streamingIngestClient =
            SnowflakeStreamingIngestClientFactory.builder(this.streamingIngestClientName)
                .setProperties(streamingClientProps)
                .build();
      } catch (SFException ex) {
        LOGGER.error(
            "Exception creating streamingIngestClient with name:{}",
            this.streamingIngestClientName);
        throw new ConnectException(ex);
      }
    }
  }

  /** Closes the streaming client. */
  private void closeStreamingClient() {
    LOGGER.info("Closing Streaming Client:{}", this.streamingIngestClientName);
    try {
      streamingIngestClient.close();
    } catch (Exception e) {
      LOGGER.error(
          Logging.logMessage(
              "Failure closing Streaming client msg:{}, cause:{}",
              e.getMessage(),
              Arrays.toString(e.getCause().getStackTrace())));
    }
  }

  private void createTableIfNotExists(final String tableName) {
    if (this.conn.tableExist(tableName)) {
      if (!this.enableSchematization) {
        if (this.conn.isTableCompatible(tableName)) {
          LOGGER.info("Using existing table {}.", tableName);
        } else {
          throw SnowflakeErrors.ERROR_5003.getException("table name: " + tableName);
        }
      } else {
        this.conn.appendMetaColIfNotExist(tableName);
      }
    } else {
      LOGGER.info("Creating new table {}.", tableName);
      if (connectorConfig.containsKey("value.converter")
          && connectorConfig
              .get("value.converter")
              .equals("io.confluent.connect.avro.AvroConverter")
          && this.enableSchematization) {
        Map<String, String> fields = GetSchema(tableName);
        this.conn.createTableWithSchema(tableName, fields);
      } else {
        this.conn.createTable(tableName);
      }
    }
  }

  private Map<String, String> GetSchema(final String tableName) {
    Map<String, String> srConfig = new HashMap<>();
    srConfig.put("schema.registry.url", connectorConfig.get("value.converter.schema.registry.url"));
    AvroConverterConfig avroConverterConfig = new AvroConverterConfig(srConfig);
    SchemaRegistryClient schemaRegistry =
        new CachedSchemaRegistryClient(
            avroConverterConfig.getSchemaRegistryUrls(),
            avroConverterConfig.getMaxSchemasPerSubject(),
            Collections.singletonList(new AvroSchemaProvider()),
            srConfig,
            avroConverterConfig.requestHeaders());

    Map<String, String> schemaMap = new HashMap<>();
    for (Map.Entry<String, String> entry : topicToTableMap.entrySet()) {
      if (entry.getValue().equals(tableName)) {
        String topicName = entry.getKey();
        String subjectName = topicName + "-value";
        SchemaMetadata schemaMeta = null;
        try {
          schemaMeta = schemaRegistry.getLatestSchemaMetadata(subjectName);
        } catch (Exception e) {
          LOGGER.error(Logging.logMessage("Failure getting latest schema"));
        }
        if (schemaMeta != null) {
          AvroSchema schema = new AvroSchema(schemaMeta.getSchema());
          for (Schema.Field field : schema.rawSchema().getFields()) {
            Schema fieldSchema = field.schema();
            if (!schemaMap.containsKey(field.name())) {
              switch (fieldSchema.getType()) {
                case BOOLEAN:
                  schemaMap.put(field.name(), "boolean");
                  break;
                case BYTES:
                  schemaMap.put(field.name(), "binary");
                  break;
                case DOUBLE:
                  schemaMap.put(field.name(), "double");
                  break;
                case FLOAT:
                  schemaMap.put(field.name(), "float");
                  break;
                case INT:
                  schemaMap.put(field.name(), "int");
                  break;
                case LONG:
                  schemaMap.put(field.name(), "number");
                  break;
                case STRING:
                  schemaMap.put(field.name(), "string");
                  break;
                default:
                  schemaMap.put(field.name(), "variant");
              }
            }
          }
        }
      }
    }
    return schemaMap;
  }
}
