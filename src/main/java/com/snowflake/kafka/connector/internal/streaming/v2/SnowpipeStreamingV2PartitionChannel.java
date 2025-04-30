package com.snowflake.kafka.connector.internal.streaming.v2;

import static org.apache.kafka.common.record.TimestampType.NO_TIMESTAMP_TYPE;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableMap;
import com.snowflake.ingest.streaming.OpenChannelResult;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestClientFactory;
import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.dlq.KafkaRecordErrorReporter;
import com.snowflake.kafka.connector.internal.KCLogger;
import com.snowflake.kafka.connector.internal.SnowflakeConnectionService;
import com.snowflake.kafka.connector.internal.SnowflakeErrors;
import com.snowflake.kafka.connector.internal.SnowflakeKafkaConnectorException;
import com.snowflake.kafka.connector.internal.streaming.StreamingClientProperties;
import com.snowflake.kafka.connector.internal.streaming.channel.TopicPartitionChannel;
import com.snowflake.kafka.connector.internal.streaming.telemetry.SnowflakeTelemetryChannelStatus;
import com.snowflake.kafka.connector.records.RecordService;
import com.snowflake.kafka.connector.records.SnowflakeJsonSchema;
import com.snowflake.kafka.connector.records.SnowflakeRecordContent;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;

public class SnowpipeStreamingV2PartitionChannel implements TopicPartitionChannel {

  private static final KCLogger LOGGER =
      new KCLogger(SnowpipeStreamingV2PartitionChannel.class.getName());

  private static final String STREAMING_CLIENT_V2_PREFIX_NAME = "KC_CLIENT_V2_";
  private static final String DEFAULT_CLIENT_NAME = "DEFAULT_CLIENT";

  private final AtomicLong processedOffset =
      new AtomicLong(NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE);

  private static final AtomicInteger createdClientId = new AtomicInteger(0);

  private final SnowflakeConnectionService connectionService;
  private final SnowflakeStreamingIngestClient streamingIngestClient;
  private final SnowflakeStreamingIngestChannel channel;
  private final RecordService recordService;
  private final KafkaRecordErrorReporter kafkaRecordErrorReporter;

  public SnowpipeStreamingV2PartitionChannel(
      String tableName,
      String channelName,
      SnowflakeConnectionService connectionService,
      Map<String, String> connectorConfig,
      RecordService recordService,
      KafkaRecordErrorReporter kafkaRecordErrorReporter) {
    this.connectionService = connectionService;
    this.recordService = recordService;
    this.kafkaRecordErrorReporter = kafkaRecordErrorReporter;

    String clientName = clientName(connectorConfig);
    String dbName = Utils.database(connectorConfig);
    String schemaName = Utils.schema(connectorConfig);

    String pipeName = PipeNameProvider.pipeName(connectorConfig.get(Utils.NAME), tableName);

    connectionService.createPipeForSSv2(tableName, pipeName);

    StreamingClientProperties streamingClientProperties =
        new StreamingClientProperties(connectorConfig);

    // TODO - single client optimization?
    this.streamingIngestClient =
        SnowflakeStreamingIngestClientFactory.builder(clientName, dbName, schemaName, pipeName)
            .setProperties(StreamingClientPropertiesMapper.getClientProperties(connectorConfig))
            .setParameterOverrides(streamingClientProperties.parameterOverrides)
            .build();

    OpenChannelResult result = streamingIngestClient.openChannel(channelName, null);
    if (result.getChannelStatus().getStatusCode().equals("SUCCESS")) {
      this.channel = result.getChannel();
    } else {
      throw new RuntimeException(
          "Got openChannel() code=" + result.getChannelStatus().getStatusCode());
    }
  }

  @Override
  public void insertRecord(SinkRecord kafkaSinkRecord, boolean isFirstRowPerPartitionInBatch) {
    transformAndSend(kafkaSinkRecord);
  }

  @Override
  public long getOffsetSafeToCommitToKafka() {
    return 0;
  }

  @Override
  public void closeChannel() {
    // TODO - consider using drop
    streamingIngestClient.close();
  }

  @Override
  public CompletableFuture<Void> closeChannelAsync() {
    // TODO - consider using drop
    throw new UnsupportedOperationException(
        "Snowpipe Streaming v2 channels cannot be closed asynchronously");
  }

  @Override
  public boolean isChannelClosed() {
    return channel.isClosed();
  }

  @Override
  public String getChannelNameFormatV1() {
    return channel.getFullyQualifiedChannelName();
  }

  @Override
  public void setLatestConsumerGroupOffset(long consumerOffset) {}

  @Override
  public long getOffsetPersistedInSnowflake() {
    return 0;
  }

  @Override
  public long getProcessedOffset() {
    // TODO take care of offset handling
    return processedOffset.get();
  }

  @Override
  public long getLatestConsumerOffset() {
    return 0;
  }

  @Override
  public SnowflakeTelemetryChannelStatus getSnowflakeTelemetryChannelStatus() {
    return null;
  }

  @Override
  public long fetchOffsetTokenWithRetry() {
    return 0;
  }

  private String clientName(Map<String, String> connectorConfig) {
    return STREAMING_CLIENT_V2_PREFIX_NAME
        + connectorConfig.getOrDefault(Utils.NAME, DEFAULT_CLIENT_NAME)
        + createdClientId.getAndIncrement();
  }

  // TODO - look for the common parts with DirectTopicPartitionChannel
  private void transformAndSend(SinkRecord kafkaSinkRecord) {
    Map<String, Object> transformedRecord = transformDataBeforeSending(kafkaSinkRecord);
    if (!transformedRecord.isEmpty()) {
      // TODO - currently returns empty object
      channel.appendRow(transformedRecord, String.valueOf(kafkaSinkRecord.kafkaOffset()));
    }
    this.processedOffset.set(kafkaSinkRecord.kafkaOffset());
  }

  private Map<String, Object> transformDataBeforeSending(SinkRecord kafkaSinkRecord) {
    SinkRecord snowflakeSinkRecord = getSnowflakeSinkRecordFromKafkaRecord(kafkaSinkRecord);
    // broken record
    if (isRecordBroken(snowflakeSinkRecord)) {
      // check for error tolerance and log tolerance values
      // errors.log.enable and errors.tolerance
      LOGGER.debug(
          "Broken record offset:{}, topic:{}",
          kafkaSinkRecord.kafkaOffset(),
          kafkaSinkRecord.topic());
      kafkaRecordErrorReporter.reportError(kafkaSinkRecord, new DataException("Broken Record"));
    } else {
      // lag telemetry, note that sink record timestamp might be null
      if (kafkaSinkRecord.timestamp() != null
          && kafkaSinkRecord.timestampType() != NO_TIMESTAMP_TYPE) {
        // TODO:SNOW-529751 telemetry
      }

      // Convert this records into Json Schema which has content and metadata, add it to DLQ if
      // there is an exception
      try {
        return recordService.getProcessedRecordForStreamingIngest(snowflakeSinkRecord);
      } catch (JsonProcessingException e) {
        LOGGER.warn(
            "Record has JsonProcessingException offset:{}, topic:{}",
            kafkaSinkRecord.kafkaOffset(),
            kafkaSinkRecord.topic());
        kafkaRecordErrorReporter.reportError(kafkaSinkRecord, e);
      } catch (SnowflakeKafkaConnectorException e) {
        if (e.checkErrorCode(SnowflakeErrors.ERROR_0010)) {
          LOGGER.warn(
              "Cannot parse record offset:{}, topic:{}. Sending to DLQ.",
              kafkaSinkRecord.kafkaOffset(),
              kafkaSinkRecord.topic());
          kafkaRecordErrorReporter.reportError(kafkaSinkRecord, e);
        } else {
          throw e;
        }
      }
    }

    // return empty
    return ImmutableMap.of();
  }

  private SinkRecord getSnowflakeSinkRecordFromKafkaRecord(final SinkRecord kafkaSinkRecord) {
    SinkRecord snowflakeRecord = kafkaSinkRecord;
    if (shouldConvertContent(kafkaSinkRecord.value())) {
      snowflakeRecord = handleNativeRecord(kafkaSinkRecord, false);
    }
    if (shouldConvertContent(kafkaSinkRecord.key())) {
      snowflakeRecord = handleNativeRecord(snowflakeRecord, true);
    }

    return snowflakeRecord;
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
      newSFContent = new SnowflakeRecordContent(schema, content, true);
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
}
