package com.snowflake.kafka.connector.internal.streaming;

import static org.apache.kafka.common.record.TimestampType.NO_TIMESTAMP_TYPE;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableMap;
import com.snowflake.kafka.connector.dlq.KafkaRecordErrorReporter;
import com.snowflake.kafka.connector.internal.KCLogger;
import com.snowflake.kafka.connector.internal.SnowflakeErrors;
import com.snowflake.kafka.connector.internal.SnowflakeKafkaConnectorException;
import com.snowflake.kafka.connector.records.RecordService;
import com.snowflake.kafka.connector.records.SnowflakeJsonSchema;
import com.snowflake.kafka.connector.records.SnowflakeRecordContent;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;

/** Service to transform data from Kafka format into a map that is accepted by ingest sdk. */
public class StreamingRecordService {
  private static final KCLogger LOGGER = new KCLogger(StreamingRecordService.class.getName());

  private final RecordService recordService;
  private final KafkaRecordErrorReporter kafkaRecordErrorReporter;

  public StreamingRecordService(
      RecordService recordService, KafkaRecordErrorReporter kafkaRecordErrorReporter) {
    this.recordService = recordService;
    this.kafkaRecordErrorReporter = kafkaRecordErrorReporter;
  }

  /**
   * @param kafkaSinkRecord a record in Kafka format
   * @return a map that format depends on the schematization settings
   */
  public Map<String, Object> transformData(SinkRecord kafkaSinkRecord) {
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

  /**
   * Converts the original kafka sink record into a Json Record. i.e key and values are converted
   * into Json so that it can be used to insert into variant column of Snowflake Table.
   *
   * <p>TODO: SNOW-630885 - When schematization is enabled, we should create the map directly from
   * the SinkRecord instead of first turning it into json
   */
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

  /**
   * This would always return false for streaming ingest use case since isBroken field is never set.
   * isBroken is set only when using Custom snowflake converters and the content was not json
   * serializable.
   *
   * <p>For Community converters, the kafka record will not be sent to Kafka connector if the record
   * is not serializable.
   */
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
