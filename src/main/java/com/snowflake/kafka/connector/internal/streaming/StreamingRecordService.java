package com.snowflake.kafka.connector.internal.streaming;

import com.snowflake.kafka.connector.dlq.KafkaRecordErrorReporter;
import com.snowflake.kafka.connector.internal.KCLogger;
import com.snowflake.kafka.connector.records.SnowflakeMetadataConfig;
import com.snowflake.kafka.connector.records.SnowflakeSinkRecord;
import java.util.Map;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;

/**
 * Service to transform data from Kafka format into a map that is accepted by the Snowflake
 * Streaming Ingest SDK.
 */
public class StreamingRecordService {

  private static final KCLogger LOGGER = new KCLogger(StreamingRecordService.class.getName());

  private final KafkaRecordErrorReporter kafkaRecordErrorReporter;
  private final SnowflakeMetadataConfig metadataConfig;

  public StreamingRecordService(
      KafkaRecordErrorReporter kafkaRecordErrorReporter, SnowflakeMetadataConfig metadataConfig) {
    this.kafkaRecordErrorReporter = kafkaRecordErrorReporter;
    this.metadataConfig = metadataConfig;
  }

  public Map<String, Object> transformData(SinkRecord kafkaSinkRecord) {
    SnowflakeSinkRecord record = SnowflakeSinkRecord.from(kafkaSinkRecord, metadataConfig);

    if (record.isBroken()) {
      LOGGER.debug(
          "Broken record offset:{}, topic:{}",
          kafkaSinkRecord.kafkaOffset(),
          kafkaSinkRecord.topic());
      kafkaRecordErrorReporter.reportError(kafkaSinkRecord, new DataException("Broken Record"));
      return Map.of();
    }

    // Tombstone records are handled by the caller (shouldSkipNullValue check)
    // If we reach here, it means we should ingest an empty record
    return record.getContentWithMetadata(metadataConfig.shouldIncludeAllMetadata());
  }
}
