package com.snowflake.kafka.connector.records;

import static com.snowflake.kafka.connector.Utils.TABLE_COLUMN_METADATA;

import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;

/**
 * A lightweight wrapper for Kafka SinkRecords that stores data in the format required by the
 * Snowflake Streaming Ingest SDK (Map<String, Object>).
 */
public final class SnowflakeSinkRecord {

  static final String OFFSET = "offset";

  static final String TOPIC = "topic";
  static final String PARTITION = "partition";
  static final String KEY = "key";
  static final String CONNECTOR_PUSH_TIME = "SnowflakeConnectorPushTime";
  static final String HEADERS = "headers";

  private final Map<String, Object> content;
  private final Map<String, Object> metadata;
  private final RecordState state;

  public enum RecordState {
    VALID,
    TOMBSTONE,
    BROKEN
  }

  private SnowflakeSinkRecord(
      Map<String, Object> content, Map<String, Object> metadata, RecordState state) {
    this.content = content;
    this.metadata = metadata;
    this.state = state;
  }

  public static SnowflakeSinkRecord from(
      SinkRecord record, SnowflakeMetadataConfig metadataConfig) {
    return from(record, metadataConfig, Instant.now());
  }

  public static SnowflakeSinkRecord from(
      SinkRecord record, SnowflakeMetadataConfig metadataConfig, Instant connectorPushTime) {

    // First validate the key if present - a broken key means a broken record
    if (record.key() != null && record.keySchema() != null) {
      try {
        KafkaRecordConverter.convertKey(record.keySchema(), record.key());
      } catch (Exception e) {
        return createBrokenRecord(record, metadataConfig, connectorPushTime);
      }
    }

    // Handle null/tombstone records
    if (record.value() == null) {
      return createTombstoneRecord(record, metadataConfig, connectorPushTime);
    }

    try {
      Map<String, Object> content =
          KafkaRecordConverter.convertToMap(record.valueSchema(), record.value());

      Map<String, Object> metadata = buildMetadata(record, metadataConfig, connectorPushTime);

      return new SnowflakeSinkRecord(content, metadata, RecordState.VALID);
    } catch (Exception e) {
      return createBrokenRecord(record, metadataConfig, connectorPushTime);
    }
  }

  private static SnowflakeSinkRecord createTombstoneRecord(
      SinkRecord record, SnowflakeMetadataConfig metadataConfig, Instant connectorPushTime) {
    Map<String, Object> metadata = buildMetadata(record, metadataConfig, connectorPushTime);
    return new SnowflakeSinkRecord(Collections.emptyMap(), metadata, RecordState.TOMBSTONE);
  }

  private static SnowflakeSinkRecord createBrokenRecord(
      SinkRecord record, SnowflakeMetadataConfig metadataConfig, Instant connectorPushTime) {
    Map<String, Object> metadata = buildMetadataSafe(record, metadataConfig, connectorPushTime);
    return new SnowflakeSinkRecord(Collections.emptyMap(), metadata, RecordState.BROKEN);
  }

  private static Map<String, Object> buildMetadataSafe(
      SinkRecord record, SnowflakeMetadataConfig metadataConfig, Instant connectorPushTime) {
    final Map<String, Object> metadata =
        buildMetadataBase(record, metadataConfig, connectorPushTime);

    // For broken records, store key as string if conversion fails
    if (record.key() != null) {
      try {
        Object convertedKey = KafkaRecordConverter.convertKey(record.keySchema(), record.key());
        metadata.put(KEY, convertedKey);
      } catch (Exception e) {
        metadata.put(KEY, String.valueOf(record.key()));
      }
    }

    // Add headers (these should be safe to convert)
    if (record.headers() != null && !record.headers().isEmpty()) {
      try {
        metadata.put(HEADERS, KafkaRecordConverter.convertHeaders(record.headers()));
      } catch (Exception e) {
        // Skip headers if conversion fails
      }
    }

    return metadata;
  }

  private static Map<String, Object> buildMetadata(
      SinkRecord record, SnowflakeMetadataConfig metadataConfig, Instant connectorPushTime) {
    final Map<String, Object> metadata =
        buildMetadataBase(record, metadataConfig, connectorPushTime);

    // Add key to metadata
    addKeyToMetadata(record, metadata);

    // Add headers
    if (record.headers() != null && !record.headers().isEmpty()) {
      metadata.put(HEADERS, KafkaRecordConverter.convertHeaders(record.headers()));
    }

    return metadata;
  }

  private static Map<String, Object> buildMetadataBase(
      SinkRecord record, SnowflakeMetadataConfig metadataConfig, Instant connectorPushTime) {
    final Map<String, Object> metadata = new HashMap<>();

    if (metadataConfig.topicFlag) {
      metadata.put(TOPIC, record.topic());
    }

    if (metadataConfig.offsetAndPartitionFlag) {
      metadata.put(OFFSET, record.kafkaOffset());
      metadata.put(PARTITION, record.kafkaPartition());
    }

    if (record.timestampType() != TimestampType.NO_TIMESTAMP_TYPE
        && metadataConfig.createtimeFlag) {
      metadata.put(record.timestampType().name, record.timestamp());
    }

    if (connectorPushTime != null && metadataConfig.connectorPushTimeFlag) {
      metadata.put(CONNECTOR_PUSH_TIME, connectorPushTime.toEpochMilli());
    }

    return metadata;
  }

  private static void addKeyToMetadata(SinkRecord record, Map<String, Object> metadata) {
    if (record.key() == null) {
      return;
    }

    Schema keySchema = record.keySchema();
    Object key = record.key();

    try {
      // Always use convertKey to ensure type validation when schema is present
      Object convertedKey = KafkaRecordConverter.convertKey(keySchema, key);
      metadata.put(KEY, convertedKey);
    } catch (Exception e) {
      // If key conversion fails, store the key as a string representation
      metadata.put(KEY, String.valueOf(key));
    }
  }

  public Map<String, Object> getContent() {
    return content;
  }

  public Map<String, Object> getContentWithMetadata(boolean includeMetadata) {
    if (!includeMetadata || metadata.isEmpty()) {
      return content;
    }

    Map<String, Object> result = new HashMap<>(content);
    result.put(TABLE_COLUMN_METADATA, metadata);
    return result;
  }

  public Map<String, Object> getMetadata() {
    return metadata;
  }

  public RecordState getState() {
    return state;
  }

  public boolean isValid() {
    return state == RecordState.VALID;
  }

  public boolean isTombstone() {
    return state == RecordState.TOMBSTONE;
  }

  public boolean isBroken() {
    return state == RecordState.BROKEN;
  }
}
