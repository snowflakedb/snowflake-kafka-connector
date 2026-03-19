package com.snowflake.kafka.connector.records;

import static com.snowflake.kafka.connector.Utils.TABLE_COLUMN_METADATA;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.internal.validation.SqlIdentifierNormalizer;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;

/**
 * A lightweight wrapper for Kafka SinkRecords that stores data in the format required by the
 * Snowflake Streaming Ingest SDK ({@code Map<String, Object>}).
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
  private final Schema schema;
  private final RecordState state;
  private final Exception brokenReason;

  public enum RecordState {
    VALID,
    TOMBSTONE,
    BROKEN
  }

  private SnowflakeSinkRecord(
      Map<String, Object> content,
      Map<String, Object> metadata,
      Schema schema,
      RecordState state,
      Exception brokenReason) {
    this.content = content;
    this.metadata = metadata;
    this.schema = schema;
    this.state = state;
    this.brokenReason = brokenReason;
  }

  public static SnowflakeSinkRecord from(
      SinkRecord record,
      SnowflakeMetadataConfig metadataConfig,
      boolean enableSchematization,
      boolean enableColumnIdentifierNormalization) {
    return from(
        record,
        metadataConfig,
        Instant.now(),
        enableSchematization,
        enableColumnIdentifierNormalization);
  }

  public static SnowflakeSinkRecord from(
      SinkRecord record,
      SnowflakeMetadataConfig metadataConfig,
      Instant connectorPushTime,
      boolean enableSchematization,
      boolean enableColumnIdentifierNormalization) {
    // First validate the key if present - a broken key means a broken record
    if (record.key() != null && record.keySchema() != null) {
      try {
        KafkaRecordConverter.convertKey(record.keySchema(), record.key());
      } catch (Exception e) {
        return createBrokenRecord(record, metadataConfig, connectorPushTime, e);
      }
    }

    if (record.value() == null) {
      return createTombstoneRecord(record, metadataConfig, connectorPushTime);
    }

    try {
      Map<String, Object> content;
      Schema schema = record.valueSchema();
      if (enableSchematization) {
        content = KafkaRecordConverter.convertToMap(schema, record.value());
        if (enableColumnIdentifierNormalization) {
          content = normalizeColumnNames(content);
          schema = normalizeSchemaFieldNames(schema);
        }
      } else {
        content = wrapAsRecordContent(schema, record.value());
      }
      Map<String, Object> metadata = buildMetadata(record, metadataConfig, connectorPushTime);
      return new SnowflakeSinkRecord(content, metadata, schema, RecordState.VALID, null);
    } catch (Exception e) {
      return createBrokenRecord(record, metadataConfig, connectorPushTime, e);
    }
  }

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static Map<String, Object> wrapAsRecordContent(Schema schema, Object value) {
    Map<String, Object> content = new HashMap<>();
    Object convertedValue;
    if (value instanceof Map || value instanceof Struct) {
      // Store as Map so the SDK infers VARIANT (not VARCHAR from a JSON string)
      convertedValue = KafkaRecordConverter.convertToMap(schema, value);
    } else {
      // For primitive values (strings, bytes, etc.), JSON-serialize so the SDK
      // can insert them as valid JSON into VARIANT columns.
      try {
        convertedValue =
            MAPPER.writeValueAsString(KafkaRecordConverter.convertValue(schema, value));
      } catch (JsonProcessingException e) {
        throw new RuntimeException("Failed to serialize record content", e);
      }
    }
    content.put(Utils.TABLE_COLUMN_CONTENT, convertedValue);
    return content;
  }

  private static SnowflakeSinkRecord createTombstoneRecord(
      SinkRecord record, SnowflakeMetadataConfig metadataConfig, Instant connectorPushTime) {
    Map<String, Object> metadata = buildMetadata(record, metadataConfig, connectorPushTime);
    return new SnowflakeSinkRecord(
        Collections.emptyMap(), metadata, record.valueSchema(), RecordState.TOMBSTONE, null);
  }

  private static SnowflakeSinkRecord createBrokenRecord(
      SinkRecord record,
      SnowflakeMetadataConfig metadataConfig,
      Instant connectorPushTime,
      Exception reason) {
    Map<String, Object> metadata = buildMetadataSafe(record, metadataConfig, connectorPushTime);
    return new SnowflakeSinkRecord(
        Collections.emptyMap(), metadata, record.valueSchema(), RecordState.BROKEN, reason);
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

  public Schema getSchema() {
    return schema;
  }

  private static Schema normalizeSchemaFieldNames(Schema schema) {
    if (schema == null || schema.type() != Schema.Type.STRUCT) {
      return schema;
    }
    SchemaBuilder builder = SchemaBuilder.struct();
    if (schema.name() != null) {
      builder.name(schema.name());
    }
    if (schema.isOptional()) {
      builder.optional();
    }
    for (Field field : schema.fields()) {
      String normalizedName = SqlIdentifierNormalizer.normalizeSqlIdentifier(field.name());
      builder.field(normalizedName, field.schema());
    }
    return builder.build();
  }

  private static Map<String, Object> normalizeColumnNames(Map<String, Object> content) {
    Map<String, Object> normalized = new HashMap<>(content.size());
    for (Map.Entry<String, Object> entry : content.entrySet()) {
      normalized.put(
          SqlIdentifierNormalizer.normalizeSqlIdentifier(entry.getKey()), entry.getValue());
    }
    return normalized;
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

  public Exception getBrokenReason() {
    return brokenReason;
  }
}
