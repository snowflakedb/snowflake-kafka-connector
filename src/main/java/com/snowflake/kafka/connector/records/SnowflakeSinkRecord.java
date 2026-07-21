package com.snowflake.kafka.connector.records;

import static com.snowflake.kafka.connector.Utils.TABLE_COLUMN_METADATA;

import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.internal.validation.SqlIdentifierNormalizer;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
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

  // The managed-Iceberg metadata schema declares BOTH timestamp fields. A Kafka record carries
  // exactly one timestamp type per topic, so at most one of the two is ever populated for a given
  // record; the other is absent from the raw metadata map, so conformIcebergMetadata pads it with
  // null (the v2 structured-OBJECT cast rejects a missing declared field).
  static final String CREATE_TIME = "CreateTime";
  static final String LOG_APPEND_TIME = "LogAppendTime";

  // This list must stay identical to IcebergDDLTypes.ICEBERG_METADATA_OBJECT_SCHEMA.
  // The managed-Iceberg v2 structured-OBJECT cast (used by conformIcebergMetadata) REJECTS any
  // declared field that is absent in the map, so (a) the list here must mirror the DDL exactly
  // and (b) conformIcebergMetadata pads every absent-able field with null before ingestion.
  // Enforced by SnowflakeSinkRecordTest#testIcebergMetadataFields_matchDdlSchema.
  public static final List<String> ICEBERG_METADATA_FIELDS =
      List.of(
          OFFSET,
          TOPIC,
          PARTITION,
          KEY,
          CREATE_TIME,
          LOG_APPEND_TIME,
          CONNECTOR_PUSH_TIME,
          HEADERS);

  // Fast membership test for conformIcebergMetadata. Derived from ICEBERG_METADATA_FIELDS so the
  // two stay in sync automatically.
  private static final java.util.Set<String> ICEBERG_METADATA_FIELD_SET =
      new java.util.HashSet<>(ICEBERG_METADATA_FIELDS);

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
        content = wrapValueAsRecordContent(schema, record.value());
        schema = RECORD_CONTENT_WRAPPER_SCHEMA;
      }
      Map<String, Object> metadata = buildMetadata(record, metadataConfig, connectorPushTime);
      return new SnowflakeSinkRecord(content, metadata, schema, RecordState.VALID, null);
    } catch (Exception e) {
      return createBrokenRecord(record, metadataConfig, connectorPushTime, e);
    }
  }

  /**
   * Wraps the record value under the {@code RECORD_CONTENT} key.
   *
   * <p>For structured types (Map/Struct) the value is converted to a Map so the SDK infers VARIANT.
   *
   * <p>For primitive types the converted value is placed directly into the map. The SSv2 SDK
   * serializes the map to NDJSON via Jackson, which handles native Java types (String, Number,
   * Boolean) correctly for VARIANT columns. Unlike KCv3/SSv1 (which required JSON-serialized
   * strings because SSv1 re-parsed them via {@code readTree}), SSv2 passes NDJSON straight to the
   * server — so JSON-serializing here would produce double-quoted strings.
   */
  private static Map<String, Object> wrapValueAsRecordContent(Schema schema, Object value) {
    Map<String, Object> content = new HashMap<>();
    Object convertedValue;
    if (value instanceof Map || value instanceof Struct) {
      convertedValue = KafkaRecordConverter.convertToMap(schema, value);
    } else {
      convertedValue = KafkaRecordConverter.convertValue(schema, value);
    }
    content.put(Utils.TABLE_COLUMN_CONTENT, convertedValue);
    return content;
  }

  /**
   * Builds a synthetic Struct schema declaring {@code RECORD_CONTENT} as STRUCT (→ VARIANT).
   *
   * <p>Assumptions:
   *
   * <ul>
   *   <li>RECORD_CONTENT is always a VARIANT column in Snowflake, regardless of the Kafka value
   *       type. Even bare strings (from StringConverter) must land as VARIANT, not VARCHAR.
   *   <li>STRUCT is used because {@link
   *       com.snowflake.kafka.connector.internal.schemaevolution.SnowflakeColumnTypeMapper} maps
   *       STRUCT to "VARIANT". If schema evolution needs to ADD this column, it must infer VARIANT.
   *   <li>This only applies to standard Snowflake tables. Iceberg tables with typed RECORD_CONTENT
   *       columns would need a different schema strategy.
   * </ul>
   */
  private static final Schema RECORD_CONTENT_WRAPPER_SCHEMA =
      SchemaBuilder.struct()
          .field(Utils.TABLE_COLUMN_CONTENT, SchemaBuilder.struct().optional().build())
          .build();

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
        metadata.put(
            HEADERS,
            KafkaRecordConverter.convertHeaders(
                record.headers(), metadataConfig.structuredHeadersFlag));
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
      metadata.put(
          HEADERS,
          KafkaRecordConverter.convertHeaders(
              record.headers(), metadataConfig.structuredHeadersFlag));
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

  /**
   * @param conformToStructuredObjectSchema when true, the RECORD_METADATA value is normalized via
   *     {@link #conformIcebergMetadata}: fields outside {@code ICEBERG_METADATA_OBJECT_SCHEMA}
   *     throw, and the {@code key} field is coerced to {@code String}. Full metadata presence is
   *     guaranteed by config-time validation (see {@code SinkTaskConfig}). FDN tables use a VARIANT
   *     RECORD_METADATA and pass {@code false} (behavior unchanged).
   */
  public Map<String, Object> getContentWithMetadata(
      boolean includeMetadata, boolean conformToStructuredObjectSchema) {
    if (!includeMetadata || metadata.isEmpty()) {
      return content;
    }

    Map<String, Object> result = new HashMap<>(content);
    result.put(
        TABLE_COLUMN_METADATA,
        conformToStructuredObjectSchema ? conformIcebergMetadata(metadata) : metadata);
    return result;
  }

  /**
   * Normalize a Kafka metadata map for ingestion into a managed-Iceberg {@code RECORD_METADATA}
   * OBJECT column.
   *
   * <p><b>This method is called only for managed-Iceberg v2 tables.</b> v3 and FDN tables use a
   * VARIANT {@code RECORD_METADATA} column and never call this path. The v2 structured-OBJECT cast
   * is <em>strict</em>: it rejects any declared OBJECT sub-field that is absent in the map with a
   * {@code Typed object schema mismatch in conversion} error, so every field declared in {@code
   * ICEBERG_METADATA_OBJECT_SCHEMA} must be present in the map (present-and-null is fine; absent is
   * rejected).
   *
   * <p>This method:
   *
   * <ol>
   *   <li>Throws {@link IllegalStateException} if it encounters a field outside the Iceberg schema.
   *       {@code buildMetadata} only ever emits schema fields, so this never fires in normal
   *       operation — it fires only if KC metadata emission and the Iceberg schema drift apart (a
   *       new field added on one side but not the other), which we want to surface loudly rather
   *       than silently drop.
   *   <li>Coerces the {@code key} field to {@code String}: the schema declares {@code key STRING},
   *       but non-String-keyed topics (e.g. INT-keyed) yield a non-String key value.
   *   <li>Pads every absent-able field with {@code null}: a Kafka record carries exactly one
   *       timestamp type (so the other is absent), and {@code key}/{@code headers} are absent on
   *       keyless/headerless records. These absent fields are padded here so the strict v2
   *       typed-OBJECT cast does not reject the row. Config-gated fields (topic, offset, partition,
   *       connector-push-time) are guaranteed present by config-time validation and are not padded.
   * </ol>
   *
   * <p>Timestamps pass through under their own name ({@code CreateTime} or {@code LogAppendTime}).
   */
  // Package-private for testing.
  static Map<String, Object> conformIcebergMetadata(Map<String, Object> metadata) {
    Map<String, Object> conformed = new HashMap<>();
    for (Map.Entry<String, Object> entry : metadata.entrySet()) {
      String field = entry.getKey();
      if (!ICEBERG_METADATA_FIELD_SET.contains(field)) {
        throw new IllegalStateException(
            "Unexpected metadata field '"
                + field
                + "' not in the managed-Iceberg RECORD_METADATA schema (ICEBERG_METADATA_FIELDS)."
                + " KC metadata emission and the Iceberg schema have drifted: add the field to"
                + " ICEBERG_METADATA_OBJECT_SCHEMA (GS) and ICEBERG_METADATA_FIELDS, or stop"
                + " emitting it for managed Iceberg.");
      }
      Object value = entry.getValue();
      // The schema declares `key STRING`, but the record key can convert to a non-String (e.g. an
      // INT-keyed topic yields an Integer, a struct/Avro key a Map). Coerce to String so the
      // strict typed-OBJECT cast accepts it.
      if (KEY.equals(field) && value != null && !(value instanceof String)) {
        value = String.valueOf(value);
      }
      // The schema declares `headers MAP(VARCHAR, VARCHAR)`. When structured headers are enabled
      // convertHeaders keeps native (non-String) values; coerce each to String so the strict cast
      // accepts them (v3/VARIANT tables never reach here and keep headers as nested VARIANT).
      // buildMetadata always produces a Map from convertHeaders, so a non-Map value is an
      // invariant violation — fail loudly rather than silently skip the coercion.
      if (HEADERS.equals(field) && value != null) {
        if (!(value instanceof Map)) {
          throw new IllegalStateException(
              "HEADERS metadata field must be a Map when non-null, but got: "
                  + value.getClass().getName()
                  + ". KC metadata emission and ICEBERG_METADATA_FIELDS have drifted.");
        }
        @SuppressWarnings("unchecked")
        Map<String, Object> headerMap = (Map<String, Object>) value;
        Map<String, String> stringified = new HashMap<>();
        for (Map.Entry<String, Object> h : headerMap.entrySet()) {
          stringified.put(h.getKey(), h.getValue() == null ? null : String.valueOf(h.getValue()));
        }
        value = stringified;
      }
      conformed.put(field, value);
    }
    // Pad every declared OBJECT sub-field that the current record left absent. A record carries
    // exactly one timestamp type (the other is absent); key and headers are absent on
    // keyless/headerless records. Config-gated fields (topic, offset, partition,
    // connector-push-time) are guaranteed present by config-time validation, so putIfAbsent
    // is a no-op for them. Iterating ICEBERG_METADATA_FIELDS guarantees we can never miss a
    // newly added field.
    for (String f : ICEBERG_METADATA_FIELDS) {
      conformed.putIfAbsent(f, null);
    }
    return conformed;
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
