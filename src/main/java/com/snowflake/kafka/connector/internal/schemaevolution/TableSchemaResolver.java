/*
 * Copyright (c) 2026 Snowflake Computing Inc. All rights reserved.
 *
 * Ported from KC v3.2 for client-side schema evolution in KC v4.
 * Adapted to use KafkaRecordConverter instead of RecordService.
 */

package com.snowflake.kafka.connector.internal.schemaevolution;

import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.records.KafkaRecordConverter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Resolves table schema from Kafka SinkRecord. Supports both schema-ful (Avro/Protobuf) and
 * schema-less (JSON) records.
 */
public class TableSchemaResolver {

  private static final Logger LOGGER = LoggerFactory.getLogger(TableSchemaResolver.class);

  private final ColumnTypeMapper columnTypeMapper;

  public TableSchemaResolver(ColumnTypeMapper columnTypeMapper) {
    this.columnTypeMapper = columnTypeMapper;
  }

  public TableSchemaResolver() {
    this(new SnowflakeColumnTypeMapper());
  }

  /**
   * With the list of columns, collect their data types from either the schema or the data itself.
   *
   * @param record the sink record that contains the schema and actual data
   * @param columnsToInclude the names of the columns to include in the schema
   * @return a TableSchema where the key is column name and value is ColumnInfos
   */
  public TableSchema resolveTableSchemaFromRecord(
      SinkRecord record, List<String> columnsToInclude) {
    if (columnsToInclude == null || columnsToInclude.isEmpty()) {
      return new TableSchema(new HashMap<>());
    }

    Set<String> columnNamesSet = new HashSet<>(columnsToInclude);

    if (hasSchema(record)) {
      return getTableSchemaFromRecordSchema(record, columnNamesSet);
    } else {
      return getTableSchemaFromData(record, columnNamesSet);
    }
  }

  private boolean hasSchema(SinkRecord record) {
    return record.valueSchema() != null
        && record.valueSchema().fields() != null
        && !record.valueSchema().fields().isEmpty();
  }

  /** For schema-ful records: extract types from the Kafka Connect schema. */
  private TableSchema getTableSchemaFromRecordSchema(
      SinkRecord record, Set<String> columnNamesSet) {
    Map<String, ColumnInfos> fullSchemaMap = getFullSchemaMapFromRecord(record);
    Map<String, ColumnInfos> filteredSchema = new HashMap<>();

    for (String colName : columnNamesSet) {
      // Column names from ValidationResult are unquoted/uppercase.
      // Schema field names from Kafka are case-sensitive.
      // Try matching both the raw name and the quoted version.
      String quotedName = Utils.quoteNameIfNeeded(colName);
      for (Map.Entry<String, ColumnInfos> entry : fullSchemaMap.entrySet()) {
        String fieldQuoted = Utils.quoteNameIfNeeded(entry.getKey());
        if (fieldQuoted.equals(quotedName)) {
          filteredSchema.put(quotedName, entry.getValue());
          break;
        }
      }
    }

    return new TableSchema(filteredSchema);
  }

  /** For schema-less records: infer types from the Java object values. */
  private TableSchema getTableSchemaFromData(SinkRecord record, Set<String> columnNamesSet) {
    Map<String, Object> recordMap =
        KafkaRecordConverter.convertToMap(record.valueSchema(), record.value());
    Map<String, ColumnInfos> inferredSchema = new HashMap<>();

    for (Map.Entry<String, Object> entry : recordMap.entrySet()) {
      String quotedName = Utils.quoteNameIfNeeded(entry.getKey());
      if (columnNamesSet.contains(quotedName) || columnNamesSet.contains(entry.getKey())) {
        String snowflakeType = columnTypeMapper.inferTypeFromJavaValue(entry.getValue());
        inferredSchema.put(quotedName, new ColumnInfos(snowflakeType));
      }
    }

    return new TableSchema(inferredSchema);
  }

  /** Extract full schema map from record's Kafka Connect schema. */
  private Map<String, ColumnInfos> getFullSchemaMapFromRecord(SinkRecord record) {
    Map<String, ColumnInfos> schemaMap = new HashMap<>();
    Schema schema = record.valueSchema();
    if (schema != null && schema.fields() != null) {
      for (Field field : schema.fields()) {
        String columnType =
            columnTypeMapper.mapToColumnType(field.schema().type(), field.schema().name());
        LOGGER.info(
            "Got the data type for field:{}, schemaName:{}, schemaDoc: {} kafkaType:{},"
                + " columnType:{}",
            field.name(),
            field.schema().name(),
            field.schema().doc(),
            field.schema().type(),
            columnType);

        schemaMap.put(field.name(), new ColumnInfos(columnType, field.schema().doc()));
      }
    }
    return schemaMap;
  }
}
