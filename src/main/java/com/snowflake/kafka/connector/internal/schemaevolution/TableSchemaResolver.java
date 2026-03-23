/*
 * Copyright (c) 2026 Snowflake Computing Inc. All rights reserved.
 */

package com.snowflake.kafka.connector.internal.schemaevolution;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Streams;
import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.internal.SnowflakeErrors;
import com.snowflake.kafka.connector.records.SnowflakeSinkRecord;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Resolves table schema from Kafka SinkRecord. Supports both schema-ful (Avro/Protobuf) and
 * schema-less (JSON) records.
 */
public class TableSchemaResolver {

  private static final Logger LOGGER = LoggerFactory.getLogger(TableSchemaResolver.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private final ColumnTypeMapper columnTypeMapper;

  public TableSchemaResolver(ColumnTypeMapper columnTypeMapper) {
    this.columnTypeMapper = columnTypeMapper;
  }

  public TableSchemaResolver() {
    this(new SnowflakeColumnTypeMapper());
  }

  /**
   * Collect column data types from either the Kafka Connect schema or the content values.
   *
   * @param record the SnowflakeSinkRecord containing schema and content
   * @param columnsToInclude the names of the columns to include in the schema
   * @return a Map object where the key is column name and value is ColumnInfos
   */
  public TableSchema resolveTableSchemaFromSnowflakeRecord(
      SnowflakeSinkRecord record, List<String> columnsToInclude) {
    if (columnsToInclude == null || columnsToInclude.isEmpty()) {
      return new TableSchema(ImmutableMap.of());
    }

    Set<String> columnNamesSet = new HashSet<>(columnsToInclude);

    if (hasSchema(record)) {
      return getTableSchemaFromRecordSchema(record, columnNamesSet);
    } else {
      return getTableSchemaFromJson(record, columnNamesSet);
    }
  }

  private boolean hasSchema(SnowflakeSinkRecord record) {
    Schema schema = record.getSchema();
    return schema != null && schema.fields() != null && !schema.fields().isEmpty();
  }

  private TableSchema getTableSchemaFromRecordSchema(
      SnowflakeSinkRecord record, Set<String> columnNamesSet) {
    JsonNode recordNode = OBJECT_MAPPER.valueToTree(record.getContent());
    Map<String, ColumnInfos> schemaMap = getFullSchemaMapFromRecord(record);
    Map<Boolean, List<ColumnValuePair>> columnsWitValue =
        Streams.stream(recordNode.fields())
            .map(ColumnValuePair::from)
            .filter(pair -> columnNamesSet.contains(pair.getQuotedColumnName()))
            .collect(
                Collectors.partitioningBy((pair -> schemaMap.containsKey(pair.getColumnName()))));

    List<ColumnValuePair> notFoundFieldsInSchema = columnsWitValue.get(false);
    List<ColumnValuePair> foundFieldsInSchema = columnsWitValue.get(true);

    if (!notFoundFieldsInSchema.isEmpty()) {
      throw SnowflakeErrors.ERROR_5022.getException(
          "Columns not found in schema: "
              + notFoundFieldsInSchema.stream()
                  .map(ColumnValuePair::getColumnName)
                  .collect(Collectors.toList())
              + ", schemaMap: "
              + schemaMap);
    }
    Map<String, ColumnInfos> columnsInferredFromSchema =
        foundFieldsInSchema.stream()
            .map(
                pair ->
                    Maps.immutableEntry(
                        Utils.quoteNameIfNeeded(pair.getQuotedColumnName()),
                        schemaMap.get(pair.getColumnName())))
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey, Map.Entry::getValue, (oldValue, newValue) -> newValue));
    return new TableSchema(columnsInferredFromSchema);
  }

  private TableSchema getTableSchemaFromJson(
      SnowflakeSinkRecord record, Set<String> columnNamesSet) {
    JsonNode recordNode = OBJECT_MAPPER.valueToTree(record.getContent());
    Map<String, ColumnInfos> columnsInferredFromJson =
        Streams.stream(recordNode.fields())
            .map(ColumnValuePair::from)
            .filter(pair -> columnNamesSet.contains(pair.getQuotedColumnName()))
            .map(
                pair ->
                    Maps.immutableEntry(
                        pair.getQuotedColumnName(),
                        new ColumnInfos(inferDataTypeFromJsonObject(pair.getJsonNode()))))
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey, Map.Entry::getValue, (oldValue, newValue) -> newValue));
    return new TableSchema(columnsInferredFromJson);
  }

  /**
   * Build column type information from a Kafka Connect schema.
   *
   * @param schema the Kafka Connect value schema
   * @return a Map where the key is the raw field name and value is ColumnInfos
   */
  private Map<String, ColumnInfos> getFullSchemaMapFromRecord(SnowflakeSinkRecord record) {
    Map<String, ColumnInfos> schemaMap = new HashMap<>();
    Schema schema = record.getSchema();
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

  /** Try to infer the data type from the data */
  private String inferDataTypeFromJsonObject(JsonNode value) {
    Schema.Type schemaType = columnTypeMapper.mapJsonNodeTypeToKafkaType(value);
    if (schemaType == null) {
      // only when the type of the value is unrecognizable for JAVA
      throw SnowflakeErrors.ERROR_5021.getException("class: " + value.getClass());
    }
    // Passing null to schemaName when there is no schema information
    return columnTypeMapper.mapToColumnType(schemaType);
  }

  // ---- ColumnValuePair ----

  private static class ColumnValuePair {
    private final String columnName;
    private final String quotedColumnName;
    private final JsonNode jsonNode;

    public static ColumnValuePair from(Map.Entry<String, JsonNode> field) {
      return new ColumnValuePair(field.getKey(), field.getValue());
    }

    private ColumnValuePair(String columnName, JsonNode jsonNode) {
      this.columnName = columnName;
      this.quotedColumnName = Utils.quoteNameIfNeeded(columnName);
      this.jsonNode = jsonNode;
    }

    public String getColumnName() {
      return columnName;
    }

    public String getQuotedColumnName() {
      return quotedColumnName;
    }

    public JsonNode getJsonNode() {
      return jsonNode;
    }
  }
}
