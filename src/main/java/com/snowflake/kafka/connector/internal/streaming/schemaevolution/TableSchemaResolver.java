package com.snowflake.kafka.connector.internal.streaming.schemaevolution;

import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.internal.SnowflakeErrors;
import com.snowflake.kafka.connector.records.RecordService;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class TableSchemaResolver {

  private static final Logger LOGGER = LoggerFactory.getLogger(TableSchemaResolver.class);

  private final ColumnTypeMapper columnTypeMapper;

  protected TableSchemaResolver(ColumnTypeMapper columnTypeMapper) {
    this.columnTypeMapper = columnTypeMapper;
  }

  /**
   * With the list of columns, collect their data types from either the schema or the data itself
   *
   * @param record the sink record that contains the schema and actual data
   * @param columnNames the names of the extra columns
   * @return a Map object where the key is column name and value is ColumnInfos
   */
  public TableSchema resolveTableSchemaFromRecord(SinkRecord record, List<String> columnNames) {
    if (columnNames == null) {
      return new TableSchema(new HashMap<>());
    }
    Map<String, ColumnInfos> columnToType = new HashMap<>();
    Map<String, ColumnInfos> schemaMap = getSchemaMapFromRecord(record);
    JsonNode recordNode = RecordService.convertToJson(record.valueSchema(), record.value(), true);
    Set<String> columnNamesSet = new HashSet<>(columnNames);

    Iterator<Map.Entry<String, JsonNode>> fields = recordNode.fields();
    while (fields.hasNext()) {
      Map.Entry<String, JsonNode> field = fields.next();
      String colName = Utils.quoteNameIfNeeded(field.getKey());
      if (columnNamesSet.contains(colName)) {
        ColumnInfos columnInfos;
        if (schemaMap.isEmpty()) {
          // No schema associated with the record, we will try to infer it based on the data
          columnInfos = new ColumnInfos(inferDataTypeFromJsonObject(field.getValue()), null);
        } else {
          // Get from the schema
          columnInfos = schemaMap.get(field.getKey());
          if (columnInfos == null) {
            // only when the type of the value is unrecognizable for JAVA
            throw SnowflakeErrors.ERROR_5022.getException(
                "column: " + field.getKey() + " schemaMap: " + schemaMap);
          }
        }
        columnToType.put(colName, columnInfos);
      }
    }
    return new TableSchema(columnToType);
  }

  /**
   * Given a SinkRecord, get the schema information from it
   *
   * @param record the sink record that contains the schema and actual data
   * @return a Map object where the key is column name and value is ColumnInfos
   */
  private Map<String, ColumnInfos> getSchemaMapFromRecord(SinkRecord record) {
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
}
