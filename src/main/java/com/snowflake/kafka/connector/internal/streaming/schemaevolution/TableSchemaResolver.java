package com.snowflake.kafka.connector.internal.streaming.schemaevolution;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Streams;
import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.internal.SnowflakeErrors;
import com.snowflake.kafka.connector.internal.streaming.schemaevolution.iceberg.ApacheIcebergColumnSchema;
import com.snowflake.kafka.connector.internal.streaming.schemaevolution.iceberg.IcebergColumnTree;
import com.snowflake.kafka.connector.internal.streaming.schemaevolution.iceberg.IcebergDataTypeParser;
import com.snowflake.kafka.connector.internal.streaming.schemaevolution.iceberg.IcebergTableSchema;
import com.snowflake.kafka.connector.records.RecordService;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import net.snowflake.ingest.streaming.internal.ColumnProperties;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.iceberg.types.Type;
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
   * @param columnsToInclude the names of the columns to include in the schema
   * @return a Map object where the key is column name and value is ColumnInfos
   */
  public TableSchema resolveTableSchemaFromRecord(
      SinkRecord record, List<String> columnsToInclude) {
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

  public IcebergTableSchema resolveIcebergSchema(SinkRecord record, List<String> columnsToInclude) {
    if (columnsToInclude == null || columnsToInclude.isEmpty()) {
      return IcebergTableSchema.Empty();
    }
    Set<String> columnNamesSet = new HashSet<>(columnsToInclude);

    if (hasSchema(record)) {
      return getTableSchemaFromRecordSCHEMAIceberg(record, columnNamesSet);
    } else {
      return getTableSchemaFromJsonIceberg(record, columnNamesSet);
    }
  }

  public IcebergTableSchema resolveIcebergSchemaFromChannel(
      Map<String, ColumnProperties> tableSchemaFromChannel, List<String> columnsToInclude) {
    // todo remember about the case with dots
    // todo potential error when cases are different - think easy to overcome
    List<ApacheIcebergColumnSchema> apacheIcebergColumnSchemas =
        tableSchemaFromChannel.entrySet().stream()
            .filter(
                (schemasFromChannelEntry) ->
                    columnsToInclude.contains(schemasFromChannelEntry.getKey()))
            .map(
                (schemasFromChannelEntry) -> {
                  String columnName = schemasFromChannelEntry.getKey();
                  ColumnProperties columnProperty = schemasFromChannelEntry.getValue();
                  String plainIcebergSchema = getIcebergSchema(columnProperty);
                  Type schema = IcebergDataTypeParser.deserializeIcebergType(plainIcebergSchema);
                  return new ApacheIcebergColumnSchema(schema, columnName);
                })
            .collect(Collectors.toList());

    List<IcebergColumnTree> icebergColumnTrees =
        apacheIcebergColumnSchemas.stream()
            .map(IcebergColumnTree::new)
            .collect(Collectors.toList());

    return new IcebergTableSchema(icebergColumnTrees);
  }

  // todo remove it just when we can
  private static String getIcebergSchema(ColumnProperties columnProperties) {
    try {
      // TODO reflection should be replaced by proper builder.setIceberg(true) call in SNOW-1728002
      java.lang.reflect.Field field =
          FieldUtils.getField(ColumnProperties.class, "icebergColumnSchema");
      field.setAccessible(true);
      return (String) field.get(columnProperties);
    } catch (IllegalAccessException e) {
      throw new IllegalStateException(
          "Couldn't set iceberg by accessing private field: " + "isIceberg", e);
    }
  }

  private boolean hasSchema(SinkRecord record) {
    return record.valueSchema() != null
        && record.valueSchema().fields() != null
        && !record.valueSchema().fields().isEmpty();
  }

  private IcebergTableSchema getTableSchemaFromRecordSCHEMAIceberg(
      SinkRecord record, Set<String> columnNamesSet) {
    // todo its second part
    JsonNode recordNode = RecordService.convertToJson(record.valueSchema(), record.value(), true);
    throw new IllegalArgumentException("not yet implemented SCHEMA path");
    // return IcebergTableSchema.Empty();
  }

  private IcebergTableSchema getTableSchemaFromJsonIceberg(
      SinkRecord record, Set<String> columnNamesSet) {
    JsonNode recordNode = RecordService.convertToJson(record.valueSchema(), record.value(), true);

    List<IcebergColumnTree> icebergColumnTrees =
        Streams.stream(recordNode.fields())
            .map(ColumnValuePair::from)
            .filter(pair -> columnNamesSet.contains(pair.getQuotedColumnName()))
            .map(pair -> new IcebergColumnTree(pair.getQuotedColumnName(), pair.getJsonNode()))
            .collect(Collectors.toList());
    return new IcebergTableSchema(icebergColumnTrees);
  }

  private TableSchema getTableSchemaFromRecordSchema(
      SinkRecord record, Set<String> columnNamesSet) {
    JsonNode recordNode = RecordService.convertToJson(record.valueSchema(), record.value(), true);
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

  private TableSchema getTableSchemaFromJson(SinkRecord record, Set<String> columnNamesSet) {
    JsonNode recordNode = RecordService.convertToJson(record.valueSchema(), record.value(), true);
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
   * Given a SinkRecord, get the schema information from it
   *
   * @param record the sink record that contains the schema and actual data
   * @return a Map object where the key is column name and value is ColumnInfos
   */
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
