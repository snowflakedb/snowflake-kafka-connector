package com.snowflake.kafka.connector.internal.streaming.schemaevolution;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Streams;
import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.internal.SnowflakeErrors;
import com.snowflake.kafka.connector.records.RecordService;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
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
   * A map of Lucerna types to Snowflake types. This is used to map the Lucerna data types to their destination Snowflake types.
   * MicroDuration omitted
   */
  private static final Map<String, String> LUCERNA_TYPES = ImmutableMap.of(
      "io.debezium.time.ZonedTimestamp", "TIMESTAMP_TZ",
      "io.debezium.time.ZonedTime", "TIME",
      "io.debezium.time.Date", "DATE",
      "io.debezium.time.Time", "TIME(3)",
      "io.debezium.time.MicroTime", "TIME(6)",
      "io.debezium.time.Timestamp", "TIMESTAMP_NTZ(3)",
      "io.debezium.time.MicroTimestamp", "TIMESTAMP_NTZ(6)"
  );

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

  private boolean hasSchema(SinkRecord record) {
    return record.valueSchema() != null
        && record.valueSchema().fields() != null
        && !record.valueSchema().fields().isEmpty();
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
    FetchSchemaClient instance = FetchSchemaClient.getInstance();
    org.apache.avro.Schema avroSchema = instance.getSchema(record.topic() + "-value");
    Schema schema = record.valueSchema();
    if (schema != null && schema.fields() != null) {
      for (Field field : schema.fields()) {
        String connectName = null;
        if (avroSchema != null && avroSchema.getField(field.name()) != null) {
          connectName = extractConnectNameFromAvroField(avroSchema.getField(field.name()).schema());
        }
        String columnType = connectName != null && LUCERNA_TYPES.get(connectName) != null ? LUCERNA_TYPES.get(connectName) : columnTypeMapper.mapToColumnType(field.schema().type(), field.schema().name());
        LOGGER.info(
            "Got the data type for field:{}, schemaName:{}, connect.name: {} kafkaType:{},"
                + " columnType:{}",
            field.name(),
            field.schema().name(),
            connectName != null ? connectName : "N/A",
            field.schema().type(),
            columnType);

        schemaMap.put(field.name(), new ColumnInfos(columnType, field.schema().doc()));
      }
    }
    return schemaMap;
  }

  /**
   * Extract connect.name property from Avro field schema, handling union types properly.
   * For union types like ["null", "string"], the connect.name property is on the non-null type.
   */
  private String extractConnectNameFromAvroField(org.apache.avro.Schema fieldSchema) {
    // First check if the schema itself has the connect.name property
    String connectName = fieldSchema.getProp("connect.name");
    if (connectName != null) {
      return connectName;
    }
    
    // If it's a union type, check each type in the union for connect.name
    if (fieldSchema.getType() == org.apache.avro.Schema.Type.UNION) {
      for (org.apache.avro.Schema unionType : fieldSchema.getTypes()) {
        // Skip null types
        if (unionType.getType() != org.apache.avro.Schema.Type.NULL) {
          connectName = unionType.getProp("connect.name");
          if (connectName != null) {
            return connectName;
          }
        }
      }
    }
    
    return null;
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
