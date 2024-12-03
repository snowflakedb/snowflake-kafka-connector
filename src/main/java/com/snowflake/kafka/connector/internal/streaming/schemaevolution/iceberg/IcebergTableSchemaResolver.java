package com.snowflake.kafka.connector.internal.streaming.schemaevolution.iceberg;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Streams;
import com.snowflake.kafka.connector.internal.SnowflakeErrors;
import com.snowflake.kafka.connector.records.RecordService;
import java.util.*;
import java.util.stream.Collectors;
import net.snowflake.ingest.streaming.internal.ColumnProperties;
import org.apache.iceberg.types.Type;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class IcebergTableSchemaResolver {
  private static final Logger LOGGER = LoggerFactory.getLogger(IcebergTableSchemaResolver.class);
  private final IcebergColumnTreeFactory treeFactory;

  public IcebergTableSchemaResolver() {
    this.treeFactory = new IcebergColumnTreeFactory();
  }

  /**
   * Retrieve IcebergSchema stored in a channel, then parse it into a tree. Filter out columns that
   * do not need to be modified.
   */
  public List<IcebergColumnTree> resolveIcebergSchemaFromChannel(
      Map<String, ColumnProperties> tableSchemaFromChannel, Set<String> columnsToEvolve) {

    return tableSchemaFromChannel.entrySet().stream()
        .filter(
            (schemaFromChannelEntry) -> {
              String columnNameFromChannel = schemaFromChannelEntry.getKey();
              return columnsToEvolve.contains(columnNameFromChannel);
            })
        .map(this::mapIcebergSchemaFromChannel)
        .map(treeFactory::fromIcebergSchema)
        .collect(Collectors.toList());
  }

  public List<IcebergColumnTree> resolveIcebergSchemaFromRecord(
      SinkRecord record, Set<String> columnsToEvolve) {
    if (columnsToEvolve == null || columnsToEvolve.isEmpty()) {
      return ImmutableList.of();
    }
    if (hasSchema(record)) {
      LOGGER.debug(
          "Schema found. Evolve columns basing on a record's schema, column: {}", columnsToEvolve);
      return getTableSchemaFromRecordSchema(record, columnsToEvolve);
    } else {
      LOGGER.debug(
          "Schema NOT found. Evolve columns basing on a record's payload, columns: {}",
          columnsToEvolve);
      return getTableSchemaFromJson(record, columnsToEvolve);
    }
  }

  private IcebergColumnSchema mapIcebergSchemaFromChannel(
      Map.Entry<String, ColumnProperties> schemaFromChannelEntry) {

    ColumnProperties columnProperty = schemaFromChannelEntry.getValue();
    String plainIcebergSchema = columnProperty.getIcebergSchema();

    Type schema = IcebergDataTypeParser.deserializeIcebergType(plainIcebergSchema);
    String columnName = schemaFromChannelEntry.getKey();
    return new IcebergColumnSchema(schema, columnName);
  }

  private boolean hasSchema(SinkRecord record) {
    return record.valueSchema() != null
        && record.valueSchema().fields() != null
        && !record.valueSchema().fields().isEmpty();
  }

  private List<IcebergColumnTree> getTableSchemaFromJson(
      SinkRecord record, Set<String> columnsToEvolve) {
    JsonNode recordNode = RecordService.convertToJson(record.valueSchema(), record.value(), true);

    return Streams.stream(recordNode.fields())
        .map(IcebergColumnJsonValuePair::from)
        .filter(pair -> columnsToEvolve.contains(pair.getColumnName().toUpperCase()))
        .map(treeFactory::fromJson)
        .collect(Collectors.toList());
  }

  /**
   * Given a SinkRecord, get the schema information from it
   *
   * @param record the sink record that contains the schema and actual data
   * @return list of column representation in a form of tree
   */
  private List<IcebergColumnTree> getTableSchemaFromRecordSchema(
      SinkRecord record, Set<String> columnsToEvolve) {

    List<Field> schemaColumns = record.valueSchema().fields();
    List<Field> foundColumns =
        schemaColumns.stream()
            .filter(
                schemaColumnName -> columnsToEvolve.contains(schemaColumnName.name().toUpperCase()))
            .collect(Collectors.toList());

    if (foundColumns.size() < columnsToEvolve.size()) {
      List<String> notFoundColumns =
          schemaColumns.stream()
              .map(Field::name)
              .filter(schemaColumnName -> !columnsToEvolve.contains(schemaColumnName.toUpperCase()))
              .collect(Collectors.toList());

      throw SnowflakeErrors.ERROR_5022.getException(
          "Columns not found in schema: "
              + notFoundColumns
              + ", schemaColumns: "
              + schemaColumns.stream().map(Field::name).collect(Collectors.toList())
              + ", foundColumns: "
              + foundColumns.stream().map(Field::name).collect(Collectors.toList()));
    }
    return foundColumns.stream().map(treeFactory::fromConnectSchema).collect(Collectors.toList());
  }
}
