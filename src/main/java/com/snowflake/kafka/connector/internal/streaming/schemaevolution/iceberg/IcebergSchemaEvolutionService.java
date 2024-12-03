package com.snowflake.kafka.connector.internal.streaming.schemaevolution.iceberg;

import com.google.common.collect.Maps;
import com.snowflake.kafka.connector.internal.SnowflakeConnectionService;
import com.snowflake.kafka.connector.internal.SnowflakeKafkaConnectorException;
import com.snowflake.kafka.connector.internal.streaming.schemaevolution.ColumnInfos;
import com.snowflake.kafka.connector.internal.streaming.schemaevolution.SchemaEvolutionService;
import com.snowflake.kafka.connector.internal.streaming.schemaevolution.SchemaEvolutionTargetItems;
import java.util.*;
import java.util.stream.Collectors;
import net.snowflake.ingest.streaming.internal.ColumnProperties;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IcebergSchemaEvolutionService implements SchemaEvolutionService {

  private static final Logger LOGGER = LoggerFactory.getLogger(IcebergSchemaEvolutionService.class);

  private final SnowflakeConnectionService conn;
  private final IcebergTableSchemaResolver icebergTableSchemaResolver;
  private final IcebergColumnTreeMerger mergeTreeService;
  private final IcebergColumnTreeTypeBuilder typeBuilder;

  public IcebergSchemaEvolutionService(SnowflakeConnectionService conn) {
    this.conn = conn;
    this.icebergTableSchemaResolver = new IcebergTableSchemaResolver();
    this.mergeTreeService = new IcebergColumnTreeMerger();
    this.typeBuilder = new IcebergColumnTreeTypeBuilder();
  }

  /**
   * @param targetItems column and field names from InsertError returned by ingest-sdk
   * @param record record that caused an error
   * @param existingSchema schema stored in a channel
   */
  @Override
  public void evolveSchemaIfNeeded(
      SchemaEvolutionTargetItems targetItems,
      SinkRecord record,
      Map<String, ColumnProperties> existingSchema) {
    String tableName = targetItems.getTableName();

    Set<String> columnsToEvolve = extractColumnNames(targetItems);

    // Add columns if needed, ignore any exceptions since other task might be succeeded
    if (!columnsToEvolve.isEmpty()) {
      LOGGER.debug("Adding columns to iceberg table: {} columns: {}", tableName, columnsToEvolve);
      // some of the column might already exist, and we will modify them, not create
      List<IcebergColumnTree> alreadyExistingColumns =
          icebergTableSchemaResolver.resolveIcebergSchemaFromChannel(
              existingSchema, columnsToEvolve);

      List<IcebergColumnTree> modifiedOrAddedColumns =
          icebergTableSchemaResolver.resolveIcebergSchemaFromRecord(record, columnsToEvolve);

      List<IcebergColumnTree> columnsToAdd =
          distinguishColumnsToAdd(alreadyExistingColumns, modifiedOrAddedColumns);

      List<IcebergColumnTree> columnsToModify =
          distinguishColumnsToModify(alreadyExistingColumns, modifiedOrAddedColumns);

      alterAddColumns(tableName, columnsToAdd);

      alterDataType(tableName, alreadyExistingColumns, columnsToModify);
    }
  }

  /**
   * Get only column names, ignore nested field names, remove double quotes.
   *
   * <p>example: TEST_STRUCT.field1 -> TEST_STRUCT
   */
  private Set<String> extractColumnNames(SchemaEvolutionTargetItems targetItems) {
    return targetItems.getColumnsToAdd().stream()
        .map(this::removeNestedFieldNames)
        .map(this::removeDoubleQuotes)
        .collect(Collectors.toSet());
  }

  private String removeNestedFieldNames(String columnNameWithNestedNames) {
    return columnNameWithNestedNames.split("\\.")[0];
  }

  private String removeDoubleQuotes(String columnName) {
    return columnName.replaceAll("\"", "");
  }

  /** Columns that are not present in a current schema are to be added */
  private List<IcebergColumnTree> distinguishColumnsToAdd(
      List<IcebergColumnTree> alreadyExistingColumns,
      List<IcebergColumnTree> modifiedOrAddedColumns) {
    return modifiedOrAddedColumns.stream()
        .filter(
            modifiedOrAddedColumn ->
                alreadyExistingColumns.stream()
                    .noneMatch(
                        tree ->
                            tree.getColumnName()
                                .equalsIgnoreCase(modifiedOrAddedColumn.getColumnName())))
        .collect(Collectors.toList());
  }

  /** If columns is present in a current schema it means it has to be modified */
  private List<IcebergColumnTree> distinguishColumnsToModify(
      List<IcebergColumnTree> alreadyExistingColumns,
      List<IcebergColumnTree> modifiedOrAddedColumns) {
    return modifiedOrAddedColumns.stream()
        .filter(
            modifiedOrAddedColumn ->
                alreadyExistingColumns.stream()
                    .anyMatch(
                        tree ->
                            tree.getColumnName()
                                .equalsIgnoreCase(modifiedOrAddedColumn.getColumnName())))
        .collect(Collectors.toList());
  }

  private void alterAddColumns(String tableName, List<IcebergColumnTree> addedColumns) {
    if (addedColumns.isEmpty()) {
      return;
    }
    Map<String, ColumnInfos> columnInfosMap = toColumnInfos(addedColumns);
    try {
      conn.appendColumnsToIcebergTable(tableName, columnInfosMap);
    } catch (SnowflakeKafkaConnectorException e) {
      logQueryFailure(tableName, e);
    }
  }

  private void alterDataType(
      String tableName,
      List<IcebergColumnTree> alreadyExistingColumns,
      List<IcebergColumnTree> modifiedColumns) {
    if (modifiedColumns.isEmpty()) {
      return;
    }
    mergeChangesIntoExistingColumns(alreadyExistingColumns, modifiedColumns);
    Map<String, ColumnInfos> columnInfosMap = toColumnInfos(alreadyExistingColumns);
    try {
      conn.alterColumnsDataTypeIcebergTable(tableName, columnInfosMap);
    } catch (SnowflakeKafkaConnectorException e) {
      logQueryFailure(tableName, e);
    }
  }

  private void mergeChangesIntoExistingColumns(
      List<IcebergColumnTree> alreadyExistingColumns, List<IcebergColumnTree> modifiedColumns) {
    alreadyExistingColumns.forEach(
        existingColumn -> {
          List<IcebergColumnTree> modifiedColumnMatchingExisting =
              modifiedColumns.stream()
                  .filter(c -> c.getColumnName().equals(existingColumn.getColumnName()))
                  .collect(Collectors.toList());
          if (modifiedColumnMatchingExisting.size() != 1) {
            LOGGER.warn(
                "Skipping schema evolution of a column {}. Incorrect number of new versions of the"
                    + " column: {}",
                existingColumn.getColumnName(),
                modifiedColumnMatchingExisting.stream()
                    .map(IcebergColumnTree::getColumnName)
                    .collect(Collectors.toList()));
          }
          mergeTreeService.merge(existingColumn, modifiedColumnMatchingExisting.get(0));
        });
  }

  private Map<String, ColumnInfos> toColumnInfos(List<IcebergColumnTree> columnTrees) {
    return columnTrees.stream()
        .map(
            columnTree ->
                Maps.immutableEntry(
                    columnTree.getColumnName(),
                    new ColumnInfos(typeBuilder.buildType(columnTree), columnTree.getComment())))
        .collect(
            Collectors.toMap(
                Map.Entry::getKey, Map.Entry::getValue, (oldValue, newValue) -> newValue));
  }

  private void logQueryFailure(String tableName, SnowflakeKafkaConnectorException e) {
    LOGGER.warn(
        String.format(
            "Failure altering iceberg table to add column: %s, this could happen when multiple"
                + " partitions try to alter the table at the same time and the warning could be"
                + " ignored",
            tableName),
        e);
  }
}
