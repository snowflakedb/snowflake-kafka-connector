package com.snowflake.kafka.connector.internal.streaming.schemaevolution.iceberg;

import com.google.common.annotations.VisibleForTesting;
import com.snowflake.kafka.connector.internal.SnowflakeConnectionService;
import com.snowflake.kafka.connector.internal.SnowflakeKafkaConnectorException;
import com.snowflake.kafka.connector.internal.streaming.schemaevolution.SchemaEvolutionTargetItems;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import net.snowflake.ingest.streaming.internal.ColumnProperties;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IcebergSchemaEvolutionService {

  private static final Logger LOGGER = LoggerFactory.getLogger(IcebergSchemaEvolutionService.class);

  private final SnowflakeConnectionService conn;
  private final IcebergTableSchemaResolver icebergTableSchemaResolver;

  public IcebergSchemaEvolutionService(SnowflakeConnectionService conn) {
    this.conn = conn;
    this.icebergTableSchemaResolver = new IcebergTableSchemaResolver();
  }

  @VisibleForTesting
  IcebergSchemaEvolutionService(
      SnowflakeConnectionService conn, IcebergTableSchemaResolver tableSchemaResolver) {
    this.conn = conn;
    this.icebergTableSchemaResolver = tableSchemaResolver;
  }

  /**
   * @param targetItems column and field names from InsertError returned by ingest-sdk
   * @param record record that caused an error
   * @param schemaAlreadyInUse schema stored in a channel
   */
  public void evolveIcebergSchemaIfNeeded(
      SchemaEvolutionTargetItems targetItems,
      SinkRecord record,
      Map<String, ColumnProperties> schemaAlreadyInUse) {
    String tableName = targetItems.getTableName();

    Set<String> columnsToEvolve = getColumnsToEvolve(targetItems);

    // Add columns if needed, ignore any exceptions since other task might be succeeded
    if (!columnsToEvolve.isEmpty()) {
      LOGGER.debug("Adding columns to iceberg table: {} columns: {}", tableName, columnsToEvolve);
      // some of the column might already exist, and we will modify them, not create
      List<IcebergColumnTree> alreadyExistingColumns =
          icebergTableSchemaResolver.resolveIcebergSchemaFromChannel(
              schemaAlreadyInUse, columnsToEvolve);

      // new columns resolved from incoming record
      List<IcebergColumnTree> modifiedOrAddedColumns =
          icebergTableSchemaResolver.resolveIcebergSchemaFromRecord(record, columnsToEvolve);

      // columns that we simply add because they are NOT present in an already existing schema.
      List<IcebergColumnTree> addedColumns =
          modifiedOrAddedColumns.stream()
              .filter(
                  modifiedOrAddedColumn ->
                      alreadyExistingColumns.stream()
                          .noneMatch(
                              tree ->
                                  tree.getColumnName()
                                      .equalsIgnoreCase(modifiedOrAddedColumn.getColumnName())))
              .collect(Collectors.toList());
      // column that are present in a schema and needs to have its type modified
      List<IcebergColumnTree> modifiedColumns =
          modifiedOrAddedColumns.stream()
              .filter(
                  modifiedOrAddedColumn ->
                      alreadyExistingColumns.stream()
                          .anyMatch(
                              tree ->
                                  tree.getColumnName()
                                      .equalsIgnoreCase(modifiedOrAddedColumn.getColumnName())))
              .collect(Collectors.toList());

      String addColumnsQuery = generateAddColumnQuery(addedColumns);

      // merge changes into already existing column
      alreadyExistingColumns.forEach(
          existingColumn -> {
            IcebergColumnTree mewVersion =
                modifiedColumns.stream()
                    .filter(c -> c.getColumnName().equals(existingColumn.getColumnName()))
                    .collect(Collectors.toList())
                    .get(0);
            existingColumn.merge(mewVersion);
          });
      String alterSetDataTypeQuery = alterSetDataTypeQuery(alreadyExistingColumns);
      try {
        conn.appendColumnsToIcebergTable(tableName, addColumnsQuery, alterSetDataTypeQuery);
      } catch (SnowflakeKafkaConnectorException e) {
        LOGGER.warn(
            String.format(
                "Failure altering iceberg table to add column: %s, this could happen when multiple"
                    + " partitions try to alter the table at the same time and the warning could be"
                    + " ignored",
                tableName),
            e);
      }
    }
  }

  /**
   * Get only column names, ignore nested field names. Remove double quotes.
   *
   * <p>example: TEST_STRUCT.field1 -> TEST_STRUCT
   */
  private Set<String> getColumnsToEvolve(SchemaEvolutionTargetItems targetItems) {
    return targetItems.getColumnsToAdd().stream()
        // remove double quotes
        .map(targetItem -> targetItem.split("\\.")[0].replaceAll("\"", ""))
        .collect(Collectors.toSet());
  }

  private String generateAddColumnQuery(List<IcebergColumnTree> columnsToAdd) {
    if (columnsToAdd.isEmpty()) {
      return "";
    }
    StringBuilder addColumnQuery = new StringBuilder("alter iceberg ");
    addColumnQuery.append("table identifier(?) add column ");

    for (IcebergColumnTree column : columnsToAdd) {
      addColumnQuery.append("if not exists ");

      String columnName = column.getColumnName();
      String dataType = column.buildType();

      addColumnQuery.append(" ").append(columnName).append(" ").append(dataType).append(", ");
    }
    // remove last comma and whitespace
    addColumnQuery.deleteCharAt(addColumnQuery.length() - 1);
    addColumnQuery.deleteCharAt(addColumnQuery.length() - 1);
    return addColumnQuery.toString();
  }

  private String alterSetDataTypeQuery(List<IcebergColumnTree> columnsToModify) {
    if (columnsToModify.isEmpty()) {
      return "";
    }
    StringBuilder setDataTypeQuery = new StringBuilder("alter iceberg ");
    setDataTypeQuery.append("table identifier(?) alter column ");
    for (IcebergColumnTree column : columnsToModify) {
      String columnName = column.getColumnName();
      String dataType = column.buildType();

      setDataTypeQuery.append(columnName).append(" set data type ").append(dataType).append(", ");
    }
    // remove last comma and whitespace
    setDataTypeQuery.deleteCharAt(setDataTypeQuery.length() - 1);
    setDataTypeQuery.deleteCharAt(setDataTypeQuery.length() - 1);

    return setDataTypeQuery.toString();
  }
}
