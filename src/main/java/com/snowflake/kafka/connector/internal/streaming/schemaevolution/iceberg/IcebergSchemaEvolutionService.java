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
    // don't care about fields, just find which columns are changed.
    Set<String> columnsToEvolve =
        targetItems.getColumnsToAdd().stream()
            .map(targetItem -> targetItem.split("\\.")[0].replaceAll("\"", ""))
            .collect(Collectors.toSet());

    // Add columns if needed, ignore any exceptions since other task might be succeeded
    if (!columnsToEvolve.isEmpty()) {
      LOGGER.debug("Adding columns to iceberg table: {} columns: {}", tableName, columnsToEvolve);
      // some of the column might already exist, and we will modify them, not create
      IcebergTableSchema alreadyExistingColumns =
          icebergTableSchemaResolver.resolveIcebergSchemaFromChannel(
              schemaAlreadyInUse, columnsToEvolve);

      // new columns resolved from incoming record
      IcebergTableSchema modifiedOrAddedColumns =
          icebergTableSchemaResolver.resolveIcebergSchema(record, columnsToEvolve);

      // columns that we simply add because they do not exist. They are NOT present in an already
      // existing
      // schema.
      List<IcebergColumnTree> addedColumns =
          modifiedOrAddedColumns.getColumns().stream()
              .filter(
                  modifiedOrAddedColumn ->
                      alreadyExistingColumns.getColumns().stream()
                          .noneMatch(
                              tree ->
                                  tree.getColumnName()
                                      .equalsIgnoreCase(modifiedOrAddedColumn.getColumnName())))
              .collect(Collectors.toList());
      // consider just getting rest of the columns
      List<IcebergColumnTree> modifiedColumns =
          modifiedOrAddedColumns.getColumns().stream()
              .filter(
                  modifiedOrAddedColumn ->
                      alreadyExistingColumns.getColumns().stream()
                          .anyMatch(
                              tree ->
                                  tree.getColumnName()
                                      .equalsIgnoreCase(modifiedOrAddedColumn.getColumnName())))
              .collect(Collectors.toList());

      // merge withAlreadyExistingColumns

      System.out.println("stop debugger");

      try {
        // todo columns to add and column to modify
        conn.appendColumnsToIcebergTable(tableName, addedColumns);
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
}
