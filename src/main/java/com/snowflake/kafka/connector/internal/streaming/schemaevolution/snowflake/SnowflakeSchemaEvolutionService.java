package com.snowflake.kafka.connector.internal.streaming.schemaevolution.snowflake;

import com.google.common.annotations.VisibleForTesting;
import com.snowflake.kafka.connector.internal.SnowflakeConnectionService;
import com.snowflake.kafka.connector.internal.SnowflakeKafkaConnectorException;
import com.snowflake.kafka.connector.internal.streaming.schemaevolution.SchemaEvolutionService;
import com.snowflake.kafka.connector.internal.streaming.schemaevolution.SchemaEvolutionTargetItems;
import com.snowflake.kafka.connector.internal.streaming.schemaevolution.TableSchema;
import com.snowflake.kafka.connector.internal.streaming.schemaevolution.TableSchemaResolver;
import java.util.List;
import java.util.Map;
import net.snowflake.ingest.streaming.internal.ColumnProperties;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SnowflakeSchemaEvolutionService implements SchemaEvolutionService {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(SnowflakeSchemaEvolutionService.class);

  private final SnowflakeConnectionService conn;
  private final TableSchemaResolver tableSchemaResolver;

  public SnowflakeSchemaEvolutionService(SnowflakeConnectionService conn) {
    this.conn = conn;
    this.tableSchemaResolver = new SnowflakeTableSchemaResolver();
  }

  @VisibleForTesting
  SnowflakeSchemaEvolutionService(
      SnowflakeConnectionService conn, TableSchemaResolver tableSchemaResolver) {
    this.conn = conn;
    this.tableSchemaResolver = tableSchemaResolver;
  }

  /**
   * Execute an ALTER TABLE command if there is any extra column that needs to be added, or any
   * column nullability that needs to be updated, used by schema evolution
   *
   * @param targetItems target items for schema evolution such as table name, columns to drop,
   *     columns to add
   * @param record the sink record that contains the schema and actual data
   * @param existingSchema is unused in this implementation
   */
  @Override
  public void evolveSchemaIfNeeded(
      SchemaEvolutionTargetItems targetItems,
      SinkRecord record,
      Map<String, ColumnProperties> existingSchema) {
    String tableName = targetItems.getTableName();
    List<String> columnsToDropNullability = targetItems.getColumnsToDropNonNullability();
    // Update nullability if needed, ignore any exceptions since other task might be succeeded
    if (!columnsToDropNullability.isEmpty()) {
      LOGGER.debug(
          "Dropping nonNullability for table: {} columns: {}", tableName, columnsToDropNullability);
      try {
        conn.alterNonNullableColumns(targetItems.getTableName(), columnsToDropNullability);
      } catch (SnowflakeKafkaConnectorException e) {
        LOGGER.warn(
            String.format(
                "Failure altering table to update nullability: %s, this could happen when multiple"
                    + " partitions try to alter the table at the same time and the warning could be"
                    + " ignored",
                tableName),
            e);
      }
    }
    List<String> columnsToAdd = targetItems.getColumnsToAdd();
    // Add columns if needed, ignore any exceptions since other task might be succeeded
    if (!columnsToAdd.isEmpty()) {
      LOGGER.debug("Adding columns to table: {} columns: {}", tableName, columnsToAdd);
      TableSchema tableSchema =
          tableSchemaResolver.resolveTableSchemaFromRecord(record, columnsToAdd);
      try {
        conn.appendColumnsToTable(tableName, tableSchema.getColumnInfos());
      } catch (SnowflakeKafkaConnectorException e) {
        LOGGER.warn(
            String.format(
                "Failure altering table to add column: %s, this could happen when multiple"
                    + " partitions try to alter the table at the same time and the warning could be"
                    + " ignored",
                tableName),
            e);
      }
    }
  }
}
