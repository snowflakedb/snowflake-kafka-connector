/*
 * Copyright (c) 2026 Snowflake Computing Inc. All rights reserved.
 */

package com.snowflake.kafka.connector.internal.schemaevolution;

import com.snowflake.kafka.connector.internal.KCLogger;
import com.snowflake.kafka.connector.internal.SnowflakeConnectionService;
import com.snowflake.kafka.connector.internal.SnowflakeKafkaConnectorException;
import com.snowflake.kafka.connector.records.SnowflakeSinkRecord;
import java.util.ArrayList;

/**
 * Executes schema evolution DDL (ALTER TABLE) based on validation results. Handles adding columns
 * and dropping NOT NULL constraints.
 */
public class SnowflakeSchemaEvolutionService {

  private static final KCLogger LOGGER =
      new KCLogger(SnowflakeSchemaEvolutionService.class.getName());

  private final SnowflakeConnectionService conn;
  private final TableSchemaResolver tableSchemaResolver;

  public SnowflakeSchemaEvolutionService(SnowflakeConnectionService conn) {
    this(conn, new TableSchemaResolver());
  }

  SnowflakeSchemaEvolutionService(
      SnowflakeConnectionService conn, TableSchemaResolver tableSchemaResolver) {
    this.conn = conn;
    this.tableSchemaResolver = tableSchemaResolver;
  }

  /**
   * Execute ALTER TABLE commands if there are columns to add or NOT NULL constraints to drop.
   *
   * <p>Note: Columns must be added BEFORE dropping NOT NULL constraints, otherwise the constraint
   * modification will fail if the column doesn't exist yet.
   *
   * @param targetItems target items for schema evolution
   * @param record the sink record that contains the schema and content
   */
  public void evolveSchemaIfNeeded(
      SchemaEvolutionTargetItems targetItems, SnowflakeSinkRecord record) {
    if (!targetItems.hasDataForSchemaEvolution()) {
      return;
    }

    String tableName = targetItems.getTableName();

    // Add new columns FIRST (must exist before we can modify constraints)
    if (!targetItems.getColumnsToAdd().isEmpty()) {
      LOGGER.debug(
          "Adding columns to table: {} columns: {}", tableName, targetItems.getColumnsToAdd());
      TableSchema tableSchema =
          tableSchemaResolver.resolveTableSchemaFromSnowflakeRecord(
              record, new ArrayList<>(targetItems.getColumnsToAdd()));
      try {
        conn.appendColumnsToTable(tableName, tableSchema.getColumnInfos());
        LOGGER.info(
            "Added columns to table: {}, columns: {}", tableName, targetItems.getColumnsToAdd());
      } catch (SnowflakeKafkaConnectorException e) {
        LOGGER.warn(
            "Failure altering table to add column: {}, this could happen when multiple"
                + " partitions try to alter the table at the same time and the warning could be"
                + " ignored",
            tableName,
            e);
      }
    }

    // Drop NOT NULL constraints AFTER columns exist
    if (!targetItems.getColumnsToDropNonNullability().isEmpty()) {
      LOGGER.debug(
          "Dropping nonNullability for table: {} columns: {}",
          tableName,
          targetItems.getColumnsToDropNonNullability());
      try {
        conn.alterNonNullableColumns(
            tableName, new ArrayList<>(targetItems.getColumnsToDropNonNullability()));
        LOGGER.info(
            "Dropped non-nullability on table: {}, columns: {}",
            tableName,
            targetItems.getColumnsToDropNonNullability());
      } catch (SnowflakeKafkaConnectorException e) {
        LOGGER.warn(
            "Failure altering table to update nullability: {}, this could happen when multiple"
                + " partitions try to alter the table at the same time and the warning could be"
                + " ignored",
            tableName,
            e);
      }
    }
  }
}
