/*
 * Copyright (c) 2026 Snowflake Computing Inc. All rights reserved.
 *
 * Ported from KC v3.2 for client-side schema evolution in KC v4.
 */

package com.snowflake.kafka.connector.internal.schemaevolution;

import com.snowflake.kafka.connector.internal.SnowflakeConnectionService;
import com.snowflake.kafka.connector.internal.SnowflakeKafkaConnectorException;
import java.util.ArrayList;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Executes schema evolution DDL (ALTER TABLE) based on validation results.
 * Handles adding columns and dropping NOT NULL constraints.
 */
public class SnowflakeSchemaEvolutionService {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(SnowflakeSchemaEvolutionService.class);

  private final SnowflakeConnectionService conn;
  private final TableSchemaResolver tableSchemaResolver;

  public SnowflakeSchemaEvolutionService(SnowflakeConnectionService conn) {
    this.conn = conn;
    this.tableSchemaResolver = new TableSchemaResolver();
  }

  SnowflakeSchemaEvolutionService(
      SnowflakeConnectionService conn, TableSchemaResolver tableSchemaResolver) {
    this.conn = conn;
    this.tableSchemaResolver = tableSchemaResolver;
  }

  /**
   * Execute ALTER TABLE commands if there are columns to add or NOT NULL constraints to drop.
   *
   * @param targetItems target items for schema evolution
   * @param record the sink record that contains the schema and actual data
   */
  public void evolveSchemaIfNeeded(SchemaEvolutionTargetItems targetItems, SinkRecord record) {
    if (!targetItems.hasDataForSchemaEvolution()) {
      return;
    }

    String tableName = targetItems.getTableName();

    // Drop NOT NULL constraints if needed
    if (!targetItems.getColumnsToDropNonNullability().isEmpty()) {
      LOGGER.debug(
          "Dropping nonNullability for table: {} columns: {}",
          tableName,
          targetItems.getColumnsToDropNonNullability());
      try {
        conn.alterNonNullableColumns(
            tableName, new ArrayList<>(targetItems.getColumnsToDropNonNullability()));
      } catch (SnowflakeKafkaConnectorException e) {
        LOGGER.warn(
            "Failure altering table to update nullability: {}, this could happen when multiple"
                + " partitions try to alter the table at the same time and the warning could be"
                + " ignored",
            tableName,
            e);
      }
    }

    // Add new columns if needed
    if (!targetItems.getColumnsToAdd().isEmpty()) {
      LOGGER.debug(
          "Adding columns to table: {} columns: {}",
          tableName,
          targetItems.getColumnsToAdd());
      TableSchema tableSchema =
          tableSchemaResolver.resolveTableSchemaFromRecord(
              record, new ArrayList<>(targetItems.getColumnsToAdd()));
      try {
        conn.appendColumnsToTable(tableName, tableSchema.getColumnInfos());
      } catch (SnowflakeKafkaConnectorException e) {
        LOGGER.warn(
            "Failure altering table to add column: {}, this could happen when multiple"
                + " partitions try to alter the table at the same time and the warning could be"
                + " ignored",
            tableName,
            e);
      }
    }
  }
}
