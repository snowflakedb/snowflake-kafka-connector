package com.snowflake.kafka.connector.internal;

import com.snowflake.kafka.connector.internal.telemetry.SnowflakeTelemetryService;
import java.sql.Connection;
import java.util.List;
import java.util.Optional;

public interface SnowflakeConnectionService {
  /**
   * Create a table with one variant columns: RECORD_METADATA
   *
   * @param tableName a string represents table name
   * @param overwrite if true, execute "create or replace table" query; otherwise, run "create table
   *     if not exists"
   */
  void createTableWithMetadataColumn(String tableName, boolean overwrite);

  /**
   * create table is not exists
   *
   * @param tableName table name
   */
  void createTableWithMetadataColumn(String tableName);

  /**
   * check table existence
   *
   * @param tableName table name
   * @return true if table exists, false otherwise
   */
  boolean tableExist(String tableName);

  /**
   * check pipe existence
   *
   * @param pipeName pipe name
   * @return true if pipe exists, false otherwise
   */
  boolean pipeExist(String pipeName);

  /**
   * Check the given table has correct schema correct schema: (record_metadata variant)
   *
   * @param tableName table name
   * @return true if schema is correct, false is schema is incorrect or table does not exist
   */
  boolean isTableCompatible(String tableName);

  /**
   * check if a given database exists
   *
   * @param databaseName database name
   */
  void databaseExists(String databaseName);

  /**
   * check if a given schema exists
   *
   * @param schemaName schema name
   */
  void schemaExists(String schemaName);

  /** @return telemetry client */
  SnowflakeTelemetryService getTelemetryClient();

  /** Close Connection */
  void close();

  /** @return true is connection is closed */
  boolean isClosed();

  /** @return name of Kafka Connector instance */
  String getConnectorName();

  /** @return the raw jdbc connection */
  Connection getConnection();

  /**
   * Create a table with only the RECORD_METADATA column. The rest of the columns might be added
   * through schema evolution
   *
   * <p>In the beginning of the function we will check if we have the permission to do schema
   * evolution, and we will error out if we don't
   *
   * @param tableName table name
   */
  void createTableWithOnlyMetadataColumn(String tableName);

  /**
   * Alter the RECORD_METADATA column to be of the required structured OBJECT type for iceberg
   * tables.
   *
   * @param tableName iceberg table name
   */
  void initializeMetadataColumnTypeForIceberg(String tableName);

  /**
   * Add the RECORD_METADATA column to the iceberg table if it does not exist.
   *
   * @param tableName iceberg table name
   */
  void addMetadataColumnForIcebergIfNotExists(String tableName);

  /**
   * Calls describe table statement and returns all columns and corresponding types.
   *
   * @param tableName - table name
   * @return Optional.empty() if table does not exist. List of all table columns and their types
   *     otherwise.
   */
  Optional<List<DescribeTableRow>> describeTable(String tableName);

  /**
   * execute sql query
   *
   * @param query sql query string
   * @param parameters query parameters
   */
  void executeQueryWithParameters(String query, String... parameters);
}
