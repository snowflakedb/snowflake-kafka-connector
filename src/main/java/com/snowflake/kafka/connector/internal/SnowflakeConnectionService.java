package com.snowflake.kafka.connector.internal;

import com.snowflake.kafka.connector.internal.streaming.schemaevolution.ColumnInfos;
import com.snowflake.kafka.connector.internal.telemetry.SnowflakeTelemetryService;
import java.sql.Connection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public interface SnowflakeConnectionService {
  /**
   * Create a table with two variant columns: RECORD_METADATA and RECORD_CONTENT
   *
   * @param tableName a string represents table name
   * @param overwrite if true, execute "create or replace table" query; otherwise, run "create table
   *     if not exists"
   */
  void createTable(String tableName, boolean overwrite);

  /**
   * create table is not exists
   *
   * @param tableName table name
   */
  void createTable(String tableName);

  /**
   * create a snowpipe
   *
   * @param pipeName pipe name
   * @param tableName table name
   * @param stageName stage name
   * @param overwrite if true, execute "create or replace pipe" statement, otherwise, run "create
   *     pipe if not exists"
   */
  void createPipe(String tableName, String stageName, String pipeName, boolean overwrite);

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
   * Check the given table has correct schema correct schema: (record_metadata variant,
   * record_content variant)
   *
   * @param tableName table name
   * @return true if schema is correct, false is schema is incorrect or table does not exist
   */
  boolean isTableCompatible(String tableName);

  /**
   * Check whether the user has the role privilege to do schema evolution and whether the schema
   * evolution option is enabled on the table
   *
   * @param tableName the name of the table
   * @param role the role of the user
   * @return whether table and role has the required permission to perform schema evolution
   */
  boolean hasSchemaEvolutionPermission(String tableName, String role);

  /**
   * Alter table to add columns according to a map from columnNames to their types
   *
   * @param tableName the name of the table
   * @param columnInfosMap the mapping from the columnNames to their columnInfos
   */
  void appendColumnsToTable(String tableName, Map<String, ColumnInfos> columnInfosMap);

  /**
   * Alter iceberg table to modify columns datatype
   *
   * @param tableName the name of the table
   * @param columnInfosMap the mapping from the columnNames to their columnInfos
   */
  void alterColumnsDataTypeIcebergTable(String tableName, Map<String, ColumnInfos> columnInfosMap);

  /**
   * Alter iceberg table to add columns according to a map from columnNames to their types
   *
   * @param tableName the name of the table
   * @param columnInfosMap the mapping from the columnNames to their columnInfos
   */
  void appendColumnsToIcebergTable(String tableName, Map<String, ColumnInfos> columnInfosMap);

  /**
   * Alter table to drop non-nullability of a list of columns
   *
   * @param tableName the name of the table
   * @param columnNames the list of columnNames
   */
  void alterNonNullableColumns(String tableName, List<String> columnNames);

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

  /**
   * drop snowpipe
   *
   * @param pipeName pipe name
   */
  void dropPipe(String pipeName);

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
   * Append a VARIANT type column "RECORD_METADATA" to the table if it is not present.
   *
   * <p>This method is only called when schematization is enabled
   *
   * @param tableName table name
   */
  void appendMetaColIfNotExist(String tableName);

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
   * Alter the RECORD_METADATA column to be of VARIANT type for iceberg tables.
   *
   * @param tableName iceberg table name
   */
  void initializeMetadataColumnTypeForIceberg(String tableName);

  /**
   * Add the RECORD_METADATA column as VARIANT to the iceberg table if it does not exist.
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
