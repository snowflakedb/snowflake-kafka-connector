package com.snowflake.kafka.connector.internal;

import com.snowflake.kafka.connector.internal.schemaevolution.ColumnInfos;
import com.snowflake.kafka.connector.internal.streaming.v2.migration.Ssv1MigrationResponse;
import com.snowflake.kafka.connector.internal.telemetry.SnowflakeTelemetryService;
import java.sql.Connection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public interface SnowflakeConnectionService {
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

  /**
   * @return telemetry client
   */
  SnowflakeTelemetryService getTelemetryClient();

  /** Close Connection */
  void close();

  /**
   * @return true is connection is closed
   */
  boolean isClosed();

  /**
   * @return name of Kafka Connector instance
   */
  String getConnectorName();

  /**
   * @return the raw jdbc connection
   */
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

  /**
   * Add columns to an existing table via ALTER TABLE ... ADD COLUMN IF NOT EXISTS.
   *
   * @param tableName table name
   * @param columnInfosMap map of column name to ColumnInfos (type + comment)
   */
  void appendColumnsToTable(String tableName, Map<String, ColumnInfos> columnInfosMap);

  /**
   * Drop NOT NULL constraints on columns via ALTER TABLE ... ALTER ... DROP NOT NULL.
   *
   * @param tableName table name
   * @param columnNames list of column names to make nullable
   */
  void alterNonNullableColumns(String tableName, List<String> columnNames);

  /**
   * Check whether the user has the role privilege to do schema evolution and whether the schema
   * evolution option is enabled on the table.
   *
   * @param tableName table name
   * @param role the role of the user
   * @return whether schema evolution has the required permission to be performed
   */
  boolean shouldEvolveSchema(String tableName, String role);

  /**
   * Check whether the given table is an iceberg table.
   *
   * @param tableName table name
   * @return true if the table is an iceberg table, false otherwise
   */
  boolean isIcebergTable(String tableName);

  /**
   * Check whether the given table has ERROR_LOGGING enabled via SHOW TABLES.
   *
   * @param tableName table name
   * @return true if error_logging is "ON", false otherwise or if the column is not present
   */
  boolean hasErrorLoggingEnabled(String tableName);

  /**
   * Calls SYSTEM$MIGRATE_SSV1_CHANNEL_OFFSET to migrate the committed offset from an SSv1 channel
   * to an SSv2 channel. The system function reads the SSv1 offset and writes it directly to the
   * SSv2 channel in FDB.
   *
   * @param tableName unqualified table name (the JDBC session's database/schema are used)
   * @param ssv1ChannelName SSv1 channel name ({topic}_{partition} or
   *     {connectorName}_{topic}_{partition})
   * @param ssv2ChannelName SSv2 channel name ({connectorName}_{topic}_{partition})
   * @param pipeName SSv2 pipe name
   * @return the parsed {@link Ssv1MigrationResponse} indicating whether the channel was found and
   *     (if so) the migrated offset value
   * @throws RuntimeException if the system function call fails (SQL error, unexpected response)
   */
  Ssv1MigrationResponse migrateSsv1ChannelOffset(
      String tableName, String ssv1ChannelName, String ssv2ChannelName, String pipeName);

  /**
   * Calls SYSTEM$GET_KC_ADVISORY_MESSAGES with the given request JSON (e.g.
   * {@code {"connectorVersion":"4.1.0"}}) and returns the advisory messages GS wants logged.
   * Fails safe: returns an empty list on any error (old GS without the function, empty policy,
   * parse failure) — never throws.
   */
  java.util.List<com.snowflake.kafka.connector.internal.advisory.AdvisoryMessage>
      getKcAdvisoryMessages(String requestJson);
}
