package com.snowflake.kafka.connector.internal;

import com.snowflake.kafka.connector.internal.streaming.ChannelMigrateOffsetTokenResponseDTO;
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
   * create a snowpipe if not exists
   *
   * @param pipeName pipe name
   * @param tableName table name
   * @param stageName stage name
   */
  void createPipe(String tableName, String stageName, String pipeName);

  /**
   * create a stage
   *
   * @param stageName stage name
   * @param overwrite if true, execute "create or replace stage" statement; otherwise, run "create
   *     stage if not exists"
   */
  void createStage(String stageName, boolean overwrite);

  /**
   * create stage if not exists
   *
   * @param stageName stage name
   */
  void createStage(String stageName);

  /**
   * check table existence
   *
   * @param tableName table name
   * @return true if table exists, false otherwise
   */
  boolean tableExist(String tableName);

  /**
   * check stage existence
   *
   * @param stageName stage name
   * @return true if stage exists, false otherwise
   */
  boolean stageExist(String stageName);

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
   * Examine all file names matches our pattern
   *
   * @param stageName stage name
   * @return true is stage is compatible, false if stage does not exist or file name invalid
   */
  boolean isStageCompatible(String stageName);

  /**
   * check snowpipe definition
   *
   * @param pipeName pipe name
   * @param tableName table name
   * @param stageName stage name
   * @return true if definition is correct, false if it is incorrect or pipe does not exists
   */
  boolean isPipeCompatible(String tableName, String stageName, String pipeName);

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

  /**
   * drop stage if the given stage is empty
   *
   * @param stageName stage name
   * @return true if stage dropped, otherwise false
   */
  boolean dropStageIfEmpty(String stageName);

  /**
   * drop stage
   *
   * @param stageName stage name
   */
  void dropStage(String stageName);

  /**
   * purge files from given stage
   *
   * @param stageName stage name
   * @param files list of file names
   */
  void purgeStage(String stageName, List<String> files);

  void moveToTableStage(String tableName, String stageName, List<String> files);

  /**
   * move all files on stage related to given pipe to table stage
   *
   * @param stageName stage name
   * @param tableName table name
   * @param prefix prefix name
   */
  void moveToTableStage(String tableName, String stageName, String prefix);

  /**
   * list a stage and return a list of file names contained in given subdirectory
   *
   * @param stageName stage name
   * @param prefix prefix name
   * @param isTableStage true if it is a table stage
   * @return a list of file names in given subdirectory, file name = "{prefix}filename"
   */
  List<String> listStage(String stageName, String prefix, boolean isTableStage);

  /**
   * list a non table stage and return a list of file names contained in given subdirectory
   *
   * @param stageName stage name
   * @param prefix prefix name
   * @return a list of file names in given subdirectory, file name = "{prefix}filename"
   */
  List<String> listStage(String stageName, String prefix);

  /**
   * put a file to stage
   *
   * @param fileName file name
   * @param content file content
   * @param stageName stage name
   */
  @Deprecated
  void put(String stageName, String fileName, String content);

  /**
   * put a file to stage. Cache credential for AWS, Azure and GCS storage.
   *
   * @param fileName file name which is of format
   *     (stageName/connectorName/tableName/partition/offsetStart_offsetEnd_timestamp)
   *     <p>Check {@link FileNameUtils to find out how fileName is determined}
   * @param content file content
   * @param stageName stage name
   */
  void putWithCache(final String stageName, final String fileName, final String content);

  /**
   * put a file to table stage
   *
   * @param tableName table name
   * @param fileName file name
   * @param content file content
   */
  void putToTableStage(String tableName, String fileName, byte[] content);

  /** @return telemetry client */
  SnowflakeTelemetryService getTelemetryClient();

  /** Close Connection */
  void close();

  /** @return true is connection is closed */
  boolean isClosed();

  /** @return name of Kafka Connector instance */
  String getConnectorName();

  /**
   * build ingest service instance for given stage and pipe
   *
   * @param stageName stage name
   * @param pipeName pipe name
   * @return an instance of SnowflakeIngestService
   */
  SnowflakeIngestionService buildIngestService(String stageName, String pipeName);

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
   * Migrate Streaming Channel offsetToken from a source Channel to a destination channel.
   *
   * <p>Here, source channel is the new channel format we created here * @see <a
   * href="https://github.com/snowflakedb/snowflake-kafka-connector/commit/3bf9106b22510c62068f7d2f7137b9e57989274c">Commit
   * </a>
   *
   * <p>Destination channel is the original Format containing only topicName and partition number.
   *
   * <p>We catch SQLException and JsonProcessingException that might happen in this method. The
   * caller should always open the Old Channel format. This old channel format will also be the key
   * to many HashMaps we will create. (For instance {@link
   * com.snowflake.kafka.connector.internal.streaming.SnowflakeSinkServiceV2#partitionsToChannel})
   *
   * @param tableName Name of the table
   * @param sourceChannelName sourceChannel name from where the offset Token will be fetched.
   *     Channel with this name will also be deleted.
   * @param destinationChannelName destinationChannel name to where the offsetToken will be copied
   *     over.
   * @return The DTO serialized from the migration response.
   */
  ChannelMigrateOffsetTokenResponseDTO migrateStreamingChannelOffsetToken(
      String tableName, String sourceChannelName, String destinationChannelName);

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
}
