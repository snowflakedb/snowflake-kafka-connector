package com.snowflake.kafka.connector.internal;

import java.sql.Connection;
import java.util.List;

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
  void put(String stageName, String fileName, String content);

  /**
   * put a file to stage. Cache credential for AWS and Azure storage. Don't cache for GCS.
   *
   * @param fileName file name
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
}
