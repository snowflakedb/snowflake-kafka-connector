package com.snowflake.kafka.connector.internal;

import java.util.List;

public interface SnowflakeTelemetryService
{

  /**
   * set app name
   * @param name app name
   */
  void setAppName(String name);

  /**
   * Event of connector start
   * @param startTime task start time
   * @param numTopics number of topics
   * @param numMappedTables number of tables
   * @param maxTasks max number of tasks
   * @param appName instance name
   */
  void reportKafkaStart(long startTime, int numTopics, int numMappedTables,
                        int maxTasks, String appName);

  /**
   * Event of connector start
   * @param startTime task start time
   * @param numTopics number of topics
   * @param numMappedTables number of tables
   * @param maxTasks max number of tasks
   */
  void reportKafkaStart(long startTime, int numTopics, int numMappedTables,
                        int maxTasks);

  /**
   * Event of connector stop
   *
   * @param startTime start timestamp
   * @param appName   application name
   */
  void reportKafkaStop(long startTime, String appName);

  /**
   * Event of connector stop
   *
   * @param startTime start timestamp
   */
  void reportKafkaStop(long startTime);

  /**
   * Event of a fatal error in the connector
   *
   * @param errorDetail error message
   * @param appName     application name
   */
  void reportKafkaFatalError(String errorDetail, String appName);

  /**
   * Event of a fatal error in the connector
   *
   * @param errorDetail error message
   */
  void reportKafkaFatalError(String errorDetail);

  /**
   * Event of a non fatal error in the connector
   *
   * @param errorDetail error message
   * @param appName     application name
   */
  void reportKafkaNonFatalError(String errorDetail, String appName);

  /**
   * Event of a non fatal error in the connector
   *
   * @param errorDetail error message
   */
  void reportKafkaNonFatalError(String errorDetail);


  /**
   * report connector usage
   *
   * @param startTime    start time of reported period
   * @param endTime      end time of reported period
   * @param recordNumber number of records sent to SF
   * @param byteNumber   number of bytes sent to SF
   * @param appName      application name
   */
  void reportKafkaUsage(long startTime, long endTime, long recordNumber, long byteNumber, String appName);

  /**
   * report connector usage
   *
   * @param startTime    start time of reported period
   * @param endTime      end time of reported period
   * @param recordNumber number of records sent to SF
   * @param byteNumber   number of bytes sent to SF
   */
  void reportKafkaUsage(long startTime, long endTime, long recordNumber, long byteNumber);

  /**
   * report table creation
   *
   * @param tableName table name
   * @param appName   application name
   */
  void reportKafkaCreateTable(String tableName, String appName);

  /**
   * report table creation
   *
   * @param tableName table name
   */
  void reportKafkaCreateTable(String tableName);

  /**
   * report table creation
   *
   * @param tableName table name
   * @param appName   application name
   */
  void reportKafkaReuseTable(String tableName, String appName);

  /**
   * report table creation
   *
   * @param tableName table name
   */
  void reportKafkaReuseTable(String tableName);

  /**
   * report stage creation
   *
   * @param stageName stage name
   * @param appName   application name
   */
  void reportKafkaCreateStage(String stageName, String appName);

  /**
   * report stage creation
   *
   * @param stageName stage name
   */
  void reportKafkaCreateStage(String stageName);

  /**
   * report stage reuse
   *
   * @param stageName stage name
   * @param appName   application name
   */
  void reportKafkaReuseStage(String stageName, String appName);

  /**
   * report stage reuse
   *
   * @param stageName stage name
   */
  void reportKafkaReuseStage(String stageName);

  /**
   * report pipe creation
   *
   * @param pipeName  pipe name
   * @param stageName stage name
   * @param tableName table name
   * @param appName   application name
   */
  void reportKafkaCreatePipe(String tableName, String stageName, String pipeName, String appName);

  /**
   * report pipe creation
   *
   * @param pipeName  pipe name
   * @param stageName stage name
   * @param tableName table name
   */
  void reportKafkaCreatePipe(String tableName, String stageName, String pipeName);

  /**
   * report file failures
   *
   * @param tableName table name
   * @param stageName stage name
   * @param filenames a list of file names
   * @param appName   application name
   */
  void reportKafkaFileFailure(String tableName, String stageName, List<String> filenames, String appName);

  /**
   * report file failures
   *
   * @param tableName table name
   * @param stageName stage name
   * @param filenames a list of file names
   */
  void reportKafkaFileFailure(String tableName, String stageName, List<String> filenames);

}
