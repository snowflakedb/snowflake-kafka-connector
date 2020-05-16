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
   * @param maxTasks max number of tasks
   */
  void reportKafkaStart(long startTime, int maxTasks);

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
   */
  void reportKafkaFatalError(String errorDetail);

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
   */
  void reportKafkaUsage(long startTime, long endTime, long recordNumber, long byteNumber);

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
   */
  void reportKafkaReuseTable(String tableName);

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
   */
  void reportKafkaReuseStage(String stageName);

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
   */
  void reportKafkaFileFailure(String tableName, String stageName, List<String> filenames);

  /**
   * report Snowflake throttle
   *
   * @param errorDetail error message
   * @param iteration slept time before execution
   */
  void reportKafkaSnowflakeThrottle(final String errorDetail, int iteration);
}
