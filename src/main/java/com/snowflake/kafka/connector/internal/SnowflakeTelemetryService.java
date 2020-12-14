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
   * set task id
   * @param taskID task id
   */
  void setTaskID(String taskID);

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
   * report connector pipe status
   *
   * @param pipeStatus   SnowflakePipeStatus object
   * @param isClosing    is the pipe closing
   */
  void reportKafkaPipeUsage(final SnowflakeTelemetryPipeStatus pipeStatus, boolean isClosing);

  /**
   * report connector pipe start
   *
   * @param objectCreation   SnowflakeObjectCreation object
   */
  void reportKafkaPipeStart(final SnowflakeTelemetryPipeCreation objectCreation);
}
