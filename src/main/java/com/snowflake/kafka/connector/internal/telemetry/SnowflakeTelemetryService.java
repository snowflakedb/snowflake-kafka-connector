package com.snowflake.kafka.connector.internal.telemetry;

import java.util.Map;

public interface SnowflakeTelemetryService {

  /**
   * set app name
   *
   * @param name app name
   */
  void setAppName(String name);

  /**
   * set task id
   *
   * @param taskID task id
   */
  void setTaskID(String taskID);

  /**
   * Event of connector start
   *
   * @param startTime task start time
   * @param userProvidedConfig max number of tasks
   */
  void reportKafkaConnectStart(long startTime, Map<String, String> userProvidedConfig);

  /**
   * Event of connector stop
   *
   * @param startTime start timestamp
   */
  void reportKafkaConnectStop(long startTime);

  /**
   * Event of a fatal error in the connector
   *
   * @param errorDetail error message
   */
  void reportKafkaConnectFatalError(String errorDetail);

  /**
   * report connector's partition usage.
   *
   * <p>It depends on the underlying implementation of Kafka connector, i.e weather it is Snowpipe
   * or Snowpipe Streaming
   *
   * @param partitionStatus SnowflakePipeStatus object
   * @param isClosing is the underlying pipe/channel closing
   */
  void reportKafkaPartitionUsage(
      final SnowflakeTelemetryBasicInfo partitionStatus, boolean isClosing);

  /**
   * report connector partition start
   *
   * @param objectCreation SnowflakeObjectCreation object
   */
  void reportKafkaPartitionStart(final SnowflakeTelemetryBasicInfo objectCreation);
}
