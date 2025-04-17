package com.snowflake.kafka.connector.internal;

import com.codahale.metrics.MetricRegistry;
import com.google.common.annotations.VisibleForTesting;
import com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig;
import com.snowflake.kafka.connector.dlq.KafkaRecordErrorReporter;
import com.snowflake.kafka.connector.records.SnowflakeMetadataConfig;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;

/** Background service of data sink, responsible to create/drop pipe and ingest/purge files */
public interface SnowflakeSinkService {
  /**
   * Start the Task. This should handle any configuration parsing and one-time setup of the task.
   *
   * @param tableName destination table name
   * @param topicPartition TopicPartition passed from Kafka
   */
  void startPartition(String tableName, TopicPartition topicPartition);

  /**
   * Start a collection of TopicPartition. This should handle any configuration parsing and one-time
   * setup of the task.
   *
   * @param partitions collection of topic partitions
   * @param topic2Table a mapping from topic to table
   */
  void startPartitions(Collection<TopicPartition> partitions, Map<String, String> topic2Table);

  /**
   * call pipe to insert a collections of JSON records will trigger time based flush
   *
   * @param records record content
   */
  void insert(final Collection<SinkRecord> records);

  /**
   * call pipe to insert a JSON record will not trigger time based flush
   *
   * @param record record content
   */
  void insert(final SinkRecord record);

  /**
   * retrieve offset of last loaded record for given pipe name
   *
   * @param topicPartition topic and partition
   * @return offset, or -1 for empty
   */
  long getOffset(TopicPartition topicPartition);

  /**
   * get the number of partitions assigned to this sink service
   *
   * @return number of partitions
   */
  int getPartitionCount();

  /** used for testing only */
  void callAllGetOffset();

  /** terminate all tasks and close this service instance */
  void closeAll();

  /**
   * terminate given topic partitions
   *
   * @param partitions a list of topic partition
   */
  void close(Collection<TopicPartition> partitions);

  /**
   * close all cleaner thread but have no effect on sink service context
   *
   * <p>Note that calling this method does not perform synchronous cleanup in Snowpipe based
   * implementation
   */
  void stop();

  /**
   * retrieve sink service status
   *
   * @return true is closed
   */
  boolean isClosed();

  /**
   * change maximum number of record cached in buffer to control the flush rate, 0 for unlimited
   *
   * @param num a non negative long number represents number of record limitation
   */
  void setRecordNumber(long num);

  /**
   * change data size of buffer to control the flush rate, the minimum file size is controlled by
   * {@link SnowflakeSinkConnectorConfig#BUFFER_SIZE_BYTES_MIN}
   *
   * <p>Please note: The buffer size for Streaming and snowpipe doesnt necessarily translate to same
   * file size in Snowflake.
   *
   * <p>There is Java to UTF conversion followed by file compression in gzip.
   *
   * @param size a non negative long number represents data size limitation
   */
  void setFileSize(long size);

  /**
   * pass topic to table map to sink service
   *
   * @param topic2TableMap a String to String Map represents topic to table map
   */
  void setTopic2TableMap(Map<String, String> topic2TableMap);

  /**
   * change flush rate of sink service the minimum flush time is controlled by {@link
   * SnowflakeSinkConnectorConfig#BUFFER_FLUSH_TIME_SEC_MIN}
   *
   * @param time a non negative long number represents service flush time in seconds
   */
  void setFlushTime(long time);

  /**
   * set the metadata config to let user control what metadata to be collected into SF db
   *
   * @param configMap a String to String Map
   */
  void setMetadataConfig(SnowflakeMetadataConfig configMap);

  /* Set the behavior on what action to perform when this( @see com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig#BEHAVIOR_ON_NULL_VALUES_CONFIG ) config is set. */
  void setBehaviorOnNullValuesConfig(SnowflakeSinkConnectorConfig.BehaviorOnNullValues behavior);

  /* Should we emit Custom SF JMX Metrics to Mbean Server? If true (Default), we emit in form of SimpleMbeans */
  void setCustomJMXMetrics(boolean enableJMX);

  /* Only used in testing and verifying what was the passed value of this behavior from config to sink service*/
  SnowflakeSinkConnectorConfig.BehaviorOnNullValues getBehaviorOnNullValuesConfig();

  /* Set Error reporter which can be used to send records to DLQ (Dead Letter Queue) */
  default void setErrorReporter(KafkaRecordErrorReporter kafkaRecordErrorReporter) {}

  /* Set the SinkTaskContext object available from SinkTask. It contains utility methods to from Kafka Connect Runtime. */
  default void setSinkTaskContext(SinkTaskContext sinkTaskContext) {}

  /* Get metric registry of an associated partition */
  @VisibleForTesting
  Optional<MetricRegistry> getMetricRegistry(final String partitionIdentifier);
}
