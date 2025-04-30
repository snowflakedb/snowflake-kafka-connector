package com.snowflake.kafka.connector.internal;

import com.codahale.metrics.MetricRegistry;
import com.google.common.annotations.VisibleForTesting;
import com.snowflake.kafka.connector.dlq.KafkaRecordErrorReporter;
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

  /* Set Error reporter which can be used to send records to DLQ (Dead Letter Queue) */
  default void setErrorReporter(KafkaRecordErrorReporter kafkaRecordErrorReporter) {}

  /* Set the SinkTaskContext object available from SinkTask. It contains utility methods to from Kafka Connect Runtime. */
  default void setSinkTaskContext(SinkTaskContext sinkTaskContext) {}

  /* Get metric registry of an associated partition */
  @VisibleForTesting
  Optional<MetricRegistry> getMetricRegistry(final String partitionIdentifier);
}
