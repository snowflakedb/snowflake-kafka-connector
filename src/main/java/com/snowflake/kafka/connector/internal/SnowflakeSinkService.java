package com.snowflake.kafka.connector.internal;

import com.snowflake.kafka.connector.records.SnowflakeMetadataConfig;
import java.util.Collection;
import java.util.Map;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;

/** Background service of data sink, responsible to create/drop pipe and ingest/purge files */
public interface SnowflakeSinkService {
  /**
   * Create new ingestion task from existing table and stage, try to reused existing pipe and
   * recovery previous task, otherwise, create a new pipe.
   *
   * @param tableName destination table name
   * @param topic topic name
   * @param partition partition index
   */
  void startTask(String tableName, String topic, int partition);

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

  /** close all cleaner thread but have no effect on sink service context */
  void setIsStoppedToTrue();

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
   * {@link com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig#BUFFER_SIZE_BYTES_MIN}
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
   * com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig#BUFFER_FLUSH_TIME_SEC_MIN}
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

  /** @return current number of record limitation */
  long getRecordNumber();

  /** @return current flush time in seconds */
  long getFlushTime();

  /** @return current file size limitation */
  long getFileSize();
}
