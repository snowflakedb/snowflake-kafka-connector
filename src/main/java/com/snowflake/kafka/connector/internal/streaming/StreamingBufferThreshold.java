package com.snowflake.kafka.connector.internal.streaming;

import java.util.concurrent.TimeUnit;
import org.apache.kafka.connect.sink.SinkRecord;

/**
 * Helper class Associated to Streaming Snowpipe runtime of Kafka Connect which can help to identify
 * if there is a need to flush the buffered records.
 *
 * <p>Please note: Flush entails invoking {@link
 * net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel#insertRows(Iterable, String)} API
 * with suitable buffered rows.
 */
public final class StreamingBufferThreshold {
  // Buffer flush thresholds set in connector
  // Set in config (Time based flush) in seconds
  /**
   * Set in config (Time based flush) in seconds
   *
   * <p>Config parameter: {@link
   * com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig#BUFFER_FLUSH_TIME_SEC}
   */
  private final long flushTimeThresholdSeconds;

  /**
   * Set in config (buffer size based flush) in bytes
   *
   * <p>Config parameter: {@link
   * com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig#BUFFER_SIZE_BYTES}
   */
  private final long bufferSizeThresholdBytes;

  /**
   * Set in config (Threshold before we call insertRows API) corresponds to # of records in kafka
   *
   * <p>Config parameter: {@link
   * com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig#BUFFER_COUNT_RECORDS}
   */
  private final long bufferKafkaRecordCountThreshold;

  private final long SECOND_TO_MILLIS = TimeUnit.SECONDS.toMillis(1);

  public StreamingBufferThreshold(
      long flushTimeThresholdSeconds,
      long bufferSizeThresholdBytes,
      long bufferKafkaRecordCountThreshold) {
    this.flushTimeThresholdSeconds = flushTimeThresholdSeconds;
    this.bufferSizeThresholdBytes = bufferSizeThresholdBytes;
    this.bufferKafkaRecordCountThreshold = bufferKafkaRecordCountThreshold;
  }

  /**
   * Returns true if size of current buffer is more than threshold provided (Both in bytes).
   *
   * <p>Threshold is config parameter: {@link
   * com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig#BUFFER_SIZE_BYTES}
   *
   * @param currentBufferSizeInBytes current size of buffer in bytes
   * @return true if bytes threshold has reached.
   */
  public boolean isFlushBufferedBytesBased(final long currentBufferSizeInBytes) {
    return currentBufferSizeInBytes >= bufferSizeThresholdBytes;
  }

  /**
   * Returns true if number of ({@link SinkRecord})s in current buffer is more than threshold
   * provided.
   *
   * <p>Threshold is config parameter: {@link
   * com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig#BUFFER_COUNT_RECORDS}
   *
   * @param currentBufferedRecordCount current size of buffer in terms of number of kafka records
   * @return true if number of kafka threshold has reached in buffer.
   */
  public boolean isFlushBufferedRecordCountBased(final long currentBufferedRecordCount) {
    return currentBufferedRecordCount != 0
        && currentBufferedRecordCount >= bufferKafkaRecordCountThreshold;
  }

  /**
   * If difference between current time and previous flush time is more than threshold return true.
   *
   * @param previousFlushTimeStampMs passed from TopicPartitionChannel to know when was previous
   *     partition flushed.
   * @return true if time based threshold has reached.
   */
  public boolean isFlushTimeBased(final long previousFlushTimeStampMs) {
    final long currentTimeMs = System.currentTimeMillis();
    return (currentTimeMs - previousFlushTimeStampMs)
        >= (this.flushTimeThresholdSeconds * SECOND_TO_MILLIS);
  }

  /** Get flush time threshold in seconds */
  public long getFlushTimeThresholdSeconds() {
    return flushTimeThresholdSeconds;
  }
}
