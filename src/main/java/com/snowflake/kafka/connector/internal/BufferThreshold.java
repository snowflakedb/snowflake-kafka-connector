package com.snowflake.kafka.connector.internal;

import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.BUFFER_FLUSH_TIME_SEC_MIN;
import static com.snowflake.kafka.connector.internal.streaming.StreamingUtils.STREAMING_BUFFER_FLUSH_TIME_MINIMUM_SEC;

import com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig;
import com.snowflake.kafka.connector.internal.streaming.IngestionMethodConfig;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class Associated to runtime of Kafka Connect which can help to identify if there is a need
 * to flush the buffered records.
 */
public abstract class BufferThreshold {

  private static final Logger LOGGER = LoggerFactory.getLogger(BufferThreshold.class);

  // What ingestion method is defined in connector.
  private final IngestionMethodConfig ingestionMethodConfig;

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

  public BufferThreshold(
      IngestionMethodConfig ingestionMethodConfig,
      long flushTimeThresholdSeconds,
      long bufferSizeThresholdBytes,
      long bufferKafkaRecordCountThreshold) {
    this.ingestionMethodConfig = ingestionMethodConfig;
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
   * @param previousFlushTimeStampMs when were previous buffered records were flushed into internal
   *     stage for snowpipe based implementation or previous buffered records were sent in
   *     insertRows API of Streaming Snowpipe.
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

  /**
   * Check if provided snowflake kafka connector buffer properties are within permissible values
   *
   * @param providedSFConnectorConfig provided by customer
   * @return true if valid.
   */
  public static boolean validateBufferThreshold(
      Map<String, String> providedSFConnectorConfig, IngestionMethodConfig ingestionMethodConfig) {
    if (!providedSFConnectorConfig.containsKey(
        SnowflakeSinkConnectorConfig.BUFFER_FLUSH_TIME_SEC)) {
      LOGGER.error(
          Logging.logMessage("{} is empty", SnowflakeSinkConnectorConfig.BUFFER_FLUSH_TIME_SEC));
      return false;
    } else {
      try {
        long time =
            Long.parseLong(
                providedSFConnectorConfig.get(SnowflakeSinkConnectorConfig.BUFFER_FLUSH_TIME_SEC));
        if (ingestionMethodConfig.equals(IngestionMethodConfig.SNOWPIPE)) {
          if (time < BUFFER_FLUSH_TIME_SEC_MIN) {
            LOGGER.error(
                (Logging.logMessage(
                    "{} is {}, it should be greater than {}",
                    SnowflakeSinkConnectorConfig.BUFFER_FLUSH_TIME_SEC,
                    time,
                    BUFFER_FLUSH_TIME_SEC_MIN)));
            return false;
          }
        } else {
          if (time < STREAMING_BUFFER_FLUSH_TIME_MINIMUM_SEC) {
            LOGGER.error(
                (Logging.logMessage(
                    "{} is {}, it should be greater than {}",
                    SnowflakeSinkConnectorConfig.BUFFER_FLUSH_TIME_SEC,
                    time,
                    STREAMING_BUFFER_FLUSH_TIME_MINIMUM_SEC)));
            return false;
          }
        }
      } catch (Exception e) {
        LOGGER.error(
            Logging.logMessage(
                "{} should be an integer", SnowflakeSinkConnectorConfig.BUFFER_FLUSH_TIME_SEC));
        return false;
      }
    }

    // verify buffer.count.records
    if (!providedSFConnectorConfig.containsKey(SnowflakeSinkConnectorConfig.BUFFER_COUNT_RECORDS)) {
      LOGGER.error(
          Logging.logMessage("{} is empty", SnowflakeSinkConnectorConfig.BUFFER_COUNT_RECORDS));
      return false;
    } else {
      try {
        long num =
            Long.parseLong(
                providedSFConnectorConfig.get(SnowflakeSinkConnectorConfig.BUFFER_COUNT_RECORDS));
        if (num < 0) {
          LOGGER.error(
              Logging.logMessage(
                  "{} is {}, it should not be negative",
                  SnowflakeSinkConnectorConfig.BUFFER_COUNT_RECORDS,
                  num));
          return false;
        }
      } catch (Exception e) {
        LOGGER.error(
            Logging.logMessage(
                "{} should be an integer", SnowflakeSinkConnectorConfig.BUFFER_COUNT_RECORDS));
        return false;
      }
    }

    // verify buffer.size.bytes
    if (providedSFConnectorConfig.containsKey(SnowflakeSinkConnectorConfig.BUFFER_SIZE_BYTES)) {
      try {
        long bsb =
            Long.parseLong(
                providedSFConnectorConfig.get(SnowflakeSinkConnectorConfig.BUFFER_SIZE_BYTES));
        if (bsb < SnowflakeSinkConnectorConfig.BUFFER_SIZE_BYTES_MIN) // 1 byte
        {
          LOGGER.error(
              Logging.logMessage(
                  "{} is too low at {}. It must be {} or greater.",
                  SnowflakeSinkConnectorConfig.BUFFER_SIZE_BYTES,
                  bsb,
                  SnowflakeSinkConnectorConfig.BUFFER_SIZE_BYTES_MIN));
          return false;
        }
      } catch (Exception e) {
        LOGGER.error(
            Logging.logMessage(
                "{} should be an integer", SnowflakeSinkConnectorConfig.BUFFER_SIZE_BYTES));
        return false;
      }
    } else {
      LOGGER.error(
          Logging.logMessage("{} is empty", SnowflakeSinkConnectorConfig.BUFFER_SIZE_BYTES));
      return false;
    }
    return true;
  }
}
