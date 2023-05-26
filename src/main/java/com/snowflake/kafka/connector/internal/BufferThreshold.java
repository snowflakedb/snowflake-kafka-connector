package com.snowflake.kafka.connector.internal;

import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.BUFFER_FLUSH_TIME_SEC_MIN;
import static com.snowflake.kafka.connector.internal.streaming.StreamingUtils.STREAMING_BUFFER_FLUSH_TIME_MINIMUM_SEC;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableMap;
import com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig;
import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.internal.streaming.IngestionMethodConfig;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Helper class associated to runtime of Kafka Connect which can help to identify if there is a need
 * to flush the buffered records.
 */
public abstract class BufferThreshold {
  private static final KCLogger LOGGER = new KCLogger(BufferThreshold.class.getName());

  // What ingestion method is defined in connector.
  private final IngestionMethodConfig ingestionMethodConfig;

  /**
   * Time based buffer flush threshold in seconds. Corresponds to the duration since last kafka
   * flush Set in config
   *
   * <p>Config parameter: {@link
   * com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig#BUFFER_FLUSH_TIME_SEC}
   */
  private final long bufferFlushTimeThreshold;

  /**
   * Size based buffer flush threshold in bytes. Corresponds to the buffer size in kafka Set in
   * config
   *
   * <p>Config parameter: {@link
   * com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig#BUFFER_SIZE_BYTES}
   */
  private final long bufferByteSizeThreshold;

  /**
   * Count based buffer flush threshold. Corresponds to the record count in kafka Set in config.
   *
   * <p>Config parameter: {@link
   * com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig#BUFFER_COUNT_RECORDS}
   */
  private final long bufferRecordCountThreshold;

  private final long SECOND_TO_MILLIS = TimeUnit.SECONDS.toMillis(1);

  /**
   * Public constructor
   *
   * @param ingestionMethodConfig enum accepting ingestion method (selected in config json)
   * @param bufferFlushTimeThreshold flush time threshold in seconds given in connector config
   * @param bufferByteSizeThreshold buffer size threshold in bytes given in connector config
   * @param bufferRecordCountThreshold record count threshold in number of kafka records given in
   *     connector config
   */
  public BufferThreshold(
      IngestionMethodConfig ingestionMethodConfig,
      long bufferFlushTimeThreshold,
      long bufferByteSizeThreshold,
      long bufferRecordCountThreshold) {
    this.ingestionMethodConfig = ingestionMethodConfig;
    this.bufferFlushTimeThreshold = bufferFlushTimeThreshold;
    this.bufferByteSizeThreshold = bufferByteSizeThreshold;
    this.bufferRecordCountThreshold = bufferRecordCountThreshold;
  }

  /**
   * Returns true the buffer should flush based on the current buffer byte size
   *
   * <p>Threshold is config parameter: {@link
   * com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig#BUFFER_SIZE_BYTES}
   *
   * @param currBufferByteSize current size of buffer in bytes
   * @return true if the currByteSize > configByteSizeThreshold
   */
  public boolean shouldFlushOnBufferByteSize(final long currBufferByteSize) {
    return currBufferByteSize >= bufferByteSizeThreshold;
  }

  /**
   * Returns true the buffer should flush based on the current buffer record count
   *
   * <p>Threshold is config parameter: {@link
   * com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig#BUFFER_COUNT_RECORDS}
   *
   * @param currentBufferedRecordCount current size of buffer in number of kafka records
   * @return true if the currRecordCount > configRecordCountThreshold
   */
  public boolean shouldFlushOnBufferRecordCount(final long currentBufferedRecordCount) {
    return currentBufferedRecordCount != 0
        && currentBufferedRecordCount >= bufferRecordCountThreshold;
  }

  /**
   * Returns true the buffer should flush based on the last flush time
   *
   * <p>Threshold is config parameter: {@link
   * com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig#BUFFER_FLUSH_TIME_SEC}
   *
   * @param previousFlushTimeStampMs when the previous buffered records flushed
   * @return true if currentTime - previousTime > configTimeThreshold
   */
  public boolean shouldFlushOnBufferTime(final long previousFlushTimeStampMs) {
    final long currentTimeMs = System.currentTimeMillis();
    return (currentTimeMs - previousFlushTimeStampMs)
        >= (this.bufferFlushTimeThreshold * SECOND_TO_MILLIS);
  }

  /** @return Get flush time threshold in seconds */
  public long getFlushTimeThresholdSeconds() {
    return this.bufferFlushTimeThreshold;
  }

  /**
   * Check if provided snowflake kafka connector buffer properties are within permissible values.
   *
   * <p>This method invokes three verifiers - Time based threshold, buffer size and buffer count
   * threshold.
   *
   * @param providedSFConnectorConfig provided by customer
   * @param ingestionMethodConfig ingestion method used. Check {@link IngestionMethodConfig}
   * @return invalid config parameters, if exists
   */
  public static ImmutableMap<String, String> validateBufferThreshold(
      Map<String, String> providedSFConnectorConfig, IngestionMethodConfig ingestionMethodConfig) {
    Map<String, String> invalidConfigParams = new HashMap<>();
    invalidConfigParams.putAll(
        verifyBufferFlushTimeThreshold(providedSFConnectorConfig, ingestionMethodConfig));
    invalidConfigParams.putAll(verifyBufferCountThreshold(providedSFConnectorConfig));
    invalidConfigParams.putAll(verifyBufferBytesThreshold(providedSFConnectorConfig));
    return ImmutableMap.copyOf(invalidConfigParams);
  }

  private static ImmutableMap<String, String> verifyBufferFlushTimeThreshold(
      Map<String, String> providedSFConnectorConfig, IngestionMethodConfig ingestionMethodConfig) {
    Map<String, String> invalidConfigParams = new HashMap();

    if (!providedSFConnectorConfig.containsKey(
        SnowflakeSinkConnectorConfig.BUFFER_FLUSH_TIME_SEC)) {
      invalidConfigParams.put(
          SnowflakeSinkConnectorConfig.BUFFER_FLUSH_TIME_SEC,
          Utils.formatString(
              "Config {} is empty", SnowflakeSinkConnectorConfig.BUFFER_FLUSH_TIME_SEC));
    } else {
      String providedFlushTimeSecondsInStr =
          providedSFConnectorConfig.get(SnowflakeSinkConnectorConfig.BUFFER_FLUSH_TIME_SEC);
      try {
        long providedFlushTimeSecondsInConfig = Long.parseLong(providedFlushTimeSecondsInStr);

        // select appropriate threshold based on ingestion method.
        long thresholdTimeToCompare =
            ingestionMethodConfig.equals(IngestionMethodConfig.SNOWPIPE)
                ? BUFFER_FLUSH_TIME_SEC_MIN
                : STREAMING_BUFFER_FLUSH_TIME_MINIMUM_SEC;
        if (providedFlushTimeSecondsInConfig < thresholdTimeToCompare) {
          invalidConfigParams.put(
              SnowflakeSinkConnectorConfig.BUFFER_FLUSH_TIME_SEC,
              Utils.formatString(
                  "{} is {}, it should be greater than {}",
                  SnowflakeSinkConnectorConfig.BUFFER_FLUSH_TIME_SEC,
                  providedFlushTimeSecondsInConfig,
                  thresholdTimeToCompare));
        }
      } catch (NumberFormatException e) {
        invalidConfigParams.put(
            SnowflakeSinkConnectorConfig.BUFFER_FLUSH_TIME_SEC,
            Utils.formatString(
                "{} should be an integer. Invalid integer was provided:{}",
                SnowflakeSinkConnectorConfig.BUFFER_FLUSH_TIME_SEC,
                providedFlushTimeSecondsInStr));
      }
    }

    return ImmutableMap.copyOf(invalidConfigParams);
  }

  private static ImmutableMap<String, String> verifyBufferBytesThreshold(
      Map<String, String> providedSFConnectorConfig) {
    Map<String, String> invalidConfigParams = new HashMap();

    // verify buffer.size.bytes
    if (!providedSFConnectorConfig.containsKey(SnowflakeSinkConnectorConfig.BUFFER_SIZE_BYTES)) {
      invalidConfigParams.put(
          SnowflakeSinkConnectorConfig.BUFFER_SIZE_BYTES,
          Utils.formatString("Config {} is empty", SnowflakeSinkConnectorConfig.BUFFER_SIZE_BYTES));
    } else {
      final String providedBufferSizeBytesStr =
          providedSFConnectorConfig.get(SnowflakeSinkConnectorConfig.BUFFER_SIZE_BYTES);
      try {
        long providedBufferSizeBytesConfig = Long.parseLong(providedBufferSizeBytesStr);
        if (providedBufferSizeBytesConfig
            < SnowflakeSinkConnectorConfig.BUFFER_SIZE_BYTES_MIN) // 1 byte
        {
          invalidConfigParams.put(
              SnowflakeSinkConnectorConfig.BUFFER_SIZE_BYTES,
              Utils.formatString(
                  "{} is too low at {}. It must be {} or greater.",
                  SnowflakeSinkConnectorConfig.BUFFER_SIZE_BYTES,
                  providedBufferSizeBytesConfig,
                  SnowflakeSinkConnectorConfig.BUFFER_SIZE_BYTES_MIN));
        }
      } catch (NumberFormatException e) {
        invalidConfigParams.put(
            SnowflakeSinkConnectorConfig.BUFFER_SIZE_BYTES,
            Utils.formatString(
                "Config {} should be an integer. Provided:{}",
                SnowflakeSinkConnectorConfig.BUFFER_SIZE_BYTES,
                providedBufferSizeBytesStr));
      }
    }

    return ImmutableMap.copyOf(invalidConfigParams);
  }

  private static ImmutableMap<String, String> verifyBufferCountThreshold(
      Map<String, String> providedSFConnectorConfig) {
    Map<String, String> invalidConfigParams = new HashMap();

    // verify buffer.count.records
    if (!providedSFConnectorConfig.containsKey(SnowflakeSinkConnectorConfig.BUFFER_COUNT_RECORDS)) {
      invalidConfigParams.put(
          SnowflakeSinkConnectorConfig.BUFFER_COUNT_RECORDS,
          Utils.formatString(
              "Config {} is empty", SnowflakeSinkConnectorConfig.BUFFER_COUNT_RECORDS));
    } else {
      final String providedBufferCountRecordsStr =
          providedSFConnectorConfig.get(SnowflakeSinkConnectorConfig.BUFFER_COUNT_RECORDS);
      try {
        long providedBufferCountRecords = Long.parseLong(providedBufferCountRecordsStr);
        if (providedBufferCountRecords <= 0) {
          invalidConfigParams.put(
              SnowflakeSinkConnectorConfig.BUFFER_COUNT_RECORDS,
              Utils.formatString(
                  "Config {} is {}, it should at least 1",
                  SnowflakeSinkConnectorConfig.BUFFER_COUNT_RECORDS,
                  providedBufferCountRecords));
        }
      } catch (NumberFormatException e) {
        invalidConfigParams.put(
            SnowflakeSinkConnectorConfig.BUFFER_COUNT_RECORDS,
            Utils.formatString(
                "Config {} should be a positive integer. Provided:{}",
                SnowflakeSinkConnectorConfig.BUFFER_COUNT_RECORDS,
                providedBufferCountRecordsStr));
      }
    }

    return ImmutableMap.copyOf(invalidConfigParams);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("bufferFlushTimeThreshold", this.bufferFlushTimeThreshold)
        .add("bufferByteSizeThreshold", this.bufferByteSizeThreshold)
        .add("bufferRecordCountThreshold", this.bufferRecordCountThreshold)
        .toString();
  }
}
