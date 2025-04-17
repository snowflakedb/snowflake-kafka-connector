package com.snowflake.kafka.connector.internal;

import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.BUFFER_FLUSH_TIME_SEC_MIN;

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

  /**
   * Time based buffer flush threshold in seconds. Corresponds to the duration since last kafka
   * flush Set in config
   *
   * <p>Config parameter: {@link SnowflakeSinkConnectorConfig#BUFFER_FLUSH_TIME_SEC}
   */
  private final long bufferFlushTimeThreshold;

  /**
   * Size based buffer flush threshold in bytes. Corresponds to the buffer size in kafka Set in
   * config
   *
   * <p>Config parameter: {@link SnowflakeSinkConnectorConfig#BUFFER_SIZE_BYTES}
   */
  private final long bufferByteSizeThreshold;

  /**
   * Count based buffer flush threshold. Corresponds to the record count in kafka Set in config.
   *
   * <p>Config parameter: {@link SnowflakeSinkConnectorConfig#BUFFER_COUNT_RECORDS}
   */
  private final long bufferRecordCountThreshold;

  private final long SECOND_TO_MILLIS = TimeUnit.SECONDS.toMillis(1);

  /**
   * Public constructor
   *
   * @param bufferFlushTimeThreshold flush time threshold in seconds given in connector config
   * @param bufferByteSizeThreshold buffer size threshold in bytes given in connector config
   * @param bufferRecordCountThreshold record count threshold in number of kafka records given in
   *     connector config
   */
  public BufferThreshold(
      long bufferFlushTimeThreshold,
      long bufferByteSizeThreshold,
      long bufferRecordCountThreshold) {
    this.bufferFlushTimeThreshold = bufferFlushTimeThreshold;
    this.bufferByteSizeThreshold = bufferByteSizeThreshold;
    this.bufferRecordCountThreshold = bufferRecordCountThreshold;
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

        if (providedFlushTimeSecondsInConfig < BUFFER_FLUSH_TIME_SEC_MIN) {
          invalidConfigParams.put(
              SnowflakeSinkConnectorConfig.BUFFER_FLUSH_TIME_SEC,
              Utils.formatString(
                  "{} is {}, it should be greater than {}",
                  SnowflakeSinkConnectorConfig.BUFFER_FLUSH_TIME_SEC,
                  providedFlushTimeSecondsInConfig,
                  BUFFER_FLUSH_TIME_SEC_MIN));
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
