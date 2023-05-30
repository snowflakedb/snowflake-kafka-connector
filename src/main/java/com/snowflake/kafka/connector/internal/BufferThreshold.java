package com.snowflake.kafka.connector.internal;

import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.BUFFER_COUNT_RECORDS_MIN;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.BUFFER_FLUSH_TIME_SEC_MIN;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.BUFFER_SIZE_BYTES_MIN;
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

  private final long bufferFlushTimeThresholdMs;

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

  // ideally this would be in the streamingBuffer object, however java doesn't allow enums to be
  // added to a protected inner class
  public enum FlushReason {
    NONE("NONE"),
    BUFFER_FLUSH_TIME(SnowflakeSinkConnectorConfig.BUFFER_FLUSH_TIME_SEC),
    BUFFER_BYTE_SIZE(SnowflakeSinkConnectorConfig.BUFFER_SIZE_BYTES),
    BUFFER_RECORD_COUNT(SnowflakeSinkConnectorConfig.BUFFER_COUNT_RECORDS),
    ;

    private final String str;

    FlushReason(String str) {
      this.str = str;
    }

    @Override
    public String toString() {
      return this.str;
    }
  }

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
    this.bufferFlushTimeThresholdMs = TimeUnit.SECONDS.toMillis(this.bufferFlushTimeThreshold);
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
    return (currentTimeMs - previousFlushTimeStampMs) >= (this.bufferFlushTimeThresholdMs);
  }

  /** Returns the buffer flush time threshold */
  public long getBufferFlushTimeThreshold() {
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
        verifyBufferThreshold(
            FlushReason.BUFFER_FLUSH_TIME, providedSFConnectorConfig, ingestionMethodConfig));
    invalidConfigParams.putAll(
        verifyBufferThreshold(
            FlushReason.BUFFER_BYTE_SIZE, providedSFConnectorConfig, ingestionMethodConfig));
    invalidConfigParams.putAll(
        verifyBufferThreshold(
            FlushReason.BUFFER_RECORD_COUNT, providedSFConnectorConfig, ingestionMethodConfig));
    return ImmutableMap.copyOf(invalidConfigParams);
  }

  private static Map<String, String> verifyBufferThreshold(
      FlushReason flushReason,
      Map<String, String> providedSFConnectorConfig,
      IngestionMethodConfig ingestionMethodConfig) {
    Map<String, String> invalidConfigParams = new HashMap();

    String sfBufferConfigName;
    long minValidThreshold;

    // get params based on buffer flush threshold
    switch (flushReason) {
      case BUFFER_FLUSH_TIME:
        sfBufferConfigName = SnowflakeSinkConnectorConfig.BUFFER_FLUSH_TIME_SEC;
        minValidThreshold =
            ingestionMethodConfig.equals(IngestionMethodConfig.SNOWPIPE)
                ? BUFFER_FLUSH_TIME_SEC_MIN
                : STREAMING_BUFFER_FLUSH_TIME_MINIMUM_SEC;
        break;
      case BUFFER_BYTE_SIZE:
        sfBufferConfigName = SnowflakeSinkConnectorConfig.BUFFER_SIZE_BYTES;
        minValidThreshold = BUFFER_SIZE_BYTES_MIN;
        break;
      case BUFFER_RECORD_COUNT:
        sfBufferConfigName = SnowflakeSinkConnectorConfig.BUFFER_COUNT_RECORDS;
        minValidThreshold = BUFFER_COUNT_RECORDS_MIN;
        break;
      default:
        invalidConfigParams.put(
            flushReason.toString(),
            "Invalid buffer flush reason provided for buffer threshold verification");
        return invalidConfigParams;
    }

    // verify threshold
    String errorMsg =
        verifyBufferThresholdHelper(
            providedSFConnectorConfig,
            ingestionMethodConfig,
            sfBufferConfigName,
            minValidThreshold);
    if (errorMsg != null && !errorMsg.isEmpty()) {
      invalidConfigParams.put(sfBufferConfigName, errorMsg);
    }

    return invalidConfigParams;
  }

  private static String verifyBufferThresholdHelper(
      Map<String, String> providedSFConnectorConfig,
      IngestionMethodConfig ingestionMethodConfig,
      String sfBufferConfigName,
      long minValidThreshold) {
    // check config has threshold
    if (!providedSFConnectorConfig.containsKey(sfBufferConfigName)) {
      return Utils.formatString("Config {} is empty", sfBufferConfigName);
    }

    String providedBufferThresholdStr = providedSFConnectorConfig.get(sfBufferConfigName);
    long providedBufferThreshold;

    // try parse long
    try {
      providedBufferThreshold = Long.parseLong(providedBufferThresholdStr);
    } catch (NumberFormatException e) {
      return Utils.formatString(
          "{} should be a positive integer. Invalid integer was provided:{}",
          sfBufferConfigName,
          providedBufferThresholdStr);
    }

    // test against threshold
    if (providedBufferThreshold < minValidThreshold) {
      return Utils.formatString(
          "{} is {}, it should be greater than {}",
          sfBufferConfigName,
          providedBufferThreshold,
          minValidThreshold);
    }

    return null;
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
