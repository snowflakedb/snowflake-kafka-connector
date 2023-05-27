package com.snowflake.kafka.connector.internal.streaming;

import com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig;
import com.snowflake.kafka.connector.internal.BufferThreshold;
import com.snowflake.kafka.connector.internal.TestUtils;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;

public class StreamingBufferThresholdTest {
  @Test
  public void testShouldFlushOnBufferByteSize() {
    final long bytesThresholdForBuffer = 10_000;

    BufferThreshold streamingBufferThreshold =
        new StreamingBufferThreshold(10, bytesThresholdForBuffer, 100);

    Assert.assertTrue(
        streamingBufferThreshold.shouldFlushOnBufferByteSize(bytesThresholdForBuffer));
    Assert.assertTrue(
        streamingBufferThreshold.shouldFlushOnBufferByteSize(bytesThresholdForBuffer + 1));
    Assert.assertFalse(
        streamingBufferThreshold.shouldFlushOnBufferByteSize(bytesThresholdForBuffer - 1));
  }

  @Test
  public void testShouldFlushOnBufferRecordCount() {
    final long bufferThresholdRecordCount = 100;

    StreamingBufferThreshold streamingBufferThreshold =
        new StreamingBufferThreshold(10, 10_000, bufferThresholdRecordCount);

    Assert.assertTrue(
        streamingBufferThreshold.shouldFlushOnBufferRecordCount(bufferThresholdRecordCount));
    Assert.assertTrue(
        streamingBufferThreshold.shouldFlushOnBufferRecordCount(bufferThresholdRecordCount + 1));
    Assert.assertFalse(
        streamingBufferThreshold.shouldFlushOnBufferRecordCount(bufferThresholdRecordCount - 1));
    Assert.assertFalse(streamingBufferThreshold.shouldFlushOnBufferRecordCount(0));
  }

  @Test
  public void testFlushTimeBased() {

    // 2020 Jan 1
    long previousFlushTimeStampMs = 1577865600000L;

    final long flushTimeThresholdSeconds = 10;

    StreamingBufferThreshold streamingBufferThreshold =
        new StreamingBufferThreshold(flushTimeThresholdSeconds, 10_0000, 100);

    Assert.assertTrue(streamingBufferThreshold.shouldFlushOnBufferTime(previousFlushTimeStampMs));

    // setting flush time to right now..
    previousFlushTimeStampMs = System.currentTimeMillis();
    Assert.assertFalse(streamingBufferThreshold.shouldFlushOnBufferTime(previousFlushTimeStampMs));

    // Subtracting 10 seconds
    previousFlushTimeStampMs = System.currentTimeMillis() - (10 * 1000);
    Assert.assertTrue(streamingBufferThreshold.shouldFlushOnBufferTime(previousFlushTimeStampMs));
  }

  @Test
  public void testValidBufferThreshold() {
    Map<String, String> config = TestUtils.getConfForStreaming();

    // test for invalid configs
    Map<String, String> invalidConfigs =
        StreamingBufferThreshold.validateBufferThreshold(
            config, IngestionMethodConfig.SNOWPIPE_STREAMING);

    // verify no invalid configs
    assert invalidConfigs.size() == 0;
  }

  @Test
  public void testInvalidBufferThresholds() {
    String invalidFlushTime = "-1";
    String invalidByteSize = "0.4";
    String invalidRecordCount = "0";

    this.testInvalidBufferThresholdRunner(
        SnowflakeSinkConnectorConfig.BUFFER_FLUSH_TIME_SEC,
        invalidFlushTime,
        BufferThreshold.FlushReason.BUFFER_FLUSH_TIME.toString());
    this.testInvalidBufferThresholdRunner(
        SnowflakeSinkConnectorConfig.BUFFER_SIZE_BYTES,
        invalidFlushTime,
        BufferThreshold.FlushReason.BUFFER_BYTE_SIZE.toString());
    this.testInvalidBufferThresholdRunner(
        SnowflakeSinkConnectorConfig.BUFFER_COUNT_RECORDS,
        invalidFlushTime,
        BufferThreshold.FlushReason.BUFFER_RECORD_COUNT.toString());
  }

  private void testInvalidBufferThresholdRunner(
      String configParam, String configVal, String invalidConfigParam) {
    Map<String, String> config = TestUtils.getConfForStreaming();

    // test invalid flush time
    config.put(configParam, configVal);

    Map<String, String> invalidConfigs =
        StreamingBufferThreshold.validateBufferThreshold(
            config, IngestionMethodConfig.SNOWPIPE_STREAMING);
    assert invalidConfigs.size() == 1;
    assert invalidConfigs.containsKey(invalidConfigParam);
  }
}
