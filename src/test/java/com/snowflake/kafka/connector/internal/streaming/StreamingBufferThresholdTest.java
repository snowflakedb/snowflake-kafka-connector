package com.snowflake.kafka.connector.internal.streaming;

import com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig;
import com.snowflake.kafka.connector.internal.BufferThreshold;
import com.snowflake.kafka.connector.internal.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.nio.Buffer;
import java.util.Map;

import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.BUFFER_COUNT_RECORDS_DEFAULT;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.BUFFER_SIZE_BYTES_DEFAULT;
import static com.snowflake.kafka.connector.internal.streaming.StreamingUtils.STREAMING_BUFFER_FLUSH_TIME_DEFAULT_SEC;

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
    config.put(SnowflakeSinkConnectorConfig.BUFFER_FLUSH_TIME_SEC, String.valueOf(STREAMING_BUFFER_FLUSH_TIME_DEFAULT_SEC));
    config.put(SnowflakeSinkConnectorConfig.BUFFER_SIZE_BYTES, String.valueOf(BUFFER_SIZE_BYTES_DEFAULT));
    config.put(SnowflakeSinkConnectorConfig.BUFFER_COUNT_RECORDS, String.valueOf(BUFFER_COUNT_RECORDS_DEFAULT));

    IngestionMethodConfig ingestionMethodConfig = IngestionMethodConfig.SNOWPIPE_STREAMING;

    Map<String, String> invalidConfigs = StreamingBufferThreshold.validateBufferThreshold(config, ingestionMethodConfig);

    // verify no invalid configs
    assert invalidConfigs.size() == 0;
  }

  @Test
  public void testInvalidBufferThreshold() {

  }
}
