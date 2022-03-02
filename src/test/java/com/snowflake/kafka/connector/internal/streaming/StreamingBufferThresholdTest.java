package com.snowflake.kafka.connector.internal.streaming;

import org.junit.Assert;
import org.junit.Test;

public class StreamingBufferThresholdTest {
  @Test
  public void testFlushBufferedBytesBased() {

    final long bytesThresholdForBuffer = 10_000;

    StreamingBufferThreshold streamingBufferThreshold =
        new StreamingBufferThreshold(10, bytesThresholdForBuffer, 100);

    Assert.assertTrue(streamingBufferThreshold.isFlushBufferedBytesBased(bytesThresholdForBuffer));

    Assert.assertTrue(
        streamingBufferThreshold.isFlushBufferedBytesBased(bytesThresholdForBuffer + 1));

    Assert.assertFalse(
        streamingBufferThreshold.isFlushBufferedBytesBased(bytesThresholdForBuffer - 1));
  }

  @Test
  public void testFlushBufferedRecordCountBased() {

    final long bufferThresholdRecordCount = 100;

    StreamingBufferThreshold streamingBufferThreshold =
        new StreamingBufferThreshold(10, 10_000, bufferThresholdRecordCount);

    Assert.assertTrue(
        streamingBufferThreshold.isFlushBufferedRecordCountBased(bufferThresholdRecordCount));

    Assert.assertTrue(
        streamingBufferThreshold.isFlushBufferedRecordCountBased(bufferThresholdRecordCount + 1));

    Assert.assertFalse(
        streamingBufferThreshold.isFlushBufferedRecordCountBased(bufferThresholdRecordCount - 1));

    Assert.assertFalse(streamingBufferThreshold.isFlushBufferedRecordCountBased(0));
  }

  @Test
  public void testFlushTimeBased() {

    // 2020 Jan 1
    long previousFlushTimeStampMs = 1577865600000L;

    final long flushTimeThresholdSeconds = 10;

    StreamingBufferThreshold streamingBufferThreshold =
        new StreamingBufferThreshold(flushTimeThresholdSeconds, 10_0000, 100);

    Assert.assertTrue(streamingBufferThreshold.isFlushTimeBased(previousFlushTimeStampMs));

    // setting flush time to right now..
    previousFlushTimeStampMs = System.currentTimeMillis();

    Assert.assertFalse(streamingBufferThreshold.isFlushTimeBased(previousFlushTimeStampMs));

    // Subtracting 10 seconds
    previousFlushTimeStampMs = System.currentTimeMillis() - (10 * 1000);

    Assert.assertTrue(streamingBufferThreshold.isFlushTimeBased(previousFlushTimeStampMs));
  }
}
