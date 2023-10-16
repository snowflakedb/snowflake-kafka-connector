package com.snowflake.kafka.connector.internal;

import org.junit.Assert;
import org.junit.Test;

public class SnowpipeBufferThresholdTest {
  @Test
  public void testFlushBufferedBytesBased() {

    final long bytesThresholdForBuffer = 10_000;

    SnowpipeBufferThreshold snowpipeBufferThreshold =
        new SnowpipeBufferThreshold(10, bytesThresholdForBuffer, 100) {};

    Assert.assertTrue(
        snowpipeBufferThreshold.shouldFlushOnBufferByteSize(bytesThresholdForBuffer));

    Assert.assertTrue(
        snowpipeBufferThreshold.shouldFlushOnBufferByteSize(bytesThresholdForBuffer + 1));

    Assert.assertFalse(
        snowpipeBufferThreshold.shouldFlushOnBufferByteSize(bytesThresholdForBuffer - 1));
  }

  @Test
  public void testFlushBufferedRecordCountBased() {

    final long bufferThresholdRecordCount = 100;

    SnowpipeBufferThreshold snowpipeBufferThreshold =
        new SnowpipeBufferThreshold(10, 10_000, bufferThresholdRecordCount);

    Assert.assertTrue(
        snowpipeBufferThreshold.shouldFlushOnBufferRecordCount(bufferThresholdRecordCount));

    Assert.assertTrue(
        snowpipeBufferThreshold.shouldFlushOnBufferRecordCount(bufferThresholdRecordCount + 1));

    Assert.assertFalse(
        snowpipeBufferThreshold.shouldFlushOnBufferRecordCount(bufferThresholdRecordCount - 1));

    Assert.assertFalse(snowpipeBufferThreshold.shouldFlushOnBufferRecordCount(0));
  }

  @Test
  public void testFlushTimeBased() {

    // 2020 Jan 1
    long previousFlushTimeStampMs = 1577865600000L;

    final long flushTimeThresholdSeconds = 10;

    SnowpipeBufferThreshold snowpipeBufferThreshold =
        new SnowpipeBufferThreshold(flushTimeThresholdSeconds, 10_0000, 100);

    Assert.assertTrue(snowpipeBufferThreshold.shouldFlushOnBufferTime(previousFlushTimeStampMs));

    // setting flush time to right now..
    previousFlushTimeStampMs = System.currentTimeMillis();

    Assert.assertFalse(snowpipeBufferThreshold.shouldFlushOnBufferTime(previousFlushTimeStampMs));

    // Subtracting 10 seconds
    previousFlushTimeStampMs = System.currentTimeMillis() - (10 * 1000);

    Assert.assertTrue(snowpipeBufferThreshold.shouldFlushOnBufferTime(previousFlushTimeStampMs));
  }
}
