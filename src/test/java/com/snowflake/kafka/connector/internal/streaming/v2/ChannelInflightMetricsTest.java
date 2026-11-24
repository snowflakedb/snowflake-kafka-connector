package com.snowflake.kafka.connector.internal.streaming.v2;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import com.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class ChannelInflightMetricsTest {

  private static final String ROWS_KEY = "snowflake.streaming.max.task.inflight.rows";
  private static final String MEGABYTES_KEY = "snowflake.streaming.max.task.inflight.megabytes";

  @AfterEach
  void tearDown() {
    FakeInflightSdk.readers = FakeInflightSdk.Reader::new;
  }

  // --- reading through the SDK reader API ---

  @Test
  void readerReadsRowsAndBytesOfTheResolvedSdkReader() throws Exception {
    FakeInflightSdk.Reader sdkReader = new FakeInflightSdk.Reader();
    sdkReader.rows = 7;
    sdkReader.bytes = 700;
    FakeInflightSdk.readers = () -> sdkReader;

    ChannelInflightMetrics.InflightReader reader =
        ChannelInflightMetrics.readersOf(FakeInflightSdk.SinkRegistered.class)
            .forChannel(mock(SnowflakeStreamingIngestChannel.class));

    assertEquals(7, reader.rows());
    assertEquals(700, reader.bytes());
    sdkReader.rows = 8;
    assertEquals(8, reader.rows());
  }

  @Test
  void readerPassesTheSdkIllegalStateExceptionThroughUnwrapped() throws Exception {
    FakeInflightSdk.Reader sdkReader = new FakeInflightSdk.Reader();
    IllegalStateException failure = new IllegalStateException("no inflight gauge pushed");
    sdkReader.failure = failure;
    FakeInflightSdk.readers = () -> sdkReader;

    ChannelInflightMetrics.InflightReader reader =
        ChannelInflightMetrics.readersOf(FakeInflightSdk.SinkRegistered.class)
            .forChannel(mock(SnowflakeStreamingIngestChannel.class));

    assertSame(failure, assertThrows(IllegalStateException.class, reader::rows));
    assertSame(failure, assertThrows(IllegalStateException.class, reader::bytes));
  }

  @Test
  void readerResolutionPassesTheSdkIllegalArgumentExceptionThrough() throws Exception {
    ChannelInflightMetrics.InflightReaderFactory readers =
        ChannelInflightMetrics.readersOf(FakeInflightSdk.SinkRegistered.class);

    assertThrows(IllegalArgumentException.class, () -> readers.forChannel(null));
  }

  @Test
  void sdkReadersReportNoReaderWhenTheSdkOnTheClasspathHasNone() {
    // The test classpath bundles an SDK without the reader API.
    IllegalStateException thrown =
        assertThrows(
            IllegalStateException.class,
            () ->
                ChannelInflightMetrics.SDK_READERS.forChannel(
                    mock(SnowflakeStreamingIngestChannel.class)));
    assertTrue(thrown.getMessage().contains("no inflight readers"), thrown.getMessage());
  }

  // --- requireSdkSupport ---

  @Test
  void sdkSupportNotRequiredWhenBothCapsDisabled() {
    assertDoesNotThrow(() -> ChannelInflightMetrics.requireSdkSupport(0, 0, "no.such.SdkClass"));
  }

  @Test
  void sdkSupportFailsWhenRowsCapEnabledAndSdkLacksJmxMetrics() {
    ConnectException thrown =
        assertThrows(
            ConnectException.class,
            () -> ChannelInflightMetrics.requireSdkSupport(1, 0, "no.such.SdkClass"));
    assertTrue(thrown.getMessage().contains(ROWS_KEY), thrown.getMessage());
    assertTrue(thrown.getMessage().contains("no.such.SdkClass"), thrown.getMessage());
    assertInstanceOf(ClassNotFoundException.class, thrown.getCause());
  }

  @Test
  void sdkSupportPassesWhenSdkHasReadersAndMetricsSinkRegistered() {
    assertDoesNotThrow(
        () ->
            ChannelInflightMetrics.requireSdkSupport(
                1, 1, FakeInflightSdk.SinkRegistered.class.getName()));
  }

  @Test
  void sdkSupportFailsWhenSdkProvidesNoInflightReaders() {
    ConnectException thrown =
        assertThrows(
            ConnectException.class,
            () ->
                ChannelInflightMetrics.requireSdkSupport(
                    0, 1, FakeInflightSdk.WithoutReader.class.getName()));
    assertTrue(thrown.getMessage().contains(ROWS_KEY), thrown.getMessage());
    assertTrue(thrown.getMessage().contains(MEGABYTES_KEY), thrown.getMessage());
    assertTrue(thrown.getMessage().contains("inflightReader"), thrown.getMessage());
    assertInstanceOf(NoSuchMethodException.class, thrown.getCause());
  }

  @Test
  void sdkSupportFailsWhenMetricsSinkDidNotRegister() {
    ConnectException thrown =
        assertThrows(
            ConnectException.class,
            () ->
                ChannelInflightMetrics.requireSdkSupport(
                    0, 1, FakeInflightSdk.SinkFailed.class.getName()));
    assertTrue(thrown.getMessage().contains(ROWS_KEY), thrown.getMessage());
    assertTrue(thrown.getMessage().contains(MEGABYTES_KEY), thrown.getMessage());
    assertTrue(
        thrown.getMessage().contains("Curated metrics are not pushed to Java"),
        thrown.getMessage());
  }

  @Test
  void sdkSupportFailsWhenSdkCannotReportMetricsSink() {
    ConnectException thrown =
        assertThrows(
            ConnectException.class,
            () ->
                ChannelInflightMetrics.requireSdkSupport(
                    1, 0, FakeInflightSdk.WithoutSinkCheck.class.getName()));
    assertTrue(thrown.getMessage().contains(ROWS_KEY), thrown.getMessage());
    assertTrue(thrown.getMessage().contains(MEGABYTES_KEY), thrown.getMessage());
    assertInstanceOf(NoSuchMethodException.class, thrown.getCause());
  }

  @Test
  void sdkSupportFailsWhenMetricsSinkCheckThrows() {
    ConnectException thrown =
        assertThrows(
            ConnectException.class,
            () ->
                ChannelInflightMetrics.requireSdkSupport(
                    1, 0, FakeInflightSdk.SinkCheckThrows.class.getName()));
    assertEquals("Failed to check the snowpipe-streaming JMX metrics sink", thrown.getMessage());
    assertInstanceOf(IllegalStateException.class, thrown.getCause());
  }
}
