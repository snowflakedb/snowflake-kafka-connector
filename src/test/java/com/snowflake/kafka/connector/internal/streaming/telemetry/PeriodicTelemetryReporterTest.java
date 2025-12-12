package com.snowflake.kafka.connector.internal.streaming.telemetry;

import static com.snowflake.kafka.connector.internal.streaming.telemetry.PeriodicTelemetryReporter.MAX_INITIAL_JITTER_MS;
import static com.snowflake.kafka.connector.internal.telemetry.TelemetryConstants.TOPIC_PARTITION_CHANNEL_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.snowflake.kafka.connector.internal.streaming.channel.TopicPartitionChannel;
import com.snowflake.kafka.connector.internal.telemetry.SnowflakeTelemetryService;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import net.snowflake.client.jdbc.telemetry.Telemetry;
import net.snowflake.client.jdbc.telemetry.TelemetryData;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class PeriodicTelemetryReporterTest {

  private static final String CONNECTOR_NAME = "test-connector";
  private static final String TASK_ID = "0";
  private static final long SHORT_REPORT_INTERVAL_MS = 100L;
  private static final long MAX_WAIT_FOR_FIRST_REPORT_MS = MAX_INITIAL_JITTER_MS + 2000;

  private MockTelemetryClient mockTelemetryClient;
  private SnowflakeTelemetryService telemetryService;
  private PeriodicTelemetryReporter reporter;

  @BeforeEach
  void setUp() {
    mockTelemetryClient = new MockTelemetryClient();
    telemetryService = new SnowflakeTelemetryService(mockTelemetryClient);
    telemetryService.setAppName(CONNECTOR_NAME);
    telemetryService.setTaskID(TASK_ID);
  }

  @AfterEach
  void tearDown() {
    if (reporter != null) {
      reporter.stop();
    }
  }

  @Test
  void shouldStartAndStopWithoutErrors() {
    // Given
    Supplier<Map<String, TopicPartitionChannel>> emptySupplier = Collections::emptyMap;
    reporter =
        new PeriodicTelemetryReporter(
            telemetryService, emptySupplier, CONNECTOR_NAME, TASK_ID, SHORT_REPORT_INTERVAL_MS);

    // When/Then
    assertDoesNotThrow(() -> reporter.start());
    assertDoesNotThrow(() -> reporter.stop());
  }

  @Test
  void shouldNotReportTelemetryWhenNoChannelsExist() throws InterruptedException {
    // Given
    Supplier<Map<String, TopicPartitionChannel>> emptySupplier = Collections::emptyMap;
    reporter =
        new PeriodicTelemetryReporter(
            telemetryService, emptySupplier, CONNECTOR_NAME, TASK_ID, SHORT_REPORT_INTERVAL_MS);

    // When
    reporter.start();

    // Wait for at least one report cycle
    Thread.sleep(SHORT_REPORT_INTERVAL_MS * 3);

    // Then
    assertTrue(
        mockTelemetryClient.getSentTelemetryData().isEmpty(),
        "No telemetry should be sent when there are no channels");
  }

  @Test
  void shouldNotReportTelemetryWhenChannelsSupplierReturnsNull() throws InterruptedException {
    // Given
    Supplier<Map<String, TopicPartitionChannel>> nullSupplier = () -> null;
    reporter =
        new PeriodicTelemetryReporter(
            telemetryService, nullSupplier, CONNECTOR_NAME, TASK_ID, SHORT_REPORT_INTERVAL_MS);

    // When
    reporter.start();

    // Wait for at least one report cycle
    Thread.sleep(SHORT_REPORT_INTERVAL_MS * 3);

    // Then
    assertTrue(
        mockTelemetryClient.getSentTelemetryData().isEmpty(),
        "No telemetry should be sent when supplier returns null");
  }

  @Test
  void shouldReportTelemetryForActiveChannels() throws InterruptedException {
    // Given
    TopicPartitionChannel mockChannel = createMockChannelWithNonEmptyStatus();
    Map<String, TopicPartitionChannel> channels = new HashMap<>();
    channels.put("channel1", mockChannel);

    Supplier<Map<String, TopicPartitionChannel>> channelSupplier = () -> channels;
    reporter =
        new PeriodicTelemetryReporter(
            telemetryService, channelSupplier, CONNECTOR_NAME, TASK_ID, SHORT_REPORT_INTERVAL_MS);

    // When
    reporter.start();

    // Wait for telemetry to be reported (accounting for jitter)
    waitForTelemetryCount(1, MAX_WAIT_FOR_FIRST_REPORT_MS);

    // Then
    assertTrue(
        mockTelemetryClient.getSentTelemetryData().size() >= 1,
        "At least one telemetry report should be sent");
  }

  @Test
  void shouldReportTelemetryForMultipleChannels() throws InterruptedException {
    // Given
    final String channelName1 = "testChannel_topic1_partition0";
    final String channelName2 = "testChannel_topic2_partition1";
    TopicPartitionChannel mockChannel1 = createMockChannelWithNonEmptyStatus(channelName1);
    TopicPartitionChannel mockChannel2 = createMockChannelWithNonEmptyStatus(channelName2);
    Map<String, TopicPartitionChannel> channels = new HashMap<>();
    channels.put("channel1", mockChannel1);
    channels.put("channel2", mockChannel2);

    Supplier<Map<String, TopicPartitionChannel>> channelSupplier = () -> channels;
    reporter =
        new PeriodicTelemetryReporter(
            telemetryService, channelSupplier, CONNECTOR_NAME, TASK_ID, SHORT_REPORT_INTERVAL_MS);

    // When
    reporter.start();

    // Wait for telemetry to be reported (at least 2 reports for 2 channels, accounting for jitter)
    waitForTelemetryCount(2, MAX_WAIT_FOR_FIRST_REPORT_MS);

    // Then
    LinkedList<TelemetryData> sentTelemetry = mockTelemetryClient.getSentTelemetryData();
    assertTrue(sentTelemetry.size() >= 2, "Telemetry should be sent for all channels");

    Set<String> reportedChannelNames =
        sentTelemetry.stream()
            .map(
                telemetryData ->
                    telemetryData
                        .getMessage()
                        .get("data")
                        .get(TOPIC_PARTITION_CHANNEL_NAME)
                        .asText())
            .collect(Collectors.toSet());

    assertEquals(2, reportedChannelNames.size(), "Telemetry should be sent for both channels");
    assertThat(reportedChannelNames).containsExactlyInAnyOrder(channelName1, channelName2);
  }

  @Test
  void shouldNotReportTelemetryForEmptyChannelStatus() throws InterruptedException {
    // Given
    TopicPartitionChannel mockChannel = createMockChannelWithEmptyStatus();
    Map<String, TopicPartitionChannel> channels = new HashMap<>();
    channels.put("channel1", mockChannel);

    Supplier<Map<String, TopicPartitionChannel>> channelSupplier = () -> channels;
    reporter =
        new PeriodicTelemetryReporter(
            telemetryService, channelSupplier, CONNECTOR_NAME, TASK_ID, SHORT_REPORT_INTERVAL_MS);

    // When
    reporter.start();

    // Wait for at least one report cycle
    Thread.sleep(SHORT_REPORT_INTERVAL_MS * 3);

    // Then - empty status should not be reported
    assertTrue(
        mockTelemetryClient.getSentTelemetryData().isEmpty(),
        "Empty channel status should not trigger telemetry");
  }

  @Test
  void shouldNotReportTelemetryWhenChannelStatusIsNull() throws InterruptedException {
    // Given
    TopicPartitionChannel mockChannel = mock(TopicPartitionChannel.class);
    when(mockChannel.getSnowflakeTelemetryChannelStatus()).thenReturn(null);
    Map<String, TopicPartitionChannel> channels = new HashMap<>();
    channels.put("channel1", mockChannel);

    Supplier<Map<String, TopicPartitionChannel>> channelSupplier = () -> channels;
    reporter =
        new PeriodicTelemetryReporter(
            telemetryService, channelSupplier, CONNECTOR_NAME, TASK_ID, SHORT_REPORT_INTERVAL_MS);

    // When
    reporter.start();

    // Wait for at least one report cycle
    Thread.sleep(SHORT_REPORT_INTERVAL_MS * 3);

    // Then - null status should not be reported
    assertTrue(
        mockTelemetryClient.getSentTelemetryData().isEmpty(),
        "Null channel status should not trigger telemetry");
  }

  @Test
  void shouldContinueReportingAfterExceptionInChannelStatusRetrieval() throws InterruptedException {
    // Given
    TopicPartitionChannel failingChannel = mock(TopicPartitionChannel.class);
    when(failingChannel.getSnowflakeTelemetryChannelStatus())
        .thenThrow(new RuntimeException("Test exception"));

    TopicPartitionChannel workingChannel = createMockChannelWithNonEmptyStatus();

    Map<String, TopicPartitionChannel> channels = new HashMap<>();
    channels.put("failingChannel", failingChannel);
    channels.put("workingChannel", workingChannel);

    Supplier<Map<String, TopicPartitionChannel>> channelSupplier = () -> channels;
    reporter =
        new PeriodicTelemetryReporter(
            telemetryService, channelSupplier, CONNECTOR_NAME, TASK_ID, SHORT_REPORT_INTERVAL_MS);

    // When
    reporter.start();

    // Wait for telemetry to be reported (accounting for jitter)
    waitForTelemetryCount(1, MAX_WAIT_FOR_FIRST_REPORT_MS);

    // Then - should still report for the working channel
    assertTrue(
        mockTelemetryClient.getSentTelemetryData().size() >= 1,
        "Telemetry should be reported despite exception in one channel");
  }

  @Test
  void shouldContinueReportingAfterExceptionInSupplier() throws InterruptedException {
    // Given
    final AtomicLong callCount = new AtomicLong(0);
    TopicPartitionChannel mockChannel = createMockChannelWithNonEmptyStatus();
    Map<String, TopicPartitionChannel> channels = new HashMap<>();
    channels.put("channel1", mockChannel);

    Supplier<Map<String, TopicPartitionChannel>> flakySupplier =
        () -> {
          if (callCount.incrementAndGet() == 1) {
            throw new RuntimeException("First call fails");
          }
          return channels;
        };

    reporter =
        new PeriodicTelemetryReporter(
            telemetryService, flakySupplier, CONNECTOR_NAME, TASK_ID, SHORT_REPORT_INTERVAL_MS);

    // When
    reporter.start();

    // Wait for telemetry to be reported (accounting for jitter + one extra interval after failure)
    waitForTelemetryCount(1, MAX_WAIT_FOR_FIRST_REPORT_MS + SHORT_REPORT_INTERVAL_MS * 2);

    // Then - should eventually report after first failure
    assertTrue(
        mockTelemetryClient.getSentTelemetryData().size() >= 1,
        "Telemetry should be reported after supplier recovers from exception");
  }

  @Test
  void shouldReportPeriodically() throws InterruptedException {
    // Given
    TopicPartitionChannel mockChannel = createMockChannelWithNonEmptyStatus();
    Map<String, TopicPartitionChannel> channels = new HashMap<>();
    channels.put("channel1", mockChannel);

    Supplier<Map<String, TopicPartitionChannel>> channelSupplier = () -> channels;
    reporter =
        new PeriodicTelemetryReporter(
            telemetryService, channelSupplier, CONNECTOR_NAME, TASK_ID, SHORT_REPORT_INTERVAL_MS);

    // When
    reporter.start();

    // Wait for multiple report cycles (jitter + at least one more interval)
    waitForTelemetryCount(2, MAX_WAIT_FOR_FIRST_REPORT_MS + SHORT_REPORT_INTERVAL_MS * 2);

    // Then - should report multiple times
    assertTrue(
        mockTelemetryClient.getSentTelemetryData().size() >= 2,
        "Telemetry should be reported periodically");
  }

  private void waitForTelemetryCount(int minCount, long maxWaitMs) throws InterruptedException {
    long startTime = System.currentTimeMillis();
    while (mockTelemetryClient.getSentTelemetryData().size() < minCount) {
      if (System.currentTimeMillis() - startTime > maxWaitMs) {
        break;
      }
      Thread.sleep(10);
    }
  }

  private TopicPartitionChannel createMockChannelWithNonEmptyStatus() {
    return createMockChannelWithNonEmptyStatus("testChannel");
  }

  private TopicPartitionChannel createMockChannelWithNonEmptyStatus(final String channelName) {
    TopicPartitionChannel mockChannel = mock(TopicPartitionChannel.class);
    SnowflakeTelemetryChannelStatus mockStatus =
        new SnowflakeTelemetryChannelStatus(
            "testTable",
            CONNECTOR_NAME,
            channelName,
            System.currentTimeMillis(),
            false,
            null,
            new AtomicLong(10L),
            new AtomicLong(5L),
            new AtomicLong(15L));
    when(mockChannel.getSnowflakeTelemetryChannelStatus()).thenReturn(mockStatus);
    return mockChannel;
  }

  private TopicPartitionChannel createMockChannelWithEmptyStatus() {
    TopicPartitionChannel mockChannel = mock(TopicPartitionChannel.class);
    SnowflakeTelemetryChannelStatus emptyStatus =
        new SnowflakeTelemetryChannelStatus(
            "testTable",
            CONNECTOR_NAME,
            "testChannel",
            System.currentTimeMillis(),
            false,
            null,
            new AtomicLong(-1L),
            new AtomicLong(-1L),
            new AtomicLong(-1L));
    when(mockChannel.getSnowflakeTelemetryChannelStatus()).thenReturn(emptyStatus);
    return mockChannel;
  }

  /** Mock implementation of Telemetry for testing. */
  static class MockTelemetryClient implements Telemetry {

    private final LinkedList<TelemetryData> telemetryDataList = new LinkedList<>();
    private final LinkedList<TelemetryData> sentTelemetryData = new LinkedList<>();
    private final ExecutorService executor = Executors.newSingleThreadExecutor();

    @Override
    public void addLogToBatch(TelemetryData telemetryData) {
      synchronized (this) {
        telemetryDataList.add(telemetryData);
      }
    }

    @Override
    public void close() {
      synchronized (this) {
        telemetryDataList.clear();
        sentTelemetryData.clear();
      }
    }

    @Override
    public Future<Boolean> sendBatchAsync() {
      return executor.submit(
          () -> {
            synchronized (MockTelemetryClient.this) {
              sentTelemetryData.addAll(telemetryDataList);
              telemetryDataList.clear();
            }
            return true;
          });
    }

    @Override
    public void postProcess(String s, String s1, int i, Throwable throwable) {}

    LinkedList<TelemetryData> getSentTelemetryData() {
      synchronized (this) {
        return new LinkedList<>(sentTelemetryData);
      }
    }
  }
}
