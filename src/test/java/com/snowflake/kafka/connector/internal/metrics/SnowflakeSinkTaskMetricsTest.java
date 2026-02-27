package com.snowflake.kafka.connector.internal.metrics;

import static com.snowflake.kafka.connector.internal.TestUtils.TEST_CONNECTOR_NAME;
import static com.snowflake.kafka.connector.internal.metrics.MetricsUtil.taskMetricName;
import static com.snowflake.kafka.connector.internal.metrics.SnowflakeSinkTaskMetrics.*;
import static org.junit.Assert.*;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SnowflakeSinkTaskMetricsTest {

  private static final String TASK_ID = "3";
  private static final String PREFIX = "task-" + TASK_ID;

  private MetricRegistry metricRegistry;
  private MetricsJmxReporter metricsJmxReporter;
  private SnowflakeSinkTaskMetrics metrics;

  @Before
  public void setUp() {
    metricRegistry = new MetricRegistry();
    metricsJmxReporter = new MetricsJmxReporter(metricRegistry, TEST_CONNECTOR_NAME);
  }

  @After
  public void tearDown() {
    if (metrics != null) {
      metrics.unregister();
    }
  }

  private void createMetrics() {
    metrics = new SnowflakeSinkTaskMetrics(TEST_CONNECTOR_NAME, TASK_ID, metricsJmxReporter);
  }

  private void createMetricsWithSdkClientCount(int initialCount) {
    AtomicInteger sdkCount = new AtomicInteger(initialCount);
    metrics =
        new SnowflakeSinkTaskMetrics(
            TEST_CONNECTOR_NAME, TASK_ID, metricsJmxReporter, sdkCount::get);
  }

  @Test
  public void testAllMetricsRegistered() {
    createMetrics();

    // Method duration timers
    assertNotNull(
        metricRegistry.getTimers().get(taskMetricName(PREFIX, TASK_SUB_DOMAIN, PUT_DURATION)));
    assertNotNull(
        metricRegistry
            .getTimers()
            .get(taskMetricName(PREFIX, TASK_SUB_DOMAIN, PRECOMMIT_DURATION)));
    assertNotNull(
        metricRegistry
            .getTimers()
            .get(taskMetricName(PREFIX, TASK_SUB_DOMAIN, PRECOMMIT_OFFSET_FETCH_DURATION)));

    // Lifecycle duration timers
    assertNotNull(
        metricRegistry
            .getTimers()
            .get(taskMetricName(PREFIX, LIFECYCLE_SUB_DOMAIN, OPEN_DURATION)));
    assertNotNull(
        metricRegistry
            .getTimers()
            .get(taskMetricName(PREFIX, LIFECYCLE_SUB_DOMAIN, CLOSE_DURATION)));
    assertNotNull(
        metricRegistry
            .getTimers()
            .get(taskMetricName(PREFIX, LIFECYCLE_SUB_DOMAIN, START_DURATION)));

    // Channel/SDK timers
    assertNotNull(
        metricRegistry
            .getTimers()
            .get(taskMetricName(PREFIX, LIFECYCLE_SUB_DOMAIN, CHANNEL_OPEN_DURATION)));
    assertNotNull(
        metricRegistry
            .getTimers()
            .get(taskMetricName(PREFIX, LIFECYCLE_SUB_DOMAIN, SDK_CLIENT_CREATE_DURATION)));

    // Meter
    assertNotNull(
        metricRegistry.getMeters().get(taskMetricName(PREFIX, TASK_SUB_DOMAIN, PUT_RECORDS)));

    // Counters
    assertNotNull(
        metricRegistry
            .getCounters()
            .get(taskMetricName(PREFIX, TASK_SUB_DOMAIN, PRECOMMIT_PARTITIONS_SKIPPED)));
    assertNotNull(
        metricRegistry.getCounters().get(taskMetricName(PREFIX, LIFECYCLE_SUB_DOMAIN, OPEN_COUNT)));
    assertNotNull(
        metricRegistry
            .getCounters()
            .get(taskMetricName(PREFIX, LIFECYCLE_SUB_DOMAIN, CLOSE_COUNT)));
    assertNotNull(
        metricRegistry
            .getCounters()
            .get(taskMetricName(PREFIX, LIFECYCLE_SUB_DOMAIN, CHANNEL_OPEN_COUNT)));

    // Gauges
    assertNotNull(
        metricRegistry
            .getGauges()
            .get(taskMetricName(PREFIX, TASK_SUB_DOMAIN, ASSIGNED_PARTITIONS)));
  }

  @Test
  public void testPutDurationTimer() {
    createMetrics();
    Timer.Context ctx = metrics.putDuration().time();
    ctx.stop();
    assertEquals(1, metrics.putDuration().getCount());
  }

  @Test
  public void testPreCommitDurationTimer() {
    createMetrics();
    Timer.Context ctx = metrics.preCommitDuration().time();
    ctx.stop();
    assertEquals(1, metrics.preCommitDuration().getCount());
  }

  @Test
  public void testLifecycleTimers() {
    createMetrics();

    Timer.Context openCtx = metrics.openDuration().time();
    openCtx.stop();
    assertEquals(1, metrics.openDuration().getCount());

    Timer.Context closeCtx = metrics.closeDuration().time();
    closeCtx.stop();
    assertEquals(1, metrics.closeDuration().getCount());

    Timer.Context startCtx = metrics.startDuration().time();
    startCtx.stop();
    assertEquals(1, metrics.startDuration().getCount());
  }

  @Test
  public void testChannelAndSdkTimers() {
    createMetrics();

    Timer.Context channelCtx = metrics.channelOpenDuration().time();
    channelCtx.stop();
    Timer.Context channelCtx2 = metrics.channelOpenDuration().time();
    channelCtx2.stop();
    assertEquals(2, metrics.channelOpenDuration().getCount());

    Timer.Context sdkCtx = metrics.sdkClientCreateDuration().time();
    sdkCtx.stop();
    assertEquals(1, metrics.sdkClientCreateDuration().getCount());

    Timer.Context fetchCtx = metrics.preCommitOffsetFetchDuration().time();
    fetchCtx.stop();
    assertEquals(1, metrics.preCommitOffsetFetchDuration().getCount());
  }

  @Test
  public void testPutRecordsMeter() {
    createMetrics();
    metrics.putRecords().mark(100);
    metrics.putRecords().mark(50);
    assertEquals(150, metrics.putRecords().getCount());
  }

  @Test
  public void testPreCommitPartitionsSkipped() {
    createMetrics();
    metrics.preCommitPartitionsSkipped().inc(3);
    assertEquals(3, metrics.preCommitPartitionsSkipped().getCount());
  }

  @Test
  public void testAssignedPartitionsGauge() {
    createMetrics();
    assertEquals(0, metrics.getAssignedPartitions());
    metrics.setAssignedPartitions(12);
    assertEquals(12, metrics.getAssignedPartitions());

    @SuppressWarnings("unchecked")
    Gauge<Integer> gauge =
        metricRegistry
            .getGauges()
            .get(taskMetricName(PREFIX, TASK_SUB_DOMAIN, ASSIGNED_PARTITIONS));
    assertEquals(Integer.valueOf(12), gauge.getValue());
  }

  @Test
  public void testLifecycleCounters() {
    createMetrics();
    metrics.openCount().inc();
    metrics.openCount().inc();
    metrics.closeCount().inc();
    assertEquals(2, metrics.openCount().getCount());
    assertEquals(1, metrics.closeCount().getCount());
  }

  @Test
  public void testChannelOpenCount() {
    createMetrics();
    metrics.channelOpenCount().inc();
    metrics.channelOpenCount().inc();
    metrics.channelOpenCount().inc();
    assertEquals(3, metrics.channelOpenCount().getCount());
  }

  @Test
  public void testSdkClientCountGauge() {
    createMetricsWithSdkClientCount(5);

    @SuppressWarnings("unchecked")
    Gauge<Integer> gauge =
        metricRegistry
            .getGauges()
            .get(taskMetricName(PREFIX, LIFECYCLE_SUB_DOMAIN, SDK_CLIENT_COUNT));
    assertNotNull(gauge);
    assertEquals(Integer.valueOf(5), gauge.getValue());
  }

  @Test
  public void testSdkClientCountGaugeNotRegisteredWithoutSupplier() {
    createMetrics();
    assertNull(
        metricRegistry
            .getGauges()
            .get(taskMetricName(PREFIX, LIFECYCLE_SUB_DOMAIN, SDK_CLIENT_COUNT)));
  }

  @Test
  public void testUnregisterRemovesAllMetrics() {
    createMetrics();
    assertFalse(metricRegistry.getMetrics().isEmpty());
    metrics.unregister();
    assertTrue(metricRegistry.getMetrics().isEmpty());
    metrics = null;
  }
}
