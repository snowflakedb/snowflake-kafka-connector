package com.snowflake.kafka.connector.internal.metrics;

import static org.junit.Assert.*;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import org.junit.Before;
import org.junit.Test;

public class MetricsJmxReporterTest {
  private MetricRegistry metricRegistry;
  private MetricsJmxReporter reporter;

  @Before
  public void setUp() {
    metricRegistry = new MetricRegistry();
    reporter = new MetricsJmxReporter(metricRegistry, "testConnector");
  }

  @Test
  public void testRemoveMetricByExactName() {
    metricRegistry.register("channel:ch1/offsets/processed-offset", (Gauge<Long>) () -> 42L);
    metricRegistry.register("channel:ch1/offsets/persisted-offset", (Gauge<Long>) () -> 10L);
    metricRegistry.register("channel:ch2/offsets/processed-offset", (Gauge<Long>) () -> 99L);

    assertEquals(3, metricRegistry.getMetrics().size());

    reporter.removeMetric("channel:ch1/offsets/processed-offset");

    assertEquals(2, metricRegistry.getMetrics().size());
    assertNull(metricRegistry.getGauges().get("channel:ch1/offsets/processed-offset"));
    assertNotNull(metricRegistry.getGauges().get("channel:ch1/offsets/persisted-offset"));
    assertNotNull(metricRegistry.getGauges().get("channel:ch2/offsets/processed-offset"));
  }

  @Test
  public void testRemoveMetricNonexistentIsNoOp() {
    metricRegistry.register("channel:ch1/offsets/processed-offset", (Gauge<Long>) () -> 42L);
    reporter.removeMetric("channel:nonexistent/offsets/foo");
    assertEquals(1, metricRegistry.getMetrics().size());
  }

  @Test
  public void testRemoveMetricsFromRegistryStillWorks() {
    metricRegistry.register("channel:ch1/offsets/a", (Gauge<Long>) () -> 1L);
    metricRegistry.register("channel:ch1/offsets/b", (Gauge<Long>) () -> 2L);
    metricRegistry.register("channel:ch2/offsets/a", (Gauge<Long>) () -> 3L);

    reporter.removeMetricsFromRegistry("channel:ch1");

    assertEquals(1, metricRegistry.getMetrics().size());
    assertNotNull(metricRegistry.getGauges().get("channel:ch2/offsets/a"));
  }
}
