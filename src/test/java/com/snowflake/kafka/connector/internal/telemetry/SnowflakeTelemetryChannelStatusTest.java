package com.snowflake.kafka.connector.internal.telemetry;

import static com.snowflake.kafka.connector.internal.TestUtils.TEST_CONNECTOR_NAME;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.codahale.metrics.MetricRegistry;
import com.snowflake.kafka.connector.internal.metrics.MetricsJmxReporter;
import com.snowflake.kafka.connector.internal.streaming.telemetry.SnowflakeTelemetryChannelStatus;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Test;
import org.mockito.Mockito;

public class SnowflakeTelemetryChannelStatusTest {
  private final String tableName = "tableName";
  private final String connectorName = "connectorName";
  private final String channelName = "channelName";

  @Test
  public void testRegisterAndUnregisterJmxMetrics() {
    MetricRegistry metricRegistry = Mockito.spy(MetricRegistry.class);
    MetricsJmxReporter metricsJmxReporter =
        Mockito.spy(new MetricsJmxReporter(metricRegistry, TEST_CONNECTOR_NAME));

    // test register
    SnowflakeTelemetryChannelStatus snowflakeTelemetryChannelStatus =
        new SnowflakeTelemetryChannelStatus(
            tableName,
            connectorName,
            channelName,
            1234,
            true,
            metricsJmxReporter,
            new AtomicLong(-1),
            new AtomicLong(-1),
            new AtomicLong(-1));
    verify(metricsJmxReporter, times(1)).start();
    verify(metricRegistry, times((int) SnowflakeTelemetryChannelStatus.NUM_METRICS))
        .register(Mockito.anyString(), Mockito.any());
    verify(metricsJmxReporter, times(1)).removeMetricsFromRegistry(channelName);

    // test unregister
    snowflakeTelemetryChannelStatus.tryUnregisterChannelJMXMetrics();
    verify(metricsJmxReporter, times(2)).removeMetricsFromRegistry(channelName);
  }

  @Test
  public void testDisabledJmx() {
    MetricRegistry metricRegistry = Mockito.spy(MetricRegistry.class);
    MetricsJmxReporter metricsJmxReporter =
        Mockito.spy(new MetricsJmxReporter(metricRegistry, TEST_CONNECTOR_NAME));

    // test register
    SnowflakeTelemetryChannelStatus snowflakeTelemetryChannelStatus =
        new SnowflakeTelemetryChannelStatus(
            tableName,
            connectorName,
            channelName,
            1234,
            false,
            metricsJmxReporter,
            new AtomicLong(-1),
            new AtomicLong(-1),
            new AtomicLong(-1));
    verify(metricsJmxReporter, times(0)).start();
    verify(metricRegistry, times(0)).register(Mockito.anyString(), Mockito.any());
    verify(metricsJmxReporter, times(0)).removeMetricsFromRegistry(channelName);

    // test unregister
    snowflakeTelemetryChannelStatus.tryUnregisterChannelJMXMetrics();
    verify(metricsJmxReporter, times(1)).removeMetricsFromRegistry(channelName);
  }

  @Test
  public void testInvalidJmxReporter() {
    // invalid jmx reporter should not error out
    SnowflakeTelemetryChannelStatus snowflakeTelemetryChannelStatus =
        new SnowflakeTelemetryChannelStatus(
            tableName,
            connectorName,
            channelName,
            1234,
            true,
            null,
            new AtomicLong(-1),
            new AtomicLong(-1),
            new AtomicLong(-1));
    snowflakeTelemetryChannelStatus.tryUnregisterChannelJMXMetrics();
  }
}
