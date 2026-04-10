package com.snowflake.kafka.connector.internal.telemetry;

import static com.snowflake.kafka.connector.internal.TestUtils.TEST_CONNECTOR_NAME;
import static com.snowflake.kafka.connector.internal.metrics.MetricsUtil.channelMetricPrefix;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.codahale.metrics.MetricRegistry;
import com.snowflake.kafka.connector.internal.metrics.MetricsJmxReporter;
import com.snowflake.kafka.connector.internal.streaming.telemetry.SnowflakeTelemetryChannelStatus;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.ObjectMapper;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.node.ObjectNode;
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

    SnowflakeTelemetryChannelStatus status =
        new SnowflakeTelemetryChannelStatus(
            tableName,
            connectorName,
            channelName,
            1234,
            Optional.of(metricsJmxReporter),
            new AtomicLong(-1),
            new AtomicLong(-1),
            new AtomicLong(-1));

    // Registration: 4 metrics registered, start() NOT called (handled at task level)
    verify(metricsJmxReporter, times(0)).start();
    verify(metricRegistry, times((int) SnowflakeTelemetryChannelStatus.NUM_METRICS))
        .register(Mockito.anyString(), Mockito.any());

    // No removeMatching scan should have been called during registration
    verify(metricsJmxReporter, times(0)).removeMetricsFromRegistry(Mockito.anyString());

    // Unregister: uses targeted removal (4 individual remove calls)
    status.tryUnregisterChannelJMXMetrics();
    verify(metricRegistry, times((int) SnowflakeTelemetryChannelStatus.NUM_METRICS))
        .remove(Mockito.anyString());
  }

  @Test
  public void testDisabledJmx() {
    MetricRegistry metricRegistry = Mockito.spy(MetricRegistry.class);
    MetricsJmxReporter metricsJmxReporter =
        Mockito.spy(new MetricsJmxReporter(metricRegistry, TEST_CONNECTOR_NAME));

    SnowflakeTelemetryChannelStatus snowflakeTelemetryChannelStatus =
        new SnowflakeTelemetryChannelStatus(
            tableName,
            connectorName,
            channelName,
            1234,
            Optional.empty(),
            new AtomicLong(-1),
            new AtomicLong(-1),
            new AtomicLong(-1));
    verify(metricsJmxReporter, times(0)).start();
    verify(metricRegistry, times(0)).register(Mockito.anyString(), Mockito.any());
    verify(metricsJmxReporter, times(0))
        .removeMetricsFromRegistry(channelMetricPrefix(channelName));

    snowflakeTelemetryChannelStatus.tryUnregisterChannelJMXMetrics();
    verify(metricsJmxReporter, times(0))
        .removeMetricsFromRegistry(channelMetricPrefix(channelName));
  }

  @Test
  public void testValidationFailureCountInDumpTo() {
    SnowflakeTelemetryChannelStatus status =
        new SnowflakeTelemetryChannelStatus(
            tableName,
            connectorName,
            channelName,
            1234,
            Optional.empty(),
            new AtomicLong(-1),
            new AtomicLong(-1),
            new AtomicLong(-1));

    // Initially zero
    ObjectNode msg = new ObjectMapper().createObjectNode();
    status.dumpTo(msg);
    assertEquals(0, msg.get(TelemetryConstants.VALIDATION_FAILURE_COUNT).asLong());

    // Increment and verify
    status.incValidationFailureCount();
    status.incValidationFailureCount();
    status.incValidationFailureCount();

    msg = new ObjectMapper().createObjectNode();
    status.dumpTo(msg);
    assertEquals(3, msg.get(TelemetryConstants.VALIDATION_FAILURE_COUNT).asLong());
  }
}
