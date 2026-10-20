package com.snowflake.kafka.connector.internal.streaming.v2;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;
import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.StandardMBean;

/**
 * Registers stand-in snowpipe-streaming {@code inflight_rows} and {@code inflight_bytes} gauges on
 * the platform MBeanServer. {@link #close()} unregisters every bean this instance registered.
 */
final class FakeInflightMBeans implements AutoCloseable {

  /** Management interface exposing a single read-only {@code Value} attribute. */
  public interface GaugeMBean {
    double getValue();
  }

  private static final class Gauge implements GaugeMBean {
    private final double value;

    Gauge(double value) {
      this.value = value;
    }

    @Override
    public double getValue() {
      return value;
    }
  }

  private final MBeanServer server = ManagementFactory.getPlatformMBeanServer();
  private final List<ObjectName> registered = new ArrayList<>();

  FakeInflightMBeans channel(String clientName, String channelName, long rows, long bytes) {
    gauge(clientName, channelName, ChannelInflightMetrics.INFLIGHT_ROWS, rows);
    gauge(clientName, channelName, ChannelInflightMetrics.INFLIGHT_BYTES, bytes);
    return this;
  }

  FakeInflightMBeans gauge(String clientName, String channelName, String metric, double value) {
    ObjectName name = ChannelInflightMetrics.objectName(clientName, channelName, metric);
    try {
      server.registerMBean(new StandardMBean(new Gauge(value), GaugeMBean.class), name);
    } catch (JMException e) {
      throw new IllegalStateException("Failed to register " + name, e);
    }
    registered.add(name);
    return this;
  }

  @Override
  public void close() throws JMException {
    for (ObjectName name : registered) {
      server.unregisterMBean(name);
    }
    registered.clear();
  }
}
