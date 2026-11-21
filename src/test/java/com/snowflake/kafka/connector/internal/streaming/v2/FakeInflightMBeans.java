package com.snowflake.kafka.connector.internal.streaming.v2;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;
import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.AttributeNotFoundException;
import javax.management.DynamicMBean;
import javax.management.JMException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanException;
import javax.management.MBeanInfo;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.ReflectionException;

/**
 * Registers stand-in snowpipe-streaming per-channel gauges on the platform MBeanServer, under
 * ObjectNames built here independently of production code. Each gauge exposes a {@code double}
 * {@code Value} attribute like the SDK bean. {@link #close()} unregisters every bean this instance
 * registered.
 */
final class FakeInflightMBeans implements AutoCloseable {

  static final String ROWS = "inflight_rows";
  static final String BYTES = "inflight_bytes";

  /** Produces a gauge {@code Value}, or throws the way the SDK bean does when it cannot. */
  interface ValueSource {
    Object get() throws AttributeNotFoundException, MBeanException, ReflectionException;
  }

  private final MBeanServer server = ManagementFactory.getPlatformMBeanServer();
  private final List<ObjectName> registered = new ArrayList<>();

  /** The SDK layout: domain {@code snowflake.ingest.sdk}, every key value quoted. */
  static ObjectName sdkObjectName(String clientName, String channelName, String metric) {
    try {
      return new ObjectName(
          "snowflake.ingest.sdk:clientName="
              + ObjectName.quote(clientName)
              + ",channel="
              + ObjectName.quote(channelName)
              + ",name="
              + ObjectName.quote(metric));
    } catch (MalformedObjectNameException e) {
      throw new IllegalArgumentException(e);
    }
  }

  FakeInflightMBeans channel(String clientName, String channelName, long rows, long bytes) {
    gauge(clientName, channelName, ROWS, rows);
    gauge(clientName, channelName, BYTES, bytes);
    return this;
  }

  FakeInflightMBeans gauge(String clientName, String channelName, String metric, double value) {
    return register(sdkObjectName(clientName, channelName, metric), () -> value);
  }

  FakeInflightMBeans register(ObjectName name, ValueSource source) {
    try {
      server.registerMBean(new Gauge(source), name);
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

  private static final class Gauge implements DynamicMBean {
    private static final MBeanInfo INFO =
        new MBeanInfo(
            Gauge.class.getName(),
            "fake gauge",
            new MBeanAttributeInfo[] {
              new MBeanAttributeInfo("Value", "double", "Value", true, false, false)
            },
            null,
            null,
            null);

    private final ValueSource source;

    Gauge(ValueSource source) {
      this.source = source;
    }

    @Override
    public Object getAttribute(String attribute)
        throws AttributeNotFoundException, MBeanException, ReflectionException {
      if (!"Value".equals(attribute)) {
        throw new AttributeNotFoundException(attribute);
      }
      return source.get();
    }

    @Override
    public void setAttribute(Attribute attribute) throws AttributeNotFoundException {
      throw new AttributeNotFoundException(attribute.getName());
    }

    @Override
    public AttributeList getAttributes(String[] attributes) {
      return new AttributeList();
    }

    @Override
    public AttributeList setAttributes(AttributeList attributes) {
      return new AttributeList();
    }

    @Override
    public Object invoke(String actionName, Object[] params, String[] signature)
        throws ReflectionException {
      throw new ReflectionException(new NoSuchMethodException(actionName));
    }

    @Override
    public MBeanInfo getMBeanInfo() {
      return INFO;
    }
  }
}
