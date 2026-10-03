package com.snowflake.kafka.connector.internal.streaming.v2;

import com.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import java.lang.reflect.Method;

/**
 * Reads {@code SnowflakeStreamingIngestChannel.getInFlightBytes()} when the bundled
 * snowpipe-streaming JAR exposes that method.
 *
 * <p>The connector currently depends on snowpipe-streaming 1.8.1, which does not have the method.
 * Reflection keeps this module compiling against 1.8.1 and automatically picks the API up after the
 * SDK (and then this connector's SDK pin) is upgraded. Until then, {@link #isSupported()} is false
 * and a configured in-flight cap fails fast at task start.
 */
public final class InFlightBytes {

  private static final Method METHOD = resolve();

  private InFlightBytes() {}

  public static boolean isSupported() {
    return METHOD != null;
  }

  public static long from(SnowflakeStreamingIngestChannel channel) {
    if (channel == null || METHOD == null) {
      return 0L;
    }
    try {
      Object value = METHOD.invoke(channel);
      return value instanceof Number ? ((Number) value).longValue() : 0L;
    } catch (ReflectiveOperationException e) {
      return 0L;
    }
  }

  private static Method resolve() {
    try {
      Method method = SnowflakeStreamingIngestChannel.class.getMethod("getInFlightBytes");
      if (method.getReturnType() == long.class
          || Number.class.isAssignableFrom(method.getReturnType())) {
        return method;
      }
      return null;
    } catch (NoSuchMethodException e) {
      return null;
    }
  }
}
