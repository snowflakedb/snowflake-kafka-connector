package com.snowflake.kafka.connector.internal.streaming.v2;

import com.google.common.annotations.VisibleForTesting;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import org.apache.kafka.connect.errors.ConnectException;

/**
 * Reads the per-channel inflight rows and bytes that snowpipe-streaming tracks: what a channel
 * instance has accepted and Snowflake has not yet acknowledged.
 *
 * <p>Contract of the SDK reader: a read includes everything this task has appended to the channel
 * instance, and reflects Snowflake acknowledgements within about 100 ms, or throws {@link
 * IllegalStateException} when no current value exists. A late acknowledgement can only overstate
 * usage. A limit checked against these values before appending can therefore be exceeded by at most
 * what is appended after the check.
 */
public final class ChannelInflightMetrics {

  /** SDK class that provides the inflight readers and reports its metrics sink. */
  static final String SDK_JMX_CLASS = "com.snowflake.ingest.streaming.JmxMetrics";

  /**
   * Static method of {@link #SDK_JMX_CLASS} that reports whether the SDK's metrics sink is live.
   */
  static final String SDK_SINK_CHECK_METHOD = "isMetricsSinkRegistered";

  /**
   * Static method of {@link #SDK_JMX_CLASS} that returns the inflight reader of a channel instance.
   */
  static final String SDK_READER_METHOD = "inflightReader";

  /** Inflight rows and bytes of one SDK channel instance. */
  public interface InflightReader {
    /**
     * @throws IllegalStateException if no current value exists
     */
    long rows();

    /**
     * @throws IllegalStateException if no current value exists
     */
    long bytes();
  }

  /** Resolves the inflight reader of an SDK channel instance. */
  @FunctionalInterface
  public interface InflightReaderFactory {
    InflightReader forChannel(SnowflakeStreamingIngestChannel channel);
  }

  /** Readers of the snowpipe-streaming SDK on the classpath. */
  public static final InflightReaderFactory SDK_READERS =
      channel -> new SdkInflightReader(SdkReaderHandles.reader(channel));

  private ChannelInflightMetrics() {}

  /**
   * Readers of the inflight reader API of {@code sdkClass}, through method handles resolved from it
   * the same way {@link #SDK_READERS} resolves them from the SDK.
   */
  @VisibleForTesting
  static InflightReaderFactory readersOf(Class<?> sdkClass) throws ReflectiveOperationException {
    MethodHandle[] handles = SdkReaderHandles.resolve(sdkClass);
    return channel -> {
      Object sdkReader = invoke(() -> (Object) handles[0].invokeExact(channel));
      return new InflightReader() {
        @Override
        public long rows() {
          return invoke(() -> (long) handles[1].invokeExact(sdkReader));
        }

        @Override
        public long bytes() {
          return invoke(() -> (long) handles[2].invokeExact(sdkReader));
        }
      };
    };
  }

  private interface HandleCall<T> {
    T call() throws Throwable;
  }

  private static <T> T invoke(HandleCall<T> call) {
    try {
      return call.call();
    } catch (RuntimeException | Error e) {
      throw e;
    } catch (Throwable e) {
      throw new IllegalStateException("Inflight reader call failed", e);
    }
  }

  /**
   * Fails when either task inflight cap is enabled and the snowpipe-streaming SDK on the classpath
   * does not provide inflight readers, or its metrics sink did not register. The check bootstraps
   * the SDK core, so SDK bootstrap settings must be applied first.
   *
   * @throws ConnectException naming both cap config keys, or if the sink check itself fails
   */
  public static void requireSdkSupport(long maxRows, long maxBytes) {
    requireSdkSupport(maxRows, maxBytes, SDK_JMX_CLASS);
  }

  static void requireSdkSupport(long maxRows, long maxBytes, String sdkClassName) {
    if (maxRows <= 0 && maxBytes <= 0) {
      return;
    }
    final Class<?> sdkClass;
    try {
      sdkClass = Class.forName(sdkClassName, false, ChannelInflightMetrics.class.getClassLoader());
    } catch (ClassNotFoundException e) {
      throw new ConnectException(
          String.format(
              "%s and %s require a snowpipe-streaming SDK that reports per-channel inflight usage,"
                  + " but %s is not on the classpath. Set both to 0 or run a connector version"
                  + " bundling such an SDK.",
              KafkaConnectorConfigParams.SNOWFLAKE_STREAMING_MAX_TASK_INFLIGHT_ROWS,
              KafkaConnectorConfigParams.SNOWFLAKE_STREAMING_MAX_TASK_INFLIGHT_MEGABYTES,
              sdkClassName),
          e);
    }
    try {
      SdkReaderHandles.resolve(sdkClass);
    } catch (ReflectiveOperationException e) {
      throw new ConnectException(
          String.format(
              "%s and %s require a snowpipe-streaming SDK whose %s provides %s(channel) with"
                  + " rows() and bytes(), but it does not. Set both to 0 or run a connector"
                  + " version bundling such an SDK.",
              KafkaConnectorConfigParams.SNOWFLAKE_STREAMING_MAX_TASK_INFLIGHT_ROWS,
              KafkaConnectorConfigParams.SNOWFLAKE_STREAMING_MAX_TASK_INFLIGHT_MEGABYTES,
              sdkClassName,
              SDK_READER_METHOD),
          e);
    }
    final Method sinkCheck;
    try {
      sinkCheck = sdkClass.getMethod(SDK_SINK_CHECK_METHOD);
    } catch (NoSuchMethodException e) {
      throw new ConnectException(
          String.format(
              "%s and %s require a snowpipe-streaming SDK that reports whether its JMX metrics"
                  + " sink is live, but %s has no %s(). Set both to 0 or run a connector version"
                  + " bundling such an SDK.",
              KafkaConnectorConfigParams.SNOWFLAKE_STREAMING_MAX_TASK_INFLIGHT_ROWS,
              KafkaConnectorConfigParams.SNOWFLAKE_STREAMING_MAX_TASK_INFLIGHT_MEGABYTES,
              sdkClassName,
              SDK_SINK_CHECK_METHOD),
          e);
    }
    final Object registered;
    try {
      registered = sinkCheck.invoke(null);
    } catch (InvocationTargetException e) {
      throw new ConnectException(
          "Failed to check the snowpipe-streaming JMX metrics sink", e.getCause());
    } catch (IllegalAccessException e) {
      throw new ConnectException("Failed to check the snowpipe-streaming JMX metrics sink", e);
    }
    if (!Boolean.TRUE.equals(registered)) {
      throw new ConnectException(
          String.format(
              "%s and %s need the snowpipe-streaming JMX metrics sink, which failed to register"
                  + " when the SDK core bootstrapped, so per-channel inflight usage is not"
                  + " updated. Check the worker log for \"Curated metrics are not pushed to"
                  + " Java\", or set both to 0.",
              KafkaConnectorConfigParams.SNOWFLAKE_STREAMING_MAX_TASK_INFLIGHT_ROWS,
              KafkaConnectorConfigParams.SNOWFLAKE_STREAMING_MAX_TASK_INFLIGHT_MEGABYTES));
    }
  }

  /** A reader of the SDK on the classpath, read through {@link SdkReaderHandles}. */
  private static final class SdkInflightReader implements InflightReader {
    private final Object sdkReader;

    SdkInflightReader(Object sdkReader) {
      this.sdkReader = sdkReader;
    }

    @Override
    public long rows() {
      return SdkReaderHandles.rows(sdkReader);
    }

    @Override
    public long bytes() {
      return SdkReaderHandles.bytes(sdkReader);
    }
  }

  /**
   * Method handles of the SDK reader API, resolved once when first used. They are static finals so
   * the JIT can inline the calls. With an SDK that lacks the API they stay null, and {@link
   * #requireSdkSupport} fails before any read when a cap is enabled.
   */
  private static final class SdkReaderHandles {
    private static final MethodHandle READER;
    private static final MethodHandle ROWS;
    private static final MethodHandle BYTES;

    static {
      MethodHandle[] handles = null;
      try {
        handles =
            resolve(
                Class.forName(SDK_JMX_CLASS, false, ChannelInflightMetrics.class.getClassLoader()));
      } catch (ReflectiveOperationException | LinkageError e) {
        // Reported by requireSdkSupport, which runs before any read when a cap is enabled.
      }
      READER = handles == null ? null : handles[0];
      ROWS = handles == null ? null : handles[1];
      BYTES = handles == null ? null : handles[2];
    }

    /**
     * Resolves {@code inflightReader(SnowflakeStreamingIngestChannel)} of {@code sdkClass} and
     * {@code rows()} and {@code bytes()} of the reader it returns, typed on {@link Object}.
     */
    static MethodHandle[] resolve(Class<?> sdkClass) throws ReflectiveOperationException {
      Class<?> readerClass =
          sdkClass
              .getMethod(SDK_READER_METHOD, SnowflakeStreamingIngestChannel.class)
              .getReturnType();
      MethodHandles.Lookup lookup = MethodHandles.publicLookup();
      return new MethodHandle[] {
        lookup
            .findStatic(
                sdkClass,
                SDK_READER_METHOD,
                MethodType.methodType(readerClass, SnowflakeStreamingIngestChannel.class))
            .asType(MethodType.methodType(Object.class, SnowflakeStreamingIngestChannel.class)),
        lookup
            .findVirtual(readerClass, "rows", MethodType.methodType(long.class))
            .asType(MethodType.methodType(long.class, Object.class)),
        lookup
            .findVirtual(readerClass, "bytes", MethodType.methodType(long.class))
            .asType(MethodType.methodType(long.class, Object.class))
      };
    }

    static Object reader(SnowflakeStreamingIngestChannel channel) {
      if (READER == null) {
        throw new IllegalStateException(
            "the snowpipe-streaming SDK on the classpath provides no inflight readers");
      }
      try {
        return (Object) READER.invokeExact(channel);
      } catch (RuntimeException | Error e) {
        throw e;
      } catch (Throwable e) {
        throw new IllegalStateException("Failed to resolve the inflight reader", e);
      }
    }

    static long rows(Object sdkReader) {
      try {
        return (long) ROWS.invokeExact(sdkReader);
      } catch (RuntimeException | Error e) {
        throw e;
      } catch (Throwable e) {
        throw new IllegalStateException("Failed to read inflight rows", e);
      }
    }

    static long bytes(Object sdkReader) {
      try {
        return (long) BYTES.invokeExact(sdkReader);
      } catch (RuntimeException | Error e) {
        throw e;
      } catch (Throwable e) {
        throw new IllegalStateException("Failed to read inflight bytes", e);
      }
    }
  }
}
