package com.snowflake.kafka.connector.internal.streaming.v2;

import com.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import java.util.function.Supplier;

/**
 * Stand-ins for the snowpipe-streaming {@code JmxMetrics} API, each with a different subset of its
 * static methods. They are public at the top level because the connector resolves them through
 * {@code MethodHandles.publicLookup()}, like the SDK classes.
 */
public final class FakeInflightSdk {

  private FakeInflightSdk() {}

  /** What every fake {@code inflightReader} returns. */
  static volatile Supplier<Reader> readers = Reader::new;

  /** A fake {@code InflightReader}: returns its fields, or throws {@link #failure}. */
  public static final class Reader {
    public volatile long rows;
    public volatile long bytes;
    public volatile RuntimeException failure;

    public long rows() {
      if (failure != null) {
        throw failure;
      }
      return rows;
    }

    public long bytes() {
      if (failure != null) {
        throw failure;
      }
      return bytes;
    }
  }

  static Reader readerFor(SnowflakeStreamingIngestChannel channel) {
    if (channel == null) {
      throw new IllegalArgumentException("channel is null");
    }
    return readers.get();
  }

  /** An SDK with readers whose metrics sink registered at bootstrap. */
  public static final class SinkRegistered {
    public static Reader inflightReader(SnowflakeStreamingIngestChannel channel) {
      return readerFor(channel);
    }

    public static boolean isMetricsSinkRegistered() {
      return true;
    }
  }

  /** An SDK with readers whose metrics sink failed to register at bootstrap. */
  public static final class SinkFailed {
    public static Reader inflightReader(SnowflakeStreamingIngestChannel channel) {
      return readerFor(channel);
    }

    public static boolean isMetricsSinkRegistered() {
      return false;
    }
  }

  /** An SDK with readers that cannot report its metrics sink. */
  public static final class WithoutSinkCheck {
    public static Reader inflightReader(SnowflakeStreamingIngestChannel channel) {
      return readerFor(channel);
    }
  }

  /** An SDK with readers whose metrics sink check fails. */
  public static final class SinkCheckThrows {
    public static Reader inflightReader(SnowflakeStreamingIngestChannel channel) {
      return readerFor(channel);
    }

    public static boolean isMetricsSinkRegistered() {
      throw new IllegalStateException("bootstrap failed");
    }
  }

  /** An SDK that reports its metrics sink but provides no inflight readers. */
  public static final class WithoutReader {
    public static boolean isMetricsSinkRegistered() {
      return true;
    }
  }
}
