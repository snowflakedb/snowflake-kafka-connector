package com.snowflake.kafka.connector.internal.metrics;

/**
 * Task-level metrics facade. Callers program against this interface; the connector wires in either
 * {@link SnowflakeSinkTaskMetrics} (real JMX) or the singleton returned by {@link #noop()} when
 * monitoring is disabled.
 *
 * <p>All methods are safe to call unconditionally -- the noop implementation is a no-op.
 */
public interface TaskMetrics {

  // ---- timing (try-with-resources) ----

  TimingContext timePut();

  TimingContext timePreCommit();

  TimingContext timeOpen();

  TimingContext timeClose();

  TimingContext timeSdkClientCreate();

  TimingContext timeChannelOpen();

  TimingContext timeOffsetFetch();

  void recordStartDuration(long nanos);

  // ---- counters ----

  void incOpenCount();

  void incCloseCount();

  void incChannelOpenCount();

  void incPreCommitPartitionsSkipped();

  // ---- throughput ----

  void markPutRecords(long count);

  // ---- gauges ----

  void setAssignedPartitions(int count);

  // ---- lifecycle ----

  void unregister();

  // ---- timing context ----

  @FunctionalInterface
  interface TimingContext extends AutoCloseable {
    TimingContext NOOP = () -> {};

    @Override
    void close();
  }

  // ---- factory ----

  static TaskMetrics noop() {
    return NoopTaskMetrics.INSTANCE;
  }
}
