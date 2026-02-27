package com.snowflake.kafka.connector.internal.metrics;

/** Null-object implementation of {@link TaskMetrics}. Every method is a no-op. */
enum NoopTaskMetrics implements TaskMetrics {
  INSTANCE;

  @Override
  public TimingContext timePut() {
    return TimingContext.NOOP;
  }

  @Override
  public TimingContext timePreCommit() {
    return TimingContext.NOOP;
  }

  @Override
  public TimingContext timeOpen() {
    return TimingContext.NOOP;
  }

  @Override
  public TimingContext timeClose() {
    return TimingContext.NOOP;
  }

  @Override
  public TimingContext timeSdkClientCreate() {
    return TimingContext.NOOP;
  }

  @Override
  public TimingContext timeChannelOpen() {
    return TimingContext.NOOP;
  }

  @Override
  public TimingContext timeOffsetFetch() {
    return TimingContext.NOOP;
  }

  @Override
  public void recordStartDuration(long nanos) {}

  @Override
  public void incOpenCount() {}

  @Override
  public void incCloseCount() {}

  @Override
  public void incChannelOpenCount() {}

  @Override
  public void incPreCommitPartitionsSkipped() {}

  @Override
  public void markPutRecords(long count) {}

  @Override
  public void setAssignedPartitions(int count) {}

  @Override
  public void unregister() {}
}
