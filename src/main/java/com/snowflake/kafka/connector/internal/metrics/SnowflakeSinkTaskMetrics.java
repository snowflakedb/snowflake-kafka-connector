package com.snowflake.kafka.connector.internal.metrics;

import static com.snowflake.kafka.connector.internal.metrics.MetricsUtil.constructMetricName;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.snowflake.kafka.connector.internal.KCLogger;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * Manages task-level JMX metrics for a single {@link
 * com.snowflake.kafka.connector.SnowflakeSinkTask} instance. Metrics are registered when the task
 * starts and unregistered when it stops.
 *
 * <p>MBean ObjectNames follow the pattern:
 *
 * <pre>snowflake.kafka.connector:connector=X,pipe=task-N,category=task|lifecycle,name=metric</pre>
 */
public class SnowflakeSinkTaskMetrics implements TaskMetrics {

  private static final KCLogger LOGGER = new KCLogger(SnowflakeSinkTaskMetrics.class.getName());

  static final String TASK_SUB_DOMAIN = "task";
  static final String LIFECYCLE_SUB_DOMAIN = "lifecycle";

  // Method duration timers
  static final String PUT_DURATION = "put-duration";
  static final String PRECOMMIT_DURATION = "precommit-duration";
  static final String OPEN_DURATION = "open-duration";
  static final String CLOSE_DURATION = "close-duration";
  static final String START_DURATION = "start-duration";

  // Channel and SDK timers
  static final String CHANNEL_OPEN_DURATION = "channel-open-duration";
  static final String SDK_CLIENT_CREATE_DURATION = "sdk-client-create-duration";
  static final String PRECOMMIT_OFFSET_FETCH_DURATION = "precommit-offset-fetch-duration";

  // Throughput
  static final String PUT_RECORDS = "put-records";

  // Counters
  static final String PRECOMMIT_PARTITIONS_SKIPPED = "precommit-partitions-skipped";
  static final String OPEN_COUNT = "open-count";
  static final String CLOSE_COUNT = "close-count";
  static final String CHANNEL_OPEN_COUNT = "channel-open-count";

  // Gauges
  static final String ASSIGNED_PARTITIONS = "assigned-partitions";
  static final String SDK_CLIENT_COUNT = "sdk-client-count";

  private final String taskMetricPrefix;
  private final MetricsJmxReporter metricsJmxReporter;

  // Method duration timers
  private final Timer putDuration;
  private final Timer preCommitDuration;
  private final Timer openDuration;
  private final Timer closeDuration;
  private final Timer startDuration;

  // Channel/SDK timers (aggregated across all channels in this task)
  private final Timer channelOpenDuration;
  private final Timer sdkClientCreateDuration;
  private final Timer preCommitOffsetFetchDuration;

  // Throughput
  private final Meter putRecords;

  // Counters
  private final Counter preCommitPartitionsSkipped;
  private final Counter openCount;
  private final Counter closeCount;
  private final Counter channelOpenCount;

  // Gauges (backed by atomics)
  private final AtomicInteger assignedPartitions;

  public SnowflakeSinkTaskMetrics(
      String connectorName, String taskId, MetricsJmxReporter metricsJmxReporter) {
    this(connectorName, taskId, metricsJmxReporter, null);
  }

  public SnowflakeSinkTaskMetrics(
      String connectorName,
      String taskId,
      MetricsJmxReporter metricsJmxReporter,
      Supplier<Integer> sdkClientCountSupplier) {
    this.taskMetricPrefix = "task-" + taskId;
    this.metricsJmxReporter = metricsJmxReporter;
    this.assignedPartitions = new AtomicInteger(0);

    MetricRegistry registry = metricsJmxReporter.getMetricRegistry();

    // Method duration timers
    this.putDuration =
        registry.timer(constructMetricName(taskMetricPrefix, TASK_SUB_DOMAIN, PUT_DURATION));
    this.preCommitDuration =
        registry.timer(constructMetricName(taskMetricPrefix, TASK_SUB_DOMAIN, PRECOMMIT_DURATION));
    this.openDuration =
        registry.timer(constructMetricName(taskMetricPrefix, LIFECYCLE_SUB_DOMAIN, OPEN_DURATION));
    this.closeDuration =
        registry.timer(constructMetricName(taskMetricPrefix, LIFECYCLE_SUB_DOMAIN, CLOSE_DURATION));
    this.startDuration =
        registry.timer(constructMetricName(taskMetricPrefix, LIFECYCLE_SUB_DOMAIN, START_DURATION));

    // Channel/SDK timers
    this.channelOpenDuration =
        registry.timer(
            constructMetricName(taskMetricPrefix, LIFECYCLE_SUB_DOMAIN, CHANNEL_OPEN_DURATION));
    this.sdkClientCreateDuration =
        registry.timer(
            constructMetricName(
                taskMetricPrefix, LIFECYCLE_SUB_DOMAIN, SDK_CLIENT_CREATE_DURATION));
    this.preCommitOffsetFetchDuration =
        registry.timer(
            constructMetricName(
                taskMetricPrefix, TASK_SUB_DOMAIN, PRECOMMIT_OFFSET_FETCH_DURATION));

    // Throughput
    this.putRecords =
        registry.meter(constructMetricName(taskMetricPrefix, TASK_SUB_DOMAIN, PUT_RECORDS));

    // Counters
    this.preCommitPartitionsSkipped =
        registry.counter(
            constructMetricName(taskMetricPrefix, TASK_SUB_DOMAIN, PRECOMMIT_PARTITIONS_SKIPPED));
    this.openCount =
        registry.counter(constructMetricName(taskMetricPrefix, LIFECYCLE_SUB_DOMAIN, OPEN_COUNT));
    this.closeCount =
        registry.counter(constructMetricName(taskMetricPrefix, LIFECYCLE_SUB_DOMAIN, CLOSE_COUNT));
    this.channelOpenCount =
        registry.counter(
            constructMetricName(taskMetricPrefix, LIFECYCLE_SUB_DOMAIN, CHANNEL_OPEN_COUNT));

    // Gauges
    registry.register(
        constructMetricName(taskMetricPrefix, TASK_SUB_DOMAIN, ASSIGNED_PARTITIONS),
        (Gauge<Integer>) assignedPartitions::get);

    if (sdkClientCountSupplier != null) {
      registry.register(
          constructMetricName(taskMetricPrefix, LIFECYCLE_SUB_DOMAIN, SDK_CLIENT_COUNT),
          (Gauge<Integer>) sdkClientCountSupplier::get);
    }

    metricsJmxReporter.start();
    LOGGER.info(
        "Registered task-level JMX metrics for connector: {}, task: {}", connectorName, taskId);
  }

  // ---- TaskMetrics interface (timing) ----

  @Override
  public TimingContext timePut() {
    return wrap(putDuration);
  }

  @Override
  public TimingContext timePreCommit() {
    return wrap(preCommitDuration);
  }

  @Override
  public TimingContext timeOpen() {
    return wrap(openDuration);
  }

  @Override
  public TimingContext timeClose() {
    return wrap(closeDuration);
  }

  @Override
  public TimingContext timeSdkClientCreate() {
    return wrap(sdkClientCreateDuration);
  }

  @Override
  public TimingContext timeChannelOpen() {
    return wrap(channelOpenDuration);
  }

  @Override
  public TimingContext timeOffsetFetch() {
    return wrap(preCommitOffsetFetchDuration);
  }

  @Override
  public void recordStartDuration(long nanos) {
    startDuration.update(nanos, TimeUnit.NANOSECONDS);
  }

  // ---- TaskMetrics interface (counters) ----

  @Override
  public void incOpenCount() {
    openCount.inc();
  }

  @Override
  public void incCloseCount() {
    closeCount.inc();
  }

  @Override
  public void incChannelOpenCount() {
    channelOpenCount.inc();
  }

  @Override
  public void incPreCommitPartitionsSkipped() {
    preCommitPartitionsSkipped.inc();
  }

  // ---- TaskMetrics interface (throughput) ----

  @Override
  public void markPutRecords(long count) {
    putRecords.mark(count);
  }

  // ---- TaskMetrics interface (gauges) ----

  @Override
  public void setAssignedPartitions(int count) {
    assignedPartitions.set(count);
  }

  // ---- TaskMetrics interface (lifecycle) ----

  @Override
  public void unregister() {
    metricsJmxReporter.removeMetricsFromRegistry(taskMetricPrefix);
    LOGGER.info("Unregistered task-level JMX metrics for prefix: {}", taskMetricPrefix);
  }

  // ---- raw accessors (package-private, for tests in the same package) ----

  Timer putDuration() {
    return putDuration;
  }

  Timer preCommitDuration() {
    return preCommitDuration;
  }

  Timer openDuration() {
    return openDuration;
  }

  Timer closeDuration() {
    return closeDuration;
  }

  Timer startDuration() {
    return startDuration;
  }

  Timer channelOpenDuration() {
    return channelOpenDuration;
  }

  Timer sdkClientCreateDuration() {
    return sdkClientCreateDuration;
  }

  Timer preCommitOffsetFetchDuration() {
    return preCommitOffsetFetchDuration;
  }

  Meter putRecords() {
    return putRecords;
  }

  Counter preCommitPartitionsSkipped() {
    return preCommitPartitionsSkipped;
  }

  Counter openCount() {
    return openCount;
  }

  Counter closeCount() {
    return closeCount;
  }

  Counter channelOpenCount() {
    return channelOpenCount;
  }

  int getAssignedPartitions() {
    return assignedPartitions.get();
  }

  // ---- internal ----

  private static TimingContext wrap(Timer timer) {
    Timer.Context ctx = timer.time();
    return ctx::stop;
  }
}
