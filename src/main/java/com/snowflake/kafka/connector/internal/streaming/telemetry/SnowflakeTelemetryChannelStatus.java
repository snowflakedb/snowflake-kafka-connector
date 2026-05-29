/*
 * Copyright (c) 2023 Snowflake Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.snowflake.kafka.connector.internal.streaming.telemetry;

import static com.snowflake.kafka.connector.internal.metrics.MetricsUtil.channelMetricName;
import static com.snowflake.kafka.connector.internal.streaming.channel.TopicPartitionChannel.NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.google.common.annotations.VisibleForTesting;
import com.snowflake.ingest.streaming.ChannelStatus;
import com.snowflake.kafka.connector.internal.metrics.MetricsJmxReporter;
import com.snowflake.kafka.connector.internal.metrics.MetricsUtil;
import com.snowflake.kafka.connector.internal.telemetry.SnowflakeTelemetryBasicInfo;
import com.snowflake.kafka.connector.internal.telemetry.SnowflakeTelemetryService;
import com.snowflake.kafka.connector.internal.telemetry.TelemetryConstants;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Extension of {@link SnowflakeTelemetryBasicInfo} class used to send data to snowflake when the
 * TopicPartitionChannel closes. Also creates and registers various metrics with JMX
 *
 * <p>Most of the data sent to Snowflake is aggregated data.
 */
public class SnowflakeTelemetryChannelStatus extends SnowflakeTelemetryBasicInfo {
  public static final long NUM_METRICS = 4; // update when new metrics are added

  static final String CHANNEL_RECOVERY_COUNT = "channel-recovery-count";

  // channel properties
  private final String connectorName;
  private final String channelName;
  private final Optional<MetricsJmxReporter> metricsJmxReporter;
  private final long channelCreationTime;

  // offsets
  private final AtomicLong offsetPersistedInSnowflake;
  private final AtomicLong processedOffset;
  private final AtomicLong latestConsumerOffset;

  // channel recovery counter (always tracked; also registered as JMX gauge if enabled)
  private final AtomicLong recoveryCount = new AtomicLong(0);

  // Client recreation counters — incremented from openChannelWithClientRecovery when a
  // client-invalid SDK error (HTTP 410 / pipe failover) triggers a swap of the streaming client.
  // attempts is bumped on entry to the recreate branch; success/failure split records whether
  // the underlying StreamingClientPools.recreateClient retry budget held.
  private final AtomicLong clientRecreationAttemptCount = new AtomicLong(0);
  private final AtomicLong clientRecreationSuccessCount = new AtomicLong(0);
  private final AtomicLong clientRecreationFailureCount = new AtomicLong(0);

  // Aggregated count of client-side validation failures for this channel.
  // Reported in channel status telemetry on close, avoiding per-record telemetry overhead.
  private final AtomicLong validationFailureCount = new AtomicLong(0);

  // Count of records where errors were tolerated (errors.tolerance=all) instead of failing the
  // task.
  private final AtomicLong errorToleratedCount = new AtomicLong(0);

  // Whether client-side validation was silently disabled due to initialization failure.
  private volatile boolean validationDisabled = false;

  // Latest SDK-reported metrics, updated on each processChannelStatus call.
  // Using volatile (not AtomicLong) since these are set, never atomically incremented.
  private volatile long rowsInsertedCount;
  private volatile long rowsParsedCount;
  private volatile long rowsErrorCount;
  private volatile long serverAvgProcessingLatencyMs = -1;

  // SDK ChannelStatus identity and error fields, updated on each processChannelStatus call.
  private volatile String databaseName;
  private volatile String schemaName;
  private volatile String pipeName;
  private volatile String statusCode;
  private volatile String lastErrorTimestamp;
  private volatile String lastErrorOffsetTokenUpperBound;

  // Counts of SDK backpressure retries and channel-reopen fallbacks during appendRow.
  private final AtomicLong backpressureRetryCount = new AtomicLong(0);
  private final AtomicLong appendRowFallbackCount = new AtomicLong(0);
  private final AtomicLong schemaEvolutionFailureCount = new AtomicLong(0);

  private volatile String[] registeredMetricNames;

  /**
   * Creates a new object tracking {@link
   * com.snowflake.kafka.connector.internal.streaming.channel.TopicPartitionChannel} metrics with
   * JMX and send telemetry data to snowflake
   *
   * @param tableName the table the channel is ingesting to
   * @param channelName the name of the TopicPartitionChannel to track
   * @param metricsJmxReporter JMX reporter; present enables channel-level metrics, empty disables
   */
  public SnowflakeTelemetryChannelStatus(
      final String tableName,
      final String connectorName,
      final String channelName,
      final long startTime,
      final Optional<MetricsJmxReporter> metricsJmxReporter,
      final AtomicLong offsetPersistedInSnowflake,
      final AtomicLong processedOffset,
      final AtomicLong latestConsumerOffset) {
    super(tableName, SnowflakeTelemetryService.TelemetryType.KAFKA_CHANNEL_USAGE);

    this.channelCreationTime = startTime;
    this.connectorName = connectorName;
    this.channelName = channelName;
    this.metricsJmxReporter = metricsJmxReporter;

    this.offsetPersistedInSnowflake = offsetPersistedInSnowflake;
    this.processedOffset = processedOffset;
    this.latestConsumerOffset = latestConsumerOffset;

    metricsJmxReporter.ifPresent(reporter -> registerChannelJMXMetrics(reporter));
  }

  @Override
  public boolean isEmpty() {
    // Check that all properties are still at the default value.
    return this.offsetPersistedInSnowflake.get() == NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE
        && this.processedOffset.get() == NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE
        && this.latestConsumerOffset.get() == NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE;
  }

  @Override
  public void dumpTo(ObjectNode msg) {
    msg.put(TelemetryConstants.TABLE_NAME, this.tableName);
    msg.put(TelemetryConstants.CONNECTOR_NAME, this.connectorName);
    msg.put(TelemetryConstants.TOPIC_PARTITION_CHANNEL_NAME, this.channelName);

    msg.put(
        TelemetryConstants.OFFSET_PERSISTED_IN_SNOWFLAKE, this.offsetPersistedInSnowflake.get());
    msg.put(TelemetryConstants.PROCESSED_OFFSET, this.processedOffset.get());
    msg.put(TelemetryConstants.LATEST_CONSUMER_OFFSET, this.latestConsumerOffset.get());

    msg.put(TelemetryConstants.TOPIC_PARTITION_CHANNEL_CREATION_TIME, this.channelCreationTime);
    msg.put(TelemetryConstants.TOPIC_PARTITION_CHANNEL_CLOSE_TIME, System.currentTimeMillis());
    msg.put(TelemetryConstants.VALIDATION_FAILURE_COUNT, this.validationFailureCount.get());
    msg.put(TelemetryConstants.ERROR_TOLERATED_COUNT, this.errorToleratedCount.get());
    msg.put(TelemetryConstants.CHANNEL_RECOVERY_COUNT, this.recoveryCount.get());
    msg.put(
        TelemetryConstants.CLIENT_RECREATION_ATTEMPT_COUNT,
        this.clientRecreationAttemptCount.get());
    msg.put(
        TelemetryConstants.CLIENT_RECREATION_SUCCESS_COUNT,
        this.clientRecreationSuccessCount.get());
    msg.put(
        TelemetryConstants.CLIENT_RECREATION_FAILURE_COUNT,
        this.clientRecreationFailureCount.get());
    msg.put(TelemetryConstants.VALIDATION_DISABLED, this.validationDisabled);
    msg.put(TelemetryConstants.ROWS_INSERTED_COUNT, this.rowsInsertedCount);
    msg.put(TelemetryConstants.ROWS_PARSED_COUNT, this.rowsParsedCount);
    msg.put(TelemetryConstants.ROWS_ERROR_COUNT, this.rowsErrorCount);
    msg.put(TelemetryConstants.SERVER_AVG_PROCESSING_LATENCY_MS, this.serverAvgProcessingLatencyMs);

    putIfNotNull(msg, TelemetryConstants.DATABASE_NAME, this.databaseName);
    putIfNotNull(msg, TelemetryConstants.SCHEMA_NAME, this.schemaName);
    putIfNotNull(msg, TelemetryConstants.PIPE_NAME, this.pipeName);
    putIfNotNull(msg, TelemetryConstants.STATUS_CODE, this.statusCode);
    putIfNotNull(msg, TelemetryConstants.LAST_ERROR_TIMESTAMP, this.lastErrorTimestamp);
    putIfNotNull(
        msg,
        TelemetryConstants.LAST_ERROR_OFFSET_TOKEN_UPPER_BOUND,
        this.lastErrorOffsetTokenUpperBound);
    msg.put(TelemetryConstants.BACKPRESSURE_RETRY_COUNT, this.backpressureRetryCount.get());
    msg.put(TelemetryConstants.APPEND_ROW_FALLBACK_COUNT, this.appendRowFallbackCount.get());
    msg.put(
        TelemetryConstants.SCHEMA_EVOLUTION_FAILURE_COUNT, this.schemaEvolutionFailureCount.get());
  }

  private void registerChannelJMXMetrics(MetricsJmxReporter reporter) {
    MetricRegistry currentMetricRegistry = reporter.getMetricRegistry();

    registeredMetricNames =
        new String[] {
          channelMetricName(
              this.channelName,
              MetricsUtil.OFFSET_SUB_DOMAIN,
              MetricsUtil.OFFSET_PERSISTED_IN_SNOWFLAKE),
          channelMetricName(
              this.channelName, MetricsUtil.OFFSET_SUB_DOMAIN, MetricsUtil.PROCESSED_OFFSET),
          channelMetricName(
              this.channelName, MetricsUtil.OFFSET_SUB_DOMAIN, MetricsUtil.LATEST_CONSUMER_OFFSET),
          channelMetricName(
              this.channelName, MetricsUtil.OFFSET_SUB_DOMAIN, CHANNEL_RECOVERY_COUNT),
        };

    @SuppressWarnings("unchecked")
    Gauge<Long>[] gauges =
        new Gauge[] {
          (Gauge<Long>) this.offsetPersistedInSnowflake::get,
          (Gauge<Long>) this.processedOffset::get,
          (Gauge<Long>) this.latestConsumerOffset::get,
          (Gauge<Long>) this.recoveryCount::get,
        };

    for (int i = 0; i < registeredMetricNames.length; i++) {
      try {
        currentMetricRegistry.register(registeredMetricNames[i], gauges[i]);
      } catch (IllegalArgumentException ex) {
        // Safe: channel registration is serialized per task within open()
        LOGGER.warn(
            "Metric already present for channel {}, replacing: {}",
            this.channelName,
            registeredMetricNames[i]);
        reporter.removeMetric(registeredMetricNames[i]);
        currentMetricRegistry.register(registeredMetricNames[i], gauges[i]);
      }
    }

    // JmxReporter is started once at task level (SnowflakeSinkTaskMetrics constructor).
    // Its MetricRegistryListener auto-registers new MBeans as metrics are added.
    // Calling start() per-channel would re-process ALL metrics: O(N) unregister + register.
  }

  /** Unregisters the JMX metrics if possible */
  public void tryUnregisterChannelJMXMetrics() {
    metricsJmxReporter.ifPresent(
        reporter -> {
          if (registeredMetricNames != null) {
            for (String name : registeredMetricNames) {
              reporter.removeMetric(name);
            }
          }
        });
  }

  /** Increments the channel recovery counter. Thread-safe. */
  public void incRecoveryCount() {
    this.recoveryCount.incrementAndGet();
  }

  /** Increments the client recreation attempt counter. Thread-safe. */
  public void incClientRecreationAttemptCount() {
    this.clientRecreationAttemptCount.incrementAndGet();
  }

  /** Increments the client recreation success counter. Thread-safe. */
  public void incClientRecreationSuccessCount() {
    this.clientRecreationSuccessCount.incrementAndGet();
  }

  /** Increments the client recreation failure counter. Thread-safe. */
  public void incClientRecreationFailureCount() {
    this.clientRecreationFailureCount.incrementAndGet();
  }

  @VisibleForTesting
  public long getClientRecreationAttemptCount() {
    return this.clientRecreationAttemptCount.get();
  }

  @VisibleForTesting
  public long getClientRecreationSuccessCount() {
    return this.clientRecreationSuccessCount.get();
  }

  @VisibleForTesting
  public long getClientRecreationFailureCount() {
    return this.clientRecreationFailureCount.get();
  }

  /** Increments the validation failure counter. Thread-safe. */
  public void incValidationFailureCount() {
    this.validationFailureCount.incrementAndGet();
  }

  /** Increments the error-tolerated counter. Thread-safe. */
  public void incErrorToleratedCount() {
    this.errorToleratedCount.incrementAndGet();
  }

  /** Marks that client-side validation was silently disabled due to initialization failure. */
  public void setValidationDisabled() {
    this.validationDisabled = true;
  }

  /** Increments the backpressure retry counter. Thread-safe. */
  public void incBackpressureRetryCount() {
    this.backpressureRetryCount.incrementAndGet();
  }

  /** Increments the append-row fallback counter. Thread-safe. */
  public void incAppendRowFallbackCount() {
    this.appendRowFallbackCount.incrementAndGet();
  }

  /** Increments the schema evolution failure counter. Thread-safe. */
  public void incSchemaEvolutionFailureCount() {
    this.schemaEvolutionFailureCount.incrementAndGet();
  }

  /** Updates SDK-reported metrics from a ChannelStatus response. */
  public void updateFromChannelStatus(ChannelStatus status) {
    this.rowsInsertedCount = status.getRowsInsertedCount();
    this.rowsParsedCount = status.getRowsParsedCount();
    this.rowsErrorCount = status.getRowsErrorCount();
    this.serverAvgProcessingLatencyMs =
        status.getServerAvgProcessingLatency() != null
            ? status.getServerAvgProcessingLatency().toMillis()
            : -1;
    this.databaseName = status.getDatabaseName();
    this.schemaName = status.getSchemaName();
    this.pipeName = status.getPipeName();
    this.statusCode = status.getStatusCode() != null ? status.getStatusCode().toString() : null;
    this.lastErrorTimestamp =
        status.getLastErrorTimestamp() != null ? status.getLastErrorTimestamp().toString() : null;
    this.lastErrorOffsetTokenUpperBound = status.getLastErrorOffsetTokenUpperBound();
  }

  private static void putIfNotNull(ObjectNode msg, String key, String value) {
    if (value != null) {
      msg.put(key, value);
    }
  }

  @VisibleForTesting
  public long getOffsetPersistedInSnowflake() {
    return this.offsetPersistedInSnowflake.get();
  }

  @VisibleForTesting
  public long getProcessedOffset() {
    return this.processedOffset.get();
  }

  @VisibleForTesting
  public long getLatestConsumerOffset() {
    return this.latestConsumerOffset.get();
  }
}
