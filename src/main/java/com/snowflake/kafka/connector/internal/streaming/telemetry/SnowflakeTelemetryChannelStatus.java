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

import static com.snowflake.kafka.connector.internal.metrics.MetricsUtil.FILE_COUNT_SUB_DOMAIN;
import static com.snowflake.kafka.connector.internal.metrics.MetricsUtil.FILE_COUNT_TABLE_STAGE_INGESTION_FAIL;
import static com.snowflake.kafka.connector.internal.metrics.MetricsUtil.LATENCY_SUB_DOMAIN;
import static com.snowflake.kafka.connector.internal.metrics.MetricsUtil.LATEST_CONSUMER_OFFSET;
import static com.snowflake.kafka.connector.internal.metrics.MetricsUtil.OFFSET_PERSISTED_IN_SNOWFLAKE;
import static com.snowflake.kafka.connector.internal.metrics.MetricsUtil.OFFSET_SUB_DOMAIN;
import static com.snowflake.kafka.connector.internal.metrics.MetricsUtil.constructMetricName;
import static com.snowflake.kafka.connector.internal.telemetry.TelemetryConstants.AVERAGE_COMMIT_LAG_FILE_COUNT;
import static com.snowflake.kafka.connector.internal.telemetry.TelemetryConstants.AVERAGE_COMMIT_LAG_MS;
import static com.snowflake.kafka.connector.internal.telemetry.TelemetryConstants.AVERAGE_INGESTION_LAG_FILE_COUNT;
import static com.snowflake.kafka.connector.internal.telemetry.TelemetryConstants.AVERAGE_INGESTION_LAG_MS;
import static com.snowflake.kafka.connector.internal.telemetry.TelemetryConstants.AVERAGE_KAFKA_LAG_MS;
import static com.snowflake.kafka.connector.internal.telemetry.TelemetryConstants.AVERAGE_KAFKA_LAG_RECORD_COUNT;
import static com.snowflake.kafka.connector.internal.telemetry.TelemetryConstants.BYTE_NUMBER;
import static com.snowflake.kafka.connector.internal.telemetry.TelemetryConstants.CHANNEL_NAME;
import static com.snowflake.kafka.connector.internal.telemetry.TelemetryConstants.CLEANER_RESTART_COUNT;
import static com.snowflake.kafka.connector.internal.telemetry.TelemetryConstants.COMMITTED_OFFSET;
import static com.snowflake.kafka.connector.internal.telemetry.TelemetryConstants.END_TIME;
import static com.snowflake.kafka.connector.internal.telemetry.TelemetryConstants.FILE_COUNT_ON_INGESTION;
import static com.snowflake.kafka.connector.internal.telemetry.TelemetryConstants.FILE_COUNT_ON_STAGE;
import static com.snowflake.kafka.connector.internal.telemetry.TelemetryConstants.FILE_COUNT_PURGED;
import static com.snowflake.kafka.connector.internal.telemetry.TelemetryConstants.FILE_COUNT_TABLE_STAGE_BROKEN_RECORD;
import static com.snowflake.kafka.connector.internal.telemetry.TelemetryConstants.FILE_COUNT_TABLE_STAGE_INGEST_FAIL;
import static com.snowflake.kafka.connector.internal.telemetry.TelemetryConstants.FLUSHED_OFFSET;
import static com.snowflake.kafka.connector.internal.telemetry.TelemetryConstants.MEMORY_USAGE;
import static com.snowflake.kafka.connector.internal.telemetry.TelemetryConstants.PIPE_NAME;
import static com.snowflake.kafka.connector.internal.telemetry.TelemetryConstants.PROCESSED_OFFSET;
import static com.snowflake.kafka.connector.internal.telemetry.TelemetryConstants.PURGED_OFFSET;
import static com.snowflake.kafka.connector.internal.telemetry.TelemetryConstants.RECORD_NUMBER;
import static com.snowflake.kafka.connector.internal.telemetry.TelemetryConstants.STAGE_NAME;
import static com.snowflake.kafka.connector.internal.telemetry.TelemetryConstants.START_TIME;
import static com.snowflake.kafka.connector.internal.telemetry.TelemetryConstants.TABLE_NAME;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.snowflake.kafka.connector.internal.metrics.MetricsJmxReporter;
import com.snowflake.kafka.connector.internal.metrics.MetricsUtil;
import com.snowflake.kafka.connector.internal.metrics.MetricsUtil.EventType;
import java.util.Arrays;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.LongUnaryOperator;

import com.snowflake.kafka.connector.internal.telemetry.SnowflakeTelemetryBasicInfo;
import com.snowflake.kafka.connector.internal.telemetry.SnowflakeTelemetryService;
import com.snowflake.kafka.connector.internal.telemetry.TelemetryConstants;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.node.ObjectNode;
import org.checkerframework.checker.units.qual.A;

/**
 * Extension of {@link SnowflakeTelemetryBasicInfo} class used to send data to snowflake
 * when the TopicPartitionChannel closes
 *
 * <p>Most of the data sent to Snowflake is an aggregated data.
 */
public class SnowflakeTelemetryChannelStatus extends SnowflakeTelemetryBasicInfo {
  public static final long NUM_METRICS = 3; // update when new metrics are added
  private static final long INITIAL_OFFSET = -1;

  // channel properties
  private final String channelName;
  private final boolean enableCustomJMXConfig;
  private final MetricsJmxReporter metricsJmxReporter;

  private final AtomicLong startTime;

  // offsets
  private AtomicLong offsetPersistedInSnowflake;
  private AtomicLong processedOffset;
  private long latestConsumerOffset;

  public SnowflakeTelemetryChannelStatus(
      final String tableName,
      final String channelName,
      final boolean enableCustomJMXConfig,
      final MetricsJmxReporter metricsJmxReporter) {
    super(tableName);
    this.channelName = channelName;
    this.enableCustomJMXConfig = enableCustomJMXConfig;
    this.metricsJmxReporter = metricsJmxReporter;

    this.startTime = new AtomicLong(System.currentTimeMillis());

    this.offsetPersistedInSnowflake = new AtomicLong(INITIAL_OFFSET);
    this.processedOffset = new AtomicLong(INITIAL_OFFSET);
    this.latestConsumerOffset = INITIAL_OFFSET;
    if (enableCustomJMXConfig) {
      if (metricsJmxReporter == null) {
        LOGGER.error("Invalid metrics JMX reporter, no metrics will be reported");
      } else {
        this.registerChannelJMXMetrics();
      }
    }
  }

  @Override
  public boolean isEmpty() {
    // Check that all properties are still at the default value.
    return this.offsetPersistedInSnowflake.get() == INITIAL_OFFSET
        && this.processedOffset.get() == INITIAL_OFFSET
        && this.latestConsumerOffset == INITIAL_OFFSET;
  }

  @Override
  public void dumpTo(ObjectNode msg) {
    msg.put(TelemetryConstants.TABLE_NAME, this.tableName);
    msg.put(TelemetryConstants.CHANNEL_NAME, this.channelName);

    msg.put(TelemetryConstants.OFFSET_PERSISTED_IN_SNOWFLAKE, offsetPersistedInSnowflake.get());
    msg.put(TelemetryConstants.PROCESSED_OFFSET, processedOffset.get());
    msg.put(TelemetryConstants.LATEST_CONSUMER_OFFSET, latestConsumerOffset);

    final long currTime = System.currentTimeMillis();
    msg.put(TelemetryConstants.START_TIME, startTime.getAndSet(currTime));
    msg.put(TelemetryConstants.END_TIME, currTime);
  }

  /**
   * Registers all the Metrics inside the metricRegistry.
   */
  private void registerChannelJMXMetrics() {
    LOGGER.debug(
        "Registering new metrics for channel:{}, removing existing metrics:{}",
        this.channelName,
        this.metricsJmxReporter.getMetricRegistry().getMetrics().keySet().toString());
    this.metricsJmxReporter.removeMetricsFromRegistry(this.channelName);

    MetricRegistry currentMetricRegistry = this.metricsJmxReporter.getMetricRegistry();

    try {
      // offsets
      currentMetricRegistry.register(
          constructMetricName(
              this.channelName, MetricsUtil.OFFSET_SUB_DOMAIN, MetricsUtil.OFFSET_PERSISTED_IN_SNOWFLAKE),
          (Gauge<Long>) this.offsetPersistedInSnowflake::get);

      currentMetricRegistry.register(
          constructMetricName(this.channelName, MetricsUtil.OFFSET_SUB_DOMAIN, MetricsUtil.PROCESSED_OFFSET),
          (Gauge<Long>) this.processedOffset::get);

      currentMetricRegistry.register(
          constructMetricName(this.channelName, MetricsUtil.OFFSET_SUB_DOMAIN, MetricsUtil.LATEST_CONSUMER_OFFSET),
          (Gauge<Long>) () -> this.latestConsumerOffset);
    } catch (IllegalArgumentException ex) {
      LOGGER.warn("Metrics already present:{}", ex.getMessage());
    }

    this.metricsJmxReporter.start();
  }

  /**
   * Unregisters the JMX metrics
   */
  public void unregisterChannelJMXMetrics() {
    if (this.metricsJmxReporter != null) {
      LOGGER.debug(
          "Removing metrics for channel:{}, existing metrics:{}",
          this.channelName,
          metricsJmxReporter.getMetricRegistry().getMetrics().keySet().toString());
      this.metricsJmxReporter.removeMetricsFromRegistry(this.channelName);
    }
  }

  public long getOffsetPersistedInSnowflake() {
    return this.offsetPersistedInSnowflake.get();
  }

  public void setOffsetPersistedInSnowflake(long offsetPersistedInSnowflake) {
    this.offsetPersistedInSnowflake.set(offsetPersistedInSnowflake);
  }

  public long getProcessedOffset() {
    return this.processedOffset.get();
  }

  public void setProcessedOffset(long processedOffset) {
    this.processedOffset.set(processedOffset);
  }

  public long getLatestConsumerOffset() {
    return this.latestConsumerOffset;
  }

  public void setLatestConsumerOffset(long latestConsumerOffset) {
    this.latestConsumerOffset = latestConsumerOffset;
  }

  public MetricsJmxReporter getMetricsJmxReporter() {
    return this.metricsJmxReporter;
  }
}
