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

import static com.snowflake.kafka.connector.internal.metrics.MetricsUtil.constructMetricName;
import static com.snowflake.kafka.connector.internal.streaming.channel.TopicPartitionChannel.NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.google.common.annotations.VisibleForTesting;
import com.snowflake.kafka.connector.internal.metrics.MetricsJmxReporter;
import com.snowflake.kafka.connector.internal.metrics.MetricsUtil;
import com.snowflake.kafka.connector.internal.telemetry.SnowflakeTelemetryBasicInfo;
import com.snowflake.kafka.connector.internal.telemetry.SnowflakeTelemetryService;
import com.snowflake.kafka.connector.internal.telemetry.TelemetryConstants;
import java.util.concurrent.atomic.AtomicLong;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Extension of {@link SnowflakeTelemetryBasicInfo} class used to send data to snowflake when the
 * TopicPartitionChannel closes. Also creates and registers various metrics with JMX
 *
 * <p>Most of the data sent to Snowflake is aggregated data.
 */
public class SnowflakeTelemetryChannelStatus extends SnowflakeTelemetryBasicInfo {
  public static final long NUM_METRICS = 3; // update when new metrics are added

  // channel properties
  private final String connectorName;
  private final String channelName;
  private final MetricsJmxReporter metricsJmxReporter;
  private final long channelCreationTime;

  // offsets
  private final AtomicLong offsetPersistedInSnowflake;
  private final AtomicLong processedOffset;
  private final AtomicLong latestConsumerOffset;

  /**
   * Creates a new object tracking {@link
   * com.snowflake.kafka.connector.internal.streaming.channel.TopicPartitionChannel} metrics with
   * JMX and send telemetry data to snowflake
   *
   * @param tableName the table the channel is ingesting to
   * @param channelName the name of the TopicPartitionChannel to track
   * @param enableCustomJMXConfig if JMX metrics are enabled
   * @param metricsJmxReporter used to report JMX metrics
   */
  public SnowflakeTelemetryChannelStatus(
      final String tableName,
      final String connectorName,
      final String channelName,
      final long startTime,
      final boolean enableCustomJMXConfig,
      final MetricsJmxReporter metricsJmxReporter,
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
  }

  /** Registers all the Metrics inside the metricRegistry. */
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
              this.channelName,
              MetricsUtil.OFFSET_SUB_DOMAIN,
              MetricsUtil.OFFSET_PERSISTED_IN_SNOWFLAKE),
          (Gauge<Long>) this.offsetPersistedInSnowflake::get);

      currentMetricRegistry.register(
          constructMetricName(
              this.channelName, MetricsUtil.OFFSET_SUB_DOMAIN, MetricsUtil.PROCESSED_OFFSET),
          (Gauge<Long>) this.processedOffset::get);

      currentMetricRegistry.register(
          constructMetricName(
              this.channelName, MetricsUtil.OFFSET_SUB_DOMAIN, MetricsUtil.LATEST_CONSUMER_OFFSET),
          (Gauge<Long>) this.latestConsumerOffset::get);
    } catch (IllegalArgumentException ex) {
      LOGGER.warn("Metrics already present:{}", ex.getMessage());
    }

    this.metricsJmxReporter.start();
  }

  /** Unregisters the JMX metrics if possible */
  public void tryUnregisterChannelJMXMetrics() {
    if (this.metricsJmxReporter != null) {
      LOGGER.debug(
          "Removing metrics for channel:{}, existing metrics:{}",
          this.channelName,
          metricsJmxReporter.getMetricRegistry().getMetrics().keySet().toString());
      this.metricsJmxReporter.removeMetricsFromRegistry(this.channelName);
    }
  }

  /**
   * Gets the JMX metrics reporter
   *
   * @return the JMX metrics reporter
   */
  public MetricsJmxReporter getMetricsJmxReporter() {
    return this.metricsJmxReporter;
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
