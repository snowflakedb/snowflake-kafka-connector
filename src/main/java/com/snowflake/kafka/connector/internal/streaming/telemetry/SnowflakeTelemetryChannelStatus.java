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

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.snowflake.kafka.connector.internal.KCLogger;
import com.snowflake.kafka.connector.internal.metrics.MetricsJmxReporter;
import com.snowflake.kafka.connector.internal.metrics.MetricsUtil;
import java.util.concurrent.atomic.AtomicLong;

public class SnowflakeTelemetryChannelStatus {
  private static final KCLogger LOGGER =
      new KCLogger(SnowflakeTelemetryChannelStatus.class.toString());

  public static final long NUM_METRICS = 3; // update when new metrics are added
  private static final long INITIAL_OFFSET = -1;

  // channel properties
  private final String channelName;
  private final MetricsJmxReporter metricsJmxReporter;

  // offsets
  private AtomicLong offsetPersistedInSnowflake;
  private AtomicLong processedOffset;
  private AtomicLong latestConsumerOffset;

  /**
   * Creates a new object tracking {@link
   * com.snowflake.kafka.connector.internal.streaming.TopicPartitionChannel} metrics with JMX
   * TODO @rcheng: update comment when extends telemetryBasicInfo
   *
   * @param tableName the table the channel is ingesting to
   * @param channelName the name of the TopicPartitionChannel to track
   * @param enableCustomJMXConfig if JMX metrics are enabled
   * @param metricsJmxReporter used to report JMX metrics
   */
  public SnowflakeTelemetryChannelStatus(
      final String tableName,
      final String channelName,
      final boolean enableCustomJMXConfig,
      final MetricsJmxReporter metricsJmxReporter) {
    this.channelName = channelName;
    this.metricsJmxReporter = metricsJmxReporter;

    this.offsetPersistedInSnowflake = new AtomicLong(INITIAL_OFFSET);
    this.processedOffset = new AtomicLong(INITIAL_OFFSET);
    this.latestConsumerOffset = new AtomicLong(INITIAL_OFFSET);
    if (enableCustomJMXConfig) {
      if (metricsJmxReporter == null) {
        LOGGER.error("Invalid metrics JMX reporter, no metrics will be reported");
      } else {
        this.registerChannelJMXMetrics();
      }
    }
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
   * Gets the offset persisted in snowflake
   *
   * @return the offset persisted in snowflake
   */
  public long getOffsetPersistedInSnowflake() {
    return this.offsetPersistedInSnowflake.get();
  }

  /**
   * Sets the offset persisted in Snowflake
   *
   * @param offsetPersistedInSnowflake value to set
   */
  public void setOffsetPersistedInSnowflake(long offsetPersistedInSnowflake) {
    this.offsetPersistedInSnowflake.set(offsetPersistedInSnowflake);
  }

  /**
   * Gets the processed offset
   *
   * @return the processed offset
   */
  public long getProcessedOffset() {
    return this.processedOffset.get();
  }

  /**
   * Sets the processed offset
   *
   * @param processedOffset value to set
   */
  public void setProcessedOffset(long processedOffset) {
    this.processedOffset.set(processedOffset);
  }

  /**
   * Gets the latest consumer offset
   *
   * @return the latest consumer offset
   */
  public long getLatestConsumerOffset() {
    return this.latestConsumerOffset.get();
  }

  /**
   * Sets the latest consumer offset
   *
   * @param latestConsumerOffset value to set
   */
  public void setLatestConsumerOffset(long latestConsumerOffset) {
    this.latestConsumerOffset.set(latestConsumerOffset);
  }

  /**
   * Gets the JMX metrics reporter
   *
   * @return the JMX metrics reporter
   */
  public MetricsJmxReporter getMetricsJmxReporter() {
    return this.metricsJmxReporter;
  }
}
