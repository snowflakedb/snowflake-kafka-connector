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
import com.snowflake.kafka.connector.internal.telemetry.SnowflakeTelemetryBasicInfo;
import com.snowflake.kafka.connector.internal.telemetry.SnowflakeTelemetryService;
import com.snowflake.kafka.connector.internal.telemetry.TelemetryConstants;
import java.util.concurrent.atomic.AtomicLong;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Extension of {@link SnowflakeTelemetryBasicInfo} class used to send data to snowflake when the
 * TopicPartitionChannel closes
 *
 * <p>Most of the data sent to Snowflake is an aggregated data.
 */
public class SnowflakeTelemetryChannelStatus {
  private static final KCLogger LOGGER = new KCLogger(SnowflakeTelemetryChannelStatus.class.toString());

  public static final long NUM_METRICS = 3; // update when new metrics are added
  private static final long INITIAL_OFFSET = -1;

  // channel properties
  private final String channelName;
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
    this.channelName = channelName;
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
          (Gauge<Long>) () -> this.latestConsumerOffset);
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
