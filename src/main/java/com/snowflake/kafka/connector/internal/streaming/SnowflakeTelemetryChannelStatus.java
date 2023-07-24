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

package com.snowflake.kafka.connector.internal.streaming;

import static com.snowflake.kafka.connector.internal.metrics.MetricsUtil.BUFFER_SUB_DOMAIN;
import static com.snowflake.kafka.connector.internal.metrics.MetricsUtil.LATENCY_SUB_DOMAIN;
import static com.snowflake.kafka.connector.internal.metrics.MetricsUtil.OFFSET_SUB_DOMAIN;
import static com.snowflake.kafka.connector.internal.metrics.MetricsUtil.constructMetricName;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.collect.Maps;
import com.snowflake.kafka.connector.internal.KCLogger;
import com.snowflake.kafka.connector.internal.metrics.MetricsJmxReporter;
import com.snowflake.kafka.connector.internal.metrics.MetricsUtil;

import java.util.Arrays;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.snowflake.kafka.connector.internal.telemetry.SnowflakeTelemetryBasicInfo;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.node.ObjectNode;

public class SnowflakeTelemetryChannelStatus extends SnowflakeTelemetryBasicInfo {
  private static final KCLogger LOGGER = new KCLogger(SnowflakeTelemetryChannelStatus.class.getName());

  private final String topicName;
  private final int partition;
  private final String channelName;
  private final boolean enableCustomJMXConfig;
  private final MetricsJmxReporter metricsJmxReporter;

  private AtomicLong startTime; // start time of the status recording period

  // JMX Metrics related to Latencies
  private ConcurrentMap<MetricsUtil.EventType, Timer> eventsByType = Maps.newConcurrentMap();

  /** offsets - see {@link TopicPartitionChannel} for description of offsets */
  private AtomicLong offsetPersistedInSnowflake;
  private AtomicLong processedOffset;
  private AtomicLong latestConsumerOffset;

  // buffer
  private AtomicLong totalNumberOfRecords; // total number of record
  private AtomicLong totalSizeOfDataInBytes; // total size of data
  private AtomicLong totalBufferFlushCount; // total number of buffer flushes

  // TODO @rcheng latencies
//  // ------------ following metrics are not cumulative, reset every time sent ------------//
//  // Need to update two values atomically when calculating lag, thus a lock is required to protect the access
//  private final Lock lagLock;
//
//  // Average lag of Kafka
//  private AtomicLong averageKafkaLagMs; // average lag on Kafka side
//  private AtomicLong averageKafkaLagRecordCount; // record count


  public SnowflakeTelemetryChannelStatus(final String tableName, final String topicName, final int partition, final String channelName, final boolean enableCustomJMXConfig, final MetricsJmxReporter metricsJmxReporter) {
    super(tableName);
    this.topicName = topicName;
    this.partition = partition;
    this.channelName = channelName;
    this.enableCustomJMXConfig = enableCustomJMXConfig;
    this.metricsJmxReporter = metricsJmxReporter;

    this.startTime = new AtomicLong(System.currentTimeMillis());

    // offsets
    this.offsetPersistedInSnowflake = new AtomicLong(-1);
    this.processedOffset = new AtomicLong(-1);
    this.latestConsumerOffset = new AtomicLong(-1);
    
    // buffer
    this.totalNumberOfRecords = new AtomicLong(0);
    this.totalSizeOfDataInBytes = new AtomicLong(0);
    this.totalBufferFlushCount = new AtomicLong(0);
    
    if (this.enableCustomJMXConfig) {
      registerChannelJMXMetrics(channelName, metricsJmxReporter);
    }
  }

  /**
   * Adds the required fields into the given ObjectNode which will then be used as payload in
   * Telemetry API
   *
   * @param msg ObjectNode in which extra fields needs to be added.
   */
  @Override
  public void dumpTo(ObjectNode msg) {

  }

  /**
   * @return true if it would suggest that their was no update to corresponding implementation's
   * member variables. Or, in other words, the corresponding partition didnt receive any
   * records, in which case we would not call telemetry API.
   */
  @Override
  public boolean isEmpty() {
    // Check that all properties are still at the default value.
    return false;
  }

  // --------------- JMX Metrics --------------- //
  
  /**
   * Registers all the Metrics inside the metricRegistry. The registered metric will be a subclass
   * of {@link Metric}
   *
   * @param channelName channelName
   * @param metricsJmxReporter wrapper class for registering all metrics related to above connector
   *     and pipe
   */
  private void registerChannelJMXMetrics(
      final String channelName, MetricsJmxReporter metricsJmxReporter) {
    MetricRegistry currentMetricRegistry = metricsJmxReporter.getMetricRegistry();

    // Lazily remove all registered metrics from the registry since this can be invoked during
    // partition reassignment
    LOGGER.debug(
        "Registering metrics for channel:{}, existing:{}",
        channelName,
        metricsJmxReporter.getMetricRegistry().getMetrics().keySet().toString());
    metricsJmxReporter.removeMetricsFromRegistry(channelName);

    try {
      // Latency JMX
      // create meter per event type
      Arrays.stream(MetricsUtil.EventType.values())
          .forEach(
              eventType ->
                  eventsByType.put(
                      eventType,
                      currentMetricRegistry.timer(
                          constructMetricName(
                              channelName, LATENCY_SUB_DOMAIN, eventType.getMetricName()))));

      // offset
      currentMetricRegistry.register(
          constructMetricName(channelName, OFFSET_SUB_DOMAIN, MetricsUtil.OFFSET_PERSISTED_IN_SNOWFLAKE),
          (Gauge<Long>) () -> this.offsetPersistedInSnowflake.get());

      currentMetricRegistry.register(
          constructMetricName(channelName, OFFSET_SUB_DOMAIN, MetricsUtil.PROCESSED_OFFSET),
          (Gauge<Long>) () -> this.processedOffset.get());

      currentMetricRegistry.register(
          constructMetricName(channelName, OFFSET_SUB_DOMAIN, MetricsUtil.LATEST_CONSUMER_OFFSET),
          (Gauge<Long>) () -> this.latestConsumerOffset.get());

      // buffer
      currentMetricRegistry.register(
          constructMetricName(channelName, BUFFER_SUB_DOMAIN, MetricsUtil.BUFFER_SIZE_BYTES),
          (Gauge<Long>) () -> this.totalSizeOfDataInBytes.get());

      currentMetricRegistry.register(
          constructMetricName(channelName, BUFFER_SUB_DOMAIN, MetricsUtil.BUFFER_RECORD_COUNT),
          (Gauge<Long>) () -> this.totalNumberOfRecords.get());

      currentMetricRegistry.register(
          constructMetricName(channelName, BUFFER_SUB_DOMAIN, MetricsUtil.BUFFER_FLUSH_COUNT),
          (Gauge<Long>) () -> this.totalBufferFlushCount.get());
    } catch (IllegalArgumentException ex) {
      LOGGER.warn("Metrics already present:{}", ex.getMessage());
    }
  }
}
