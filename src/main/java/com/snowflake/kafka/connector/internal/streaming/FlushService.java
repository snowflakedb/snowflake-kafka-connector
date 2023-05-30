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

import com.google.common.annotations.VisibleForTesting;
import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.internal.KCLogger;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.TopicPartition;

// TODO @rcheng: docs
public class FlushService {
  public static final int FLUSH_SERVICE_THREAD_COUNT = 1;
  public static final long FLUSH_SERVICE_DELAY_MS = 500;

  private static class FlushServiceProviderSingleton {
    private static final FlushService flushService = new FlushService();
  }

  public static FlushService getFlushServiceInstance() {
    return FlushServiceProviderSingleton.flushService;
  }
  // TODO @rcheng: add logging just in addition to todos
  public final KCLogger LOGGER = new KCLogger(this.getClass().toString());
  private ScheduledExecutorService flushExecutor;
  private Map<TopicPartition, TopicPartitionChannel> topicPartitionsMap;

  private FlushService() {
    this.flushExecutor = Executors.newScheduledThreadPool(FLUSH_SERVICE_THREAD_COUNT);
    this.topicPartitionsMap = new HashMap<>();
  }

  public void init() {
    this.flushExecutor.scheduleAtFixedRate(
        this::tryFlushTopicPartitionChannels,
        FLUSH_SERVICE_DELAY_MS,
        FLUSH_SERVICE_DELAY_MS,
        TimeUnit.MILLISECONDS);
  }

  public void shutdown() {
    this.topicPartitionsMap = new HashMap<>();
    this.flushExecutor.shutdown();
  }

  public void registerTopicPartitionChannel(
      TopicPartition topicPartition, TopicPartitionChannel topicPartitionChannel) {
    if (topicPartition == null || topicPartitionChannel == null) {
      // TODO @rcheng: log
      return;
    }

    if (this.topicPartitionsMap.containsKey(topicPartition)) {
      // TODO @rcheng: log replace
    }
    this.topicPartitionsMap.put(topicPartition, topicPartitionChannel);
  }

  public void removeTopicPartitionChannel(TopicPartition topicPartition) {
    if (topicPartition != null && this.topicPartitionsMap.containsKey(topicPartition)) {
      this.topicPartitionsMap.get(topicPartition).tryFlushCurrentStreamingBuffer();
      this.topicPartitionsMap.remove(topicPartition);
    }
  }

  // @rcheng question - this technically should be private, since it should only be called in the
  // background, but we need it for testing. should i add a visiblefortesting tag?
  public int tryFlushTopicPartitionChannels() {
    final long currTime = System.currentTimeMillis();

    int flushCount = 0;
    for (TopicPartitionChannel topicPartitionChannel : this.topicPartitionsMap.values()) {
      if (topicPartitionChannel
          .getStreamingBufferThreshold()
          .shouldFlushOnBufferTime(
              topicPartitionChannel.getPreviousFlushTimeStampMs() - currTime)) {
        topicPartitionChannel.tryFlushCurrentStreamingBuffer();
        flushCount++;
      }
    }

    LOGGER.info(
        Utils.formatLogMessage("FlushService successfully flushed {} channels"), flushCount);

    return flushCount;
  }

  /** ALL FOLLOWING CODE IS ONLY FOR TESTING */
  /** Get a flush service with injected properties */
  @VisibleForTesting
  public static FlushService getFlushServiceForTests(
      ScheduledExecutorService flushExecutor,
      Map<TopicPartition, TopicPartitionChannel> topicPartitionsMap) {
    return new FlushService(flushExecutor, topicPartitionsMap);
  }

  @VisibleForTesting
  private FlushService(
      ScheduledExecutorService flushExecutor,
      Map<TopicPartition, TopicPartitionChannel> topicPartitionsMap) {
    super();
    this.flushExecutor = flushExecutor;
    this.topicPartitionsMap = topicPartitionsMap;
  }

  @VisibleForTesting
  public Map<TopicPartition, TopicPartitionChannel> getTopicPartitionsMap() {
    return this.topicPartitionsMap;
  }
}
