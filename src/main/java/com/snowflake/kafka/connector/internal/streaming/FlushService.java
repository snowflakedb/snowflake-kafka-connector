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

import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.internal.KCLogger;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.TopicPartition;
import org.graalvm.compiler.core.CompilerThread;

// TODO @rcheng: docs
public class FlushService {
  private static class FlushServiceProviderSingleton {
    private static final FlushService flushService = new FlushService();
  }

  public static FlushService getFlushServiceInstance() {
    return FlushServiceProviderSingleton.flushService;
  }

  // TODO @rcheng: logging
  private final KCLogger LOGGER = new KCLogger(this.getClass().toString());
  private final int THREAD_COUNT = 1;
  private final int DELAY_MS = 500;

  private ScheduledExecutorService flushExecutor;
  private Map<TopicPartition, TopicPartitionChannel> topicPartitionsMap;

  private FlushService() {
    this.flushExecutor = Executors.newScheduledThreadPool(THREAD_COUNT);
    this.topicPartitionsMap = new HashMap<>();
  }

  public void init() {
    this.flushExecutor.scheduleAtFixedRate(this::tryFlushTopicPartitionChannels, DELAY_MS, DELAY_MS, TimeUnit.MILLISECONDS);
  }

  public void shutdown() {
    this.flushExecutor.shutdown();
  }

  public void registerTopicPartitionChannel(
      TopicPartition topicPartition, TopicPartitionChannel topicPartitionChannel) {
    if (this.topicPartitionsMap.containsKey(topicPartition)) {
      // TODO @rcheng: log replace
    }
    this.topicPartitionsMap.put(topicPartition, topicPartitionChannel);
  }

  public void closeTopicPartitionChannel(TopicPartition topicPartition) {
    if (this.topicPartitionsMap.containsKey(topicPartition)) {
      this.topicPartitionsMap.get(topicPartition).tryFlushCurrentStreamingBuffer();
      this.topicPartitionsMap.remove(topicPartition);
    }
  }

  public void tryFlushTopicPartitionChannels() {
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
  }
}
