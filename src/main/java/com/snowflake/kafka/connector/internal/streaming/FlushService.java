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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.kafka.common.TopicPartition;

// TODO @rcheng: docs
public class FlushService {
  private static class FlushServiceProviderSingleton {
    private static final FlushService flushService = new FlushService();
  }

  public static FlushService getFlushServiceInstance() {
    return FlushServiceProviderSingleton.flushService;
  }

  private final KCLogger LOGGER = new KCLogger(this.getClass().toString());
  private ExecutorService flushExecutor;
  private Map<TopicPartition, TopicPartitionChannel> topicPartitionsMap;

  private FlushService() {
    // TODO @rcheng: log creating new service
    this.flushExecutor = Executors.newSingleThreadExecutor();
    this.topicPartitionsMap = new HashMap<>();
  }

  private void tryFlushTopicPartitionChannels() {
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

  public void registerTopicPartitionChannel(
      TopicPartition topicPartition, TopicPartitionChannel topicPartitionChannel) {
    this.topicPartitionsMap.put(topicPartition, topicPartitionChannel);
    // TODO @rcheng: log adding tpchannel
  }

  public void closeTopicPartitionChannel(TopicPartition topicPartition) {
    if (this.topicPartitionsMap.containsKey(topicPartition)) {
      this.topicPartitionsMap.get(topicPartition).tryFlushCurrentStreamingBuffer();
      this.topicPartitionsMap.remove(topicPartition);
    }

    // TODO @rcheng: log no tpchannel
  }
}
