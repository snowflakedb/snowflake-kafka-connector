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

import java.sql.Time;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.TopicPartition;

// TODO @rcheng: docs
public class FlushService {
  public static final int SCHEDULER_THREAD_COUNT = 1;
  public static final long FLUSH_SERVICE_DELAY_MS = 500;
  private static final int FLUSH_EXECUTOR_MIN_THREAD_COUNT = 1;
  private static final long THREAD_TIMEOUT = 10;
  private static final TimeUnit THREAD_TIMEOUT_UNIT = TimeUnit.SECONDS;

  private static class FlushServiceProviderSingleton {
    private static final FlushService flushService = new FlushService();
  }

  public static FlushService getFlushServiceInstance() {
    return FlushServiceProviderSingleton.flushService;
  }

  // TODO @rcheng: add logging just in addition to todos
  public final KCLogger LOGGER = new KCLogger(this.getClass().toString());

  private ScheduledExecutorService flushScheduler;
  private BlockingQueue<Runnable> flushTaskQueue;
  private ThreadPoolExecutor flushExecutorPool;
  private Map<TopicPartition, TopicPartitionChannel> topicPartitionsMap;

  private FlushService() {
    this.flushScheduler = Executors.newScheduledThreadPool(SCHEDULER_THREAD_COUNT);
    this.flushTaskQueue = new SynchronousQueue<Runnable>(true);
    this.flushExecutorPool = new ThreadPoolExecutor(FLUSH_EXECUTOR_MIN_THREAD_COUNT, FLUSH_EXECUTOR_MIN_THREAD_COUNT, THREAD_TIMEOUT, THREAD_TIMEOUT_UNIT, this.flushTaskQueue);
    this.flushExecutorPool.allowCoreThreadTimeOut(true);
    this.topicPartitionsMap = new HashMap<>();
  }

  public void init() {
    this.flushScheduler.scheduleAtFixedRate(
        this::tryFlushTopicPartitionChannels,
        FLUSH_SERVICE_DELAY_MS,
        FLUSH_SERVICE_DELAY_MS,
        TimeUnit.MILLISECONDS);
  }

  public void shutdown() {
    this.topicPartitionsMap = new HashMap<>();
    this.flushScheduler.shutdown();
    this.flushExecutorPool.shutdown();
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
    this.flushExecutorPool.setMaximumPoolSize(this.topicPartitionsMap.size());
  }

  public void removeTopicPartitionChannel(TopicPartition topicPartition) {
    if (topicPartition != null && this.topicPartitionsMap.containsKey(topicPartition)) {
      this.topicPartitionsMap.get(topicPartition).tryFlushCurrentStreamingBuffer();
      this.topicPartitionsMap.remove(topicPartition);
      this.flushExecutorPool.setMaximumPoolSize(this.topicPartitionsMap.size());
    }
  }

  // @rcheng question - this technically should be private, since it should only be called in the
  // background, but we need it for testing. should i add a visiblefortesting tag?
  public int tryFlushTopicPartitionChannels() {
    LOGGER.info(Utils.formatString("FlushService checking {} channels against flush time threshold", this.topicPartitionsMap.size()));

    int flushCount = 0;
    for (TopicPartitionChannel topicPartitionChannel : this.topicPartitionsMap.values()) {
      // TODO @rcheng potential opt: faster to save buffer threshold locally vs accessing it every time
      if (topicPartitionChannel
          .getStreamingBufferThreshold()
          .shouldFlushOnBufferTime(
              topicPartitionChannel.getPreviousFlushTimeStampMs())) {
        try {
          this.flushExecutorPool.execute(topicPartitionChannel::tryFlushCurrentStreamingBuffer);
        } catch (NullPointerException e) {
          // TODO @rcheng: log
          throw e;
        } catch (RejectedExecutionException e) {
          // TODO @rcheng: handle when max threads used is full
          throw e;
        }
        flushCount++;
      }
    }

    LOGGER.info(
        Utils.formatLogMessage("FlushService began flushing on {} channels"), flushCount);

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
    this.flushScheduler = flushExecutor;
    this.topicPartitionsMap = topicPartitionsMap;
  }

  @VisibleForTesting
  public Map<TopicPartition, TopicPartitionChannel> getTopicPartitionsMap() {
    return this.topicPartitionsMap;
  }
}
