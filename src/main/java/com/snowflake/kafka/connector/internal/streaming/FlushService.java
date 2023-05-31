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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.common.TopicPartition;

// TODO @rcheng: docs
public class FlushService {
  // singleton logic
  private static class FlushServiceProviderSingleton {
    private static final FlushService flushService = new FlushService();
  }

  public static FlushService getFlushServiceInstance() {
    return FlushServiceProviderSingleton.flushService;
  }

  // constants
  public static final int SCHEDULER_THREAD_COUNT = 1;
  public static final long FLUSH_SERVICE_DELAY_MS = 500;

  private static final int FLUSH_EXECUTOR_MIN_THREAD_COUNT = 1;
  private static final long THREAD_TIMEOUT = 10;
  private static final TimeUnit THREAD_TIMEOUT_UNIT = TimeUnit.SECONDS;
  private static final long FLUSH_TIMEOUT = 1;
  private static final TimeUnit FLUSH_TIMEOUT_UNIT = TimeUnit.SECONDS;

  public final KCLogger LOGGER = new KCLogger(this.getClass().toString());

  private ScheduledExecutorService flushScheduler;
  private BlockingQueue<Runnable> flushTaskQueue;
  private ThreadPoolExecutor flushExecutorPool;
  private ConcurrentMap<TopicPartition, TopicPartitionChannel> topicPartitionsMap;

  private FlushService() {
    this.flushScheduler = Executors.newScheduledThreadPool(SCHEDULER_THREAD_COUNT);
    this.flushTaskQueue = new SynchronousQueue<Runnable>(true);
    this.flushExecutorPool =
        new ThreadPoolExecutor(
            FLUSH_EXECUTOR_MIN_THREAD_COUNT,
            FLUSH_EXECUTOR_MIN_THREAD_COUNT,
            THREAD_TIMEOUT,
            THREAD_TIMEOUT_UNIT,
            this.flushTaskQueue);
    this.flushExecutorPool.allowCoreThreadTimeOut(true);
    this.topicPartitionsMap = new ConcurrentHashMap<>();
  }

  // schedule flushing
  public void init() {
    this.flushScheduler.scheduleAtFixedRate(
        this::tryFlushTopicPartitionChannels,
        FLUSH_SERVICE_DELAY_MS,
        FLUSH_SERVICE_DELAY_MS,
        TimeUnit.MILLISECONDS);
    LOGGER.info(
        Utils.formatString(
            "begin flush checking {} channels with delay {}",
            this.topicPartitionsMap.size(),
            FLUSH_SERVICE_DELAY_MS));
  }

  // clear map, shutdown executors
  public void shutdown() {
    LOGGER.info(
        Utils.formatString(
            "shutting down flush service, there are {} channels, ",
            this.topicPartitionsMap.size()));
    this.topicPartitionsMap = new ConcurrentHashMap<>();

    // shut down scheduler before pool
    this.flushScheduler.shutdown();
    this.flushExecutorPool.shutdown();
  }

  public void registerTopicPartitionChannel(
      TopicPartition topicPartition, TopicPartitionChannel topicPartitionChannel) {
    if (topicPartition == null || topicPartitionChannel == null) {
      LOGGER.info(Utils.formatString("Invalid topicPartition or topicPartitionChannel"));
      return;
    }

    LOGGER.info(
        Utils.formatString(
            "Register new channel for {} topicPartition: {}",
            this.topicPartitionsMap.containsKey(topicPartition) ? "existing" : "new",
            topicPartition.toString()));

    this.topicPartitionsMap.put(topicPartition, topicPartitionChannel);
    this.flushExecutorPool.setMaximumPoolSize(this.topicPartitionsMap.size());
  }

  public void removeTopicPartitionChannel(TopicPartition topicPartition) {
    if (topicPartition != null && this.topicPartitionsMap.containsKey(topicPartition)) {
      this.topicPartitionsMap.get(topicPartition).tryFlushCurrentStreamingBuffer();
      this.topicPartitionsMap.remove(topicPartition);
      this.flushExecutorPool.setMaximumPoolSize(this.topicPartitionsMap.size());
      LOGGER.info(
          Utils.formatString(
              "Removing channel for topic partition: {}", topicPartition.toString()));
      return;
    }
    LOGGER.info(
        Utils.formatString(
            "Topic partition is null or was not registered, unnecessary remove call"));
  }

  public int tryFlushTopicPartitionChannels() {
    long beginFlushTime = System.currentTimeMillis();

    if (this.flushExecutorPool.getMaximumPoolSize() != this.topicPartitionsMap.size()) {
      LOGGER.info(
          "max pool size was not set correctly. maxpoolsize: {}, mapsize: {}",
          this.flushExecutorPool.getMaximumPoolSize(),
          this.topicPartitionsMap.size());
      this.flushExecutorPool.setMaximumPoolSize(this.topicPartitionsMap.size());
    }

    LOGGER.info(
        Utils.formatString(
            "FlushService checking {} channels against flush time threshold",
            this.topicPartitionsMap.size()));

    List<Future> flushFutures = new ArrayList<>();

    for (TopicPartitionChannel topicPartitionChannel : this.topicPartitionsMap.values()) {
      // TODO @rcheng potential opt: faster to save buffer threshold locally vs accessing it every
      // time
      if (topicPartitionChannel
          .getStreamingBufferThreshold()
          .shouldFlushOnBufferTime(topicPartitionChannel.getPreviousFlushTimeStampMs())) {
        try {
          flushFutures.add(
              this.flushExecutorPool.submit(topicPartitionChannel::tryFlushCurrentStreamingBuffer));
        } catch (NullPointerException e) {
          // TODO @rcheng: log
          throw e;
        } catch (RejectedExecutionException e) {
          // TODO @rcheng: handle when max threads used is full, ok to swallow but queue bloat on
          // just one flush
          // throw e;
          LOGGER.info("max threads used, unable to schedule another flush");
        }
      }
    }

    long beginAwaitTime = System.currentTimeMillis();
    flushFutures.forEach(
        future -> {
          try {
            future.get(FLUSH_TIMEOUT, FLUSH_TIMEOUT_UNIT);
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          } catch (ExecutionException e) {
            LOGGER.info("Channel flush failed execution");
          } catch (TimeoutException e) {
            LOGGER.info("Channel flush timed out");
          }
        });

    final long currTime = System.currentTimeMillis();
    LOGGER.info(
        Utils.formatString(
            "FlushService tried flushing on {} channels. {} ms to join threads, {} ms to try"
                + " flush"),
        flushFutures.size(),
        currTime - beginAwaitTime,
        currTime - beginFlushTime);

    return flushFutures.size();
  }

  /** ALL FOLLOWING CODE IS ONLY FOR TESTING */

  /** Get a flush service with injected properties */
  @VisibleForTesting
  public static FlushService getFlushServiceForTests(
      ScheduledExecutorService flushExecutor,
      ConcurrentMap<TopicPartition, TopicPartitionChannel> topicPartitionsMap) {
    return new FlushService(flushExecutor, topicPartitionsMap);
  }

  @VisibleForTesting
  private FlushService(
      ScheduledExecutorService flushExecutor,
      ConcurrentMap<TopicPartition, TopicPartitionChannel> topicPartitionsMap) {
    super();
    this.flushScheduler = flushExecutor;
    this.topicPartitionsMap = topicPartitionsMap;
  }

  @VisibleForTesting
  public Map<TopicPartition, TopicPartitionChannel> getTopicPartitionsMap() {
    return this.topicPartitionsMap;
  }
}
