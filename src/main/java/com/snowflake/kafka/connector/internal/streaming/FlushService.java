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
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
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
  public static final long SCHEDULER_DELAY_MS = 500;

  private static final int EXECUTOR_MIN_THREAD_COUNT = 1;
  private static final long EXECUTOR_THREAD_TIMEOUT = 10;
  private static final TimeUnit EXECUTOR_THREAD_TIMEOUT_UNIT = TimeUnit.SECONDS;

  public static final long FLUSH_TIMEOUT = 10;
  public static final TimeUnit FLUSH_TIMEOUT_UNIT = TimeUnit.SECONDS;

  public final KCLogger LOGGER = new KCLogger(this.getClass().toString());

  private ScheduledExecutorService flushScheduler;
  private ScheduledFuture flushScheduleFuture;
  private BlockingQueue<Runnable> flushTaskQueue;
  private ThreadPoolExecutor flushExecutorPool;
  private ConcurrentMap<TopicPartition, TopicPartitionChannel> topicPartitionsMap;

  // single threaded scheduler periodically tries flushing all tpchannels in the map. it uses the
  // thread pool that should at max have 1 tpchannel : 1 thread, waiting for all threads to finish
  // flushing before continuing
  private FlushService() {
    this.flushScheduler = Executors.newScheduledThreadPool(SCHEDULER_THREAD_COUNT);
    this.flushScheduleFuture = null;
    this.flushTaskQueue = new SynchronousQueue<Runnable>(true);
    this.flushExecutorPool =
        new ThreadPoolExecutor(
            EXECUTOR_MIN_THREAD_COUNT,
            EXECUTOR_MIN_THREAD_COUNT,
            EXECUTOR_THREAD_TIMEOUT,
            EXECUTOR_THREAD_TIMEOUT_UNIT,
            this.flushTaskQueue);
    this.flushExecutorPool.allowCoreThreadTimeOut(true);
    this.topicPartitionsMap = new ConcurrentHashMap<>();
  }

  // schedule flushing
  // concurrency considerations: should only be called from SnowflakeSinkConnector.java
  public void init() {
    this.flushScheduleFuture = this.flushScheduler.scheduleAtFixedRate(
        this::tryFlushTopicPartitionChannels,
        SCHEDULER_DELAY_MS,
        SCHEDULER_DELAY_MS,
        TimeUnit.MILLISECONDS);
    LOGGER.info(
        Utils.formatString(
            "begin flush checking {} channels with delay {}",
            this.topicPartitionsMap.size(),
            SCHEDULER_DELAY_MS));
  }

  // clear map, shutdown executors
  // concurrency considerations: should only be called from SnowflakeSinkConnector.java
  public void shutdown(boolean shouldForceFlush) {
    LOGGER.info(
        Utils.formatString(
            "shutting down flush service, flushing all {} channels, ",
            this.topicPartitionsMap.size()));


    // shut down scheduler before pool
    try {
      this.flushScheduler.awaitTermination(FLUSH_TIMEOUT, FLUSH_TIMEOUT_UNIT);
      this.flushExecutorPool.awaitTermination(FLUSH_TIMEOUT, FLUSH_TIMEOUT_UNIT);
    } catch (InterruptedException e) {
      LOGGER.info("failed to terminate threads");
      throw new RuntimeException(e);
    }

    if (shouldForceFlush) {
      this.tryFlushTopicPartitionChannels();
    }
    this.topicPartitionsMap = new ConcurrentHashMap<>();
  }

  // concurrency considerations: must handle concurrency, called from each tpchannel creation
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
  }

  // concurrency considerations: must handle concurrency, called from each tpchannel creation
  public void removeTopicPartitionChannel(TopicPartition topicPartition) {
    if (topicPartition == null || !this.topicPartitionsMap.containsKey(topicPartition)) {
      LOGGER.info(
          Utils.formatString(
              "Invalid topic partition given for removal. TopicPartition: {}", topicPartition == null ? "null" : topicPartition.toString()));
      return;
    }

    this.topicPartitionsMap.get(topicPartition).tryFlushCurrentStreamingBuffer();
    this.topicPartitionsMap.remove(topicPartition);
    LOGGER.info(
        Utils.formatString(
            "Removing channel for topic partition: {}", topicPartition.toString()));
  }

  // concurrency considerations: should only be called from scheduled thread, which is currently
  // single threaded so it is ok
  public int tryFlushTopicPartitionChannels() {
    LOGGER.info(
        Utils.formatString(
            "FlushService checking {} channels against flush time threshold",
            this.topicPartitionsMap.size()));
    final long beginFlushTime = System.currentTimeMillis();

    // set pool size if needed
    int currPoolSize = this.flushExecutorPool.getMaximumPoolSize();
    int mapSize = this.topicPartitionsMap.size();
    if (currPoolSize != mapSize) {
      this.flushExecutorPool.setMaximumPoolSize(mapSize);
    }

    // start flushing
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
          // really shouldnt happen?
          LOGGER.info("max threads used, unable to schedule another flush");
        }
      }
    }

    // join all flush threads, we want to block on this to reduce thread usage and ensure 1:1
    // tpchannel to thread mapping
    final long beginJoinTime = System.currentTimeMillis();
    flushFutures.forEach(
        future -> {
          try {
            future.get(FLUSH_TIMEOUT, FLUSH_TIMEOUT_UNIT);
          } catch (ExecutionException e) {
            LOGGER.info("Channel flush failed execution");
          } catch (TimeoutException e) {
            LOGGER.info("Channel flush timed out");
          } catch (Exception e) {
            LOGGER.info(
                "Unexpected exception, swallowing exception. message: {}, cause: {}",
                e.getMessage(),
                e.getCause());
          }
        });

    // log info
    final long currTime = System.currentTimeMillis();
    LOGGER.info(
        Utils.formatString(
            "FlushService tried flushing on {} channels. {} ms to join threads, {} ms to try"
                + " flush"),
        flushFutures.size(),
        currTime - beginJoinTime,
        currTime - beginFlushTime);

    return flushFutures.size();
  }

  /** ALL FOLLOWING CODE IS ONLY FOR TESTING */

  /** Get a flush service with injected properties */
  @VisibleForTesting
  public static FlushService getFlushServiceForTests(
      ScheduledExecutorService flushExecutor,
      ScheduledFuture flushScheduleFuture,
      ScheduledThreadPoolExecutor flushExecutorPool,
      ConcurrentMap<TopicPartition, TopicPartitionChannel> topicPartitionsMap) {
    return new FlushService(flushExecutor, flushScheduleFuture, flushExecutorPool, topicPartitionsMap);
  }

  @VisibleForTesting
  private FlushService(
      ScheduledExecutorService flushScheduler,
      ScheduledFuture flushScheduleFuture,
      ScheduledThreadPoolExecutor flushExecutorPool,
      ConcurrentMap<TopicPartition, TopicPartitionChannel> topicPartitionsMap) {
    super();
    this.flushScheduler = flushScheduler;
    this.flushScheduleFuture = flushScheduleFuture;
    this.flushExecutorPool = flushExecutorPool;
    this.topicPartitionsMap = topicPartitionsMap;
  }

  @VisibleForTesting
  public Map<TopicPartition, TopicPartitionChannel> getTopicPartitionsMap() {
    return this.topicPartitionsMap;
  }
}
