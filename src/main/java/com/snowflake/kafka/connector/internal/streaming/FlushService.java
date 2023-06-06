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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.TopicPartition;

/**
 * This class checks and tries to flush the buffer in all {@link TopicPartitionChannel}.
 * Specifically targets the flush time threshold
 */
public class FlushService {
  // singleton logic to create a new flush service on init
  private static class FlushServiceProviderSingleton {
    private static final FlushService flushService = new FlushService();
  }

  /**
   * Ensure that there is only one instance of the flush service
   *
   * @return The current flush service
   */
  public static FlushService getFlushServiceInstance() {
    return FlushServiceProviderSingleton.flushService;
  }

  // constants
  public final KCLogger LOGGER = new KCLogger(this.getClass().toString());

  public static final long THREAD_TIMEOUT = 10;
  public static final TimeUnit THREAD_TIMEOUT_UNIT = TimeUnit.SECONDS;
  public static final int SCHEDULER_THREAD_COUNT = 1;
  public static final long SCHEDULER_DELAY_MS = 500;
  private static final int EXECUTOR_MIN_THREAD_COUNT = 1;

  private ScheduledExecutorService flushScheduler;
  private ThreadPoolExecutor flushExecutorPool;
  private ConcurrentMap<TopicPartition, TopicPartitionChannel> topicPartitionsMap;
  private boolean isActive = false;

  /**
   * The flush service uses a single threaded scheduler to periodically try flushing all registered
   * tpchannels. Each tpchannel that needs flushing receives its own thread from the thread pool
   *
   * <p>Must be initialized with the connector config to enable or disable this service
   *
   * <p>Private constructor for singleton
   */
  private FlushService() {
    this.flushScheduler = Executors.newScheduledThreadPool(SCHEDULER_THREAD_COUNT);
    this.flushExecutorPool =
        new ThreadPoolExecutor(
            EXECUTOR_MIN_THREAD_COUNT,
            EXECUTOR_MIN_THREAD_COUNT,
            THREAD_TIMEOUT,
            THREAD_TIMEOUT_UNIT,
            new SynchronousQueue<Runnable>(true));
    this.flushExecutorPool.allowCoreThreadTimeOut(true);
    this.topicPartitionsMap = new ConcurrentHashMap<>();
    this.isActive = false;
  }

  /**
   * Activates the flush service - start trying to flush all registered tpchannels. Not threadsafe,
   * should be called once during runtime from SnowflakeSinkConnector.java
   */
  public void activateScheduledFlushing(boolean isEnabled) {
    if (isEnabled) {
      try {
        this.flushScheduler.scheduleAtFixedRate(
            this::tryFlushTopicPartitionChannels,
            SCHEDULER_DELAY_MS,
            SCHEDULER_DELAY_MS,
            TimeUnit.MILLISECONDS);
        this.isActive = true;

        LOGGER.info(
            "Flush service activated, begin checking {} channels with delay {} ms",
            this.topicPartitionsMap.size(),
            SCHEDULER_DELAY_MS);
      } catch (RejectedExecutionException ex) {
        // RejectedExecutionException is only thrown if flush scheduler already has something
        // scheduled
        LOGGER.warn(Utils.getCustomExceptionStr("Flush service has already been activated", ex));
      } catch (Exception ex) {
        LOGGER.warn(Utils.getCustomExceptionStr("Unable to activate flush service", ex));
      }
    } else {
      LOGGER.info("Flush service is not enabled");
      this.shutdown();
    }
  }

  /**
   * Shut down the flush service. Specifically the scheduler, and thread pool. Clear the registered
   * channels. Not threadsafe, should not be called in parallel
   */
  public void shutdown() {
    LOGGER.info(
        Utils.formatString(
            "Shutting down flush service with {} registered channels...",
            this.topicPartitionsMap.size()));

    try {
      // shut down scheduler first, pool threads are able to timeout if scheduler throws an error
      this.isActive = false;
      this.flushScheduler.shutdown();
      this.flushExecutorPool.shutdown();
      this.topicPartitionsMap = new ConcurrentHashMap<>();
    } catch (Exception ex) {
      LOGGER.warn(Utils.getCustomExceptionStr("Failed to shut down flush service: ", ex));
    }
  }

  /**
   * Register or update a topic partition channel with the flush service. Needs to be threadsafe
   *
   * @param topicPartition The topic partition the channel ingests from
   * @param topicPartitionChannel The channel
   */
  public void registerTopicPartitionChannel(
      TopicPartition topicPartition, TopicPartitionChannel topicPartitionChannel) {
    if (topicPartition == null || topicPartitionChannel == null) {
      LOGGER.info(Utils.formatString("Invalid topicPartition or topicPartitionChannel"));
      return;
    }

    this.topicPartitionsMap.put(topicPartition, topicPartitionChannel);

    LOGGER.info(
        Utils.formatString(
            "Registered new channel for the {} topicPartition: {}",
            this.topicPartitionsMap.containsKey(topicPartition) ? "existing" : "new",
            topicPartition.toString()));
  }

  /**
   * Remove a topic partition channel from the flush service and force flush. Needs to be threadsafe
   *
   * @param topicPartition The topic partition the channel ingests from
   */
  public void removeTopicPartitionChannel(TopicPartition topicPartition) {
    if (topicPartition == null || !this.topicPartitionsMap.containsKey(topicPartition)) {
      LOGGER.info(
          Utils.formatString(
              "Invalid topic partition given for removal. TopicPartition: {}",
              topicPartition == null ? "null" : topicPartition.toString()));
      return;
    }

    LOGGER.info(
        Utils.formatString("Removing channel for topic partition: {}", topicPartition.toString()));
    this.topicPartitionsMap.remove(topicPartition);
  }

  /**
   * Tries flushing all registered topic partition channels
   *
   * @return The number of flushed channels
   */
  public int tryFlushTopicPartitionChannels() {
    // return if not initialized or nothing to flush
    if (!this.isActive || this.topicPartitionsMap.size() == 0) {
      return 0;
    }

    LOGGER.info(
        Utils.formatString(
            "FlushService checking all {} channels against flush time threshold",
            this.topicPartitionsMap.size()));

    // update thread pool size if needed, should have 1 thread : 1 tpChannel
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
              this.flushExecutorPool.submit(
                  () -> topicPartitionChannel.tryFlushCurrentStreamingBuffer(false)));
        } catch (RejectedExecutionException ex) {
          // RejectedExecutionException thrown if there are no idle threads
          LOGGER.warn(
              Utils.getCustomExceptionStr(
                  "No available thread to flush channel " + topicPartitionChannel.toString(), ex));
        } catch (Exception ex) {
          LOGGER.warn(
              Utils.getCustomExceptionStr(
                  "Unable to flush channel " + topicPartitionChannel.toString(), ex));
        }
      }
    }

    // join all flush threads, we want to block on this to and ensure 1:1 tpChannel to thread
    // mapping
    final long beginJoinTime = System.currentTimeMillis();
    flushFutures.forEach(
        future -> {
          try {
            future.get(THREAD_TIMEOUT, THREAD_TIMEOUT_UNIT);
          } catch (Exception ex) {
            LOGGER.warn(
                Utils.getCustomExceptionStr("Unexpected exception during flush execution", ex));
          }
        });

    int tryFlushCount = flushFutures.size();

    LOGGER.info(
        Utils.formatString(
            "FlushService tried flushing on {} / {} channels that hit the flush time threshold. Max"
                + " flush execution time: {}"),
        tryFlushCount,
        this.topicPartitionsMap.size(),
        System.currentTimeMillis() - beginJoinTime);

    return tryFlushCount;
  }

  /** ALL FOLLOWING CODE IS ONLY FOR TESTING */

  /** Get a flush service with injected properties */
  @VisibleForTesting
  public static FlushService getFlushServiceForTests(
      ScheduledExecutorService flushExecutor,
      ThreadPoolExecutor flushExecutorPool,
      ConcurrentMap<TopicPartition, TopicPartitionChannel> topicPartitionsMap,
      boolean isInitialized) {
    return new FlushService(flushExecutor, flushExecutorPool, topicPartitionsMap, isInitialized);
  }

  @VisibleForTesting
  private FlushService(
      ScheduledExecutorService flushScheduler,
      ThreadPoolExecutor flushExecutorPool,
      ConcurrentMap<TopicPartition, TopicPartitionChannel> topicPartitionsMap,
      boolean isInitialized) {
    super();
    this.flushScheduler = flushScheduler;
    this.flushExecutorPool = flushExecutorPool;
    this.topicPartitionsMap = topicPartitionsMap;
    this.isActive = isInitialized;
  }

  @VisibleForTesting
  public Map<TopicPartition, TopicPartitionChannel> getTopicPartitionsMap() {
    return this.topicPartitionsMap;
  }
}
