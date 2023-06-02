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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.TopicPartition;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatchers;

public class FlushServiceTest {
  private ScheduledExecutorService flushScheduler;
  private ScheduledThreadPoolExecutor flushPoolExecutor;
  private ConcurrentMap<TopicPartition, TopicPartitionChannel> topicPartitionsMap;
  private FlushService flushService;

  // defaults to false, as flush pool executor should only be called in the tryflush
  private boolean shouldCallFlushPoolExecutor;
  private int runnerIteration;

  private TopicPartition validTp0;
  private TopicPartition validTp1;
  private TopicPartitionChannel validTpChannel0;
  private TopicPartitionChannel validTpChannel1;

  @Before
  public void before() throws InterruptedException {
    this.flushScheduler = mock(ScheduledExecutorService.class);
    this.flushPoolExecutor = mock(ScheduledThreadPoolExecutor.class);
    this.topicPartitionsMap = new ConcurrentHashMap<>();
    this.flushService =
        FlushService.getFlushServiceForTests(
            this.flushScheduler, this.flushPoolExecutor, this.topicPartitionsMap);

    // init test mocks
    this.validTp0 = new TopicPartition("validTopic0", 0);
    this.validTp1 = new TopicPartition("validTopic1", 1);
    this.validTpChannel0 = mock(TopicPartitionChannel.class);
    this.validTpChannel1 = mock(TopicPartitionChannel.class);

    this.shouldCallFlushPoolExecutor = false;
    this.runnerIteration = 0;
  }

  @After
  public void after() {
    this.flushService.shutdown();
  }

  @Test
  public void testActivate() {
    // test init
    this.flushService.activate();

    // verify flushing was scheduled
    verify(this.flushScheduler, times(1))
        .scheduleAtFixedRate(
            any(),
            ArgumentMatchers.eq(FlushService.SCHEDULER_DELAY_MS),
            ArgumentMatchers.eq(FlushService.SCHEDULER_DELAY_MS),
            ArgumentMatchers.eq(TimeUnit.MILLISECONDS));
  }

  @Test
  public void testMultipleActivate() {
    // test multiple activates doesn't crash
    this.flushService.activate();
    this.flushService.activate();
    this.flushService.activate();

    // verify flushing was scheduled
    verify(this.flushScheduler, times(3))
        .scheduleAtFixedRate(
            any(),
            ArgumentMatchers.eq(FlushService.SCHEDULER_DELAY_MS),
            ArgumentMatchers.eq(FlushService.SCHEDULER_DELAY_MS),
            ArgumentMatchers.eq(TimeUnit.MILLISECONDS));
  }

  @Test
  public void testActivateException() {
    // scheduleAtFixedRate throws the following exceptions, should not crash
    this.testActivateExceptionRunner(new RejectedExecutionException());
    this.testActivateExceptionRunner(new NullPointerException());
    this.testActivateExceptionRunner(new IllegalArgumentException());
  }

  private void testActivateExceptionRunner(Exception exToThrow) {
    this.runnerIteration++;

    when(this.flushScheduler.scheduleAtFixedRate(
            any(),
            ArgumentMatchers.eq(FlushService.SCHEDULER_DELAY_MS),
            ArgumentMatchers.eq(FlushService.SCHEDULER_DELAY_MS),
            ArgumentMatchers.eq(TimeUnit.MILLISECONDS)))
        .thenThrow(exToThrow);

    // test activate doesn't throw error
    this.flushService.activate();

    // verify flushing was scheduled
    verify(this.flushScheduler, times(this.runnerIteration))
        .scheduleAtFixedRate(
            any(),
            ArgumentMatchers.eq(FlushService.SCHEDULER_DELAY_MS),
            ArgumentMatchers.eq(FlushService.SCHEDULER_DELAY_MS),
            ArgumentMatchers.eq(TimeUnit.MILLISECONDS));
  }

  @Test
  public void testShutdown() throws InterruptedException {
    // successful shutdown
    this.testShutdownFailureRunner(true, true);

    // failures should not crash and also try shutting down everything
    this.testShutdownFailureRunner(true, false);
    this.testShutdownFailureRunner(false, true);
    this.testShutdownFailureRunner(false, false);
  }

  private void testShutdownFailureRunner(
      boolean shouldFlushSchedulerTerminate, boolean shouldFlushPoolExecutorTerminate)
      throws InterruptedException {
    this.runnerIteration++;

    // setup with one channel
    this.topicPartitionsMap.put(this.validTp0, this.validTpChannel0);

    when(this.flushScheduler.awaitTermination(anyLong(), any()))
        .thenReturn(shouldFlushSchedulerTerminate);
    when(this.flushPoolExecutor.awaitTermination(anyLong(), any()))
        .thenReturn(shouldFlushPoolExecutorTerminate);

    // test shutdown shouldn't crash
    this.flushService.shutdown();

    // verify executor and pool attempted shutdown and tpmap clear
    verify(this.flushScheduler, times(this.runnerIteration))
        .awaitTermination(FlushService.THREAD_TIMEOUT, FlushService.THREAD_TIMEOUT_UNIT);
    verify(this.flushPoolExecutor, times(this.runnerIteration))
        .awaitTermination(FlushService.THREAD_TIMEOUT, FlushService.THREAD_TIMEOUT_UNIT);
    assert this.flushService.getTopicPartitionsMap().size() == 0;
  }

  @Test
  public void testRegisterValidTopicPartitionChannel() {
    // test adding valid tp and tpchannel
    this.flushService.registerTopicPartitionChannel(this.validTp0, this.validTpChannel0);

    // verify map has channel
    Map<TopicPartition, TopicPartitionChannel> tpChannelMap =
        this.flushService.getTopicPartitionsMap();
    assert tpChannelMap.size() == 1;
    assert tpChannelMap.get(this.validTp0).equals(this.validTpChannel0);
  }

  @Test
  public void testReplaceValidTopicPartitionChannel() {
    // setup with valid channel
    this.flushService.registerTopicPartitionChannel(this.validTp0, this.validTpChannel0);

    // test replacing channel
    TopicPartitionChannel newTpChannel = mock(TopicPartitionChannel.class);
    this.flushService.registerTopicPartitionChannel(this.validTp0, newTpChannel);

    // verify map has channel
    Map<TopicPartition, TopicPartitionChannel> resMap = this.flushService.getTopicPartitionsMap();
    assert resMap.size() == 1;
    assert !resMap.get(this.validTp0).equals(this.validTpChannel0);
    assert resMap.get(this.validTp0).equals(newTpChannel);
  }

  @Test
  public void testRegisterInvalidTopicPartitionChannel() {
    // test and verify empty map with null tp
    this.flushService.registerTopicPartitionChannel(null, this.validTpChannel0);
    assert this.flushService.getTopicPartitionsMap().isEmpty();

    // test and verify empty map with null tp channel
    this.flushService.registerTopicPartitionChannel(this.validTp0, null);
    assert this.flushService.getTopicPartitionsMap().isEmpty();
  }

  @Test
  public void testRemoveValidTopicPartitionChannel() {
    // setup with valid channel
    this.flushService.registerTopicPartitionChannel(this.validTp0, this.validTpChannel0);
    this.flushService.registerTopicPartitionChannel(this.validTp1, this.validTpChannel1);
    assert this.flushService.getTopicPartitionsMap().size() == 2;

    // test closing channel
    this.flushService.removeTopicPartitionChannel(this.validTp0);

    // verify map has channel
    Map<TopicPartition, TopicPartitionChannel> resMap = this.flushService.getTopicPartitionsMap();
    assert resMap.size() == 1;
    assert resMap.get(this.validTp1).equals(this.validTpChannel1);
  }

  @Test
  public void testRemoveInvalidTopicPartitionChannel() {
    // setup with valid channel
    this.flushService.registerTopicPartitionChannel(this.validTp0, this.validTpChannel0);
    assert this.flushService.getTopicPartitionsMap().size() == 1;

    // test removing invalid doesn't break anything
    this.flushService.removeTopicPartitionChannel(null);

    assert this.flushService.getTopicPartitionsMap().size() == 1;
  }

  @Test
  public void testTryFlushPartitionChannels() {
    when(this.flushPoolExecutor.getMaximumPoolSize()).thenReturn(2);

    // setup with two channels
    StreamingBufferThreshold streamingBufferThreshold0 = mock(StreamingBufferThreshold.class);
    final long previousFlushTime0 = 1234L;
    when(streamingBufferThreshold0.shouldFlushOnBufferTime(previousFlushTime0)).thenReturn(true);
    when(this.validTpChannel0.getStreamingBufferThreshold()).thenReturn(streamingBufferThreshold0);
    when(this.validTpChannel0.getPreviousFlushTimeStampMs()).thenReturn(previousFlushTime0);

    StreamingBufferThreshold streamingBufferThreshold1 = mock(StreamingBufferThreshold.class);
    final long previousFlushTime1 = 5678L;
    when(streamingBufferThreshold1.shouldFlushOnBufferTime(previousFlushTime1)).thenReturn(true);
    when(this.validTpChannel1.getStreamingBufferThreshold()).thenReturn(streamingBufferThreshold1);
    when(this.validTpChannel1.getPreviousFlushTimeStampMs()).thenReturn(previousFlushTime1);

    this.topicPartitionsMap.put(this.validTp0, this.validTpChannel0);
    this.topicPartitionsMap.put(this.validTp1, this.validTpChannel1);

    this.flushService =
        FlushService.getFlushServiceForTests(
            this.flushScheduler, this.flushPoolExecutor, this.topicPartitionsMap);

    // test flush
    int flushCount = this.flushService.tryFlushTopicPartitionChannels();

    // verify
    assert flushCount == 2;

    verify(streamingBufferThreshold0, times(1)).shouldFlushOnBufferTime(previousFlushTime0);
    verify(this.validTpChannel0, times(1)).getStreamingBufferThreshold();
    verify(this.validTpChannel0, times(1)).getPreviousFlushTimeStampMs();

    verify(streamingBufferThreshold1, times(1)).shouldFlushOnBufferTime(previousFlushTime1);
    verify(this.validTpChannel1, times(1)).getStreamingBufferThreshold();
    verify(this.validTpChannel1, times(1)).getPreviousFlushTimeStampMs();

    verify(this.flushPoolExecutor, times(2)).submit(any(Callable.class));
    verify(this.flushPoolExecutor, times(1)).getMaximumPoolSize();
  }

  @Test
  public void testTryFlushZeroPartitionChannels() {
    this.flushService =
        FlushService.getFlushServiceForTests(
            this.flushScheduler, this.flushPoolExecutor, this.topicPartitionsMap);

    // test flush does nothing
    assert this.flushService.tryFlushTopicPartitionChannels() == 0;
  }

  @Test
  public void testFlushServiceSchedulesInBackground() throws InterruptedException {
    // setup
    ScheduledExecutorService flushScheduler = spy(ScheduledExecutorService.class);
    ScheduledThreadPoolExecutor flushPoolExecutor = spy(ScheduledThreadPoolExecutor.class);

    this.flushService =
        FlushService.getFlushServiceForTests(
            flushScheduler, flushPoolExecutor, this.topicPartitionsMap);

    // test and verify activate scheduler
    this.flushService.activate();
    verify(this.flushScheduler, times(1))
        .scheduleAtFixedRate(
            any(),
            ArgumentMatchers.eq(FlushService.SCHEDULER_DELAY_MS),
            ArgumentMatchers.eq(FlushService.SCHEDULER_DELAY_MS),
            ArgumentMatchers.eq(TimeUnit.MILLISECONDS));

    // test and verify registering channel 0
    this.flushService.registerTopicPartitionChannel(this.validTp0, this.validTpChannel0);
    Map<TopicPartition, TopicPartitionChannel> tpMap = this.flushService.getTopicPartitionsMap();
    assert tpMap.size() == 1;
    assert tpMap.get(this.validTp0).equals(this.validTpChannel0);

    // verify flush channel 0

    // test and verify registering channel 1
    this.flushService.registerTopicPartitionChannel(this.validTp1, this.validTpChannel1);
    tpMap = this.flushService.getTopicPartitionsMap();
    assert tpMap.size() == 2;
    assert tpMap.get(this.validTp0).equals(this.validTpChannel0);
    assert tpMap.get(this.validTp1).equals(this.validTpChannel1);

    // verify flush channel 0 and channel 1

    // test and verify removing channel 0
    this.flushService.removeTopicPartitionChannel(this.validTp0);
    tpMap = this.flushService.getTopicPartitionsMap();
    assert tpMap.size() == 1;
    assert tpMap.get(this.validTp1).equals(this.validTpChannel1);

    // verify flush channel 1

    // test and verify shutdown
    this.flushService.shutdown();
    verify(this.flushScheduler, times(1))
        .awaitTermination(FlushService.THREAD_TIMEOUT, FlushService.THREAD_TIMEOUT_UNIT);
    verify(this.flushPoolExecutor, times(1))
        .awaitTermination(FlushService.THREAD_TIMEOUT, FlushService.THREAD_TIMEOUT_UNIT);
    assert this.flushService.getTopicPartitionsMap().size() == 0;
  }
}
