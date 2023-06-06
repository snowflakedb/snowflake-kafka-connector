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
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.common.TopicPartition;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.ArgumentMatchers;

@RunWith(Parameterized.class)
public class FlushServiceTest {
  // param to run test with or without flush service
  @Parameterized.Parameters(name = "isActive: {0}")
  public static Collection<Object[]> input() {
    return Arrays.asList(new Object[][] {{true}, {false}});
  }

  private final boolean isActive;

  public FlushServiceTest(boolean isActive) {
    this.isActive = isActive;
  }

  private ScheduledExecutorService flushScheduler;
  private ScheduledThreadPoolExecutor flushExecutorPool;
  private ConcurrentMap<TopicPartition, TopicPartitionChannel> topicPartitionsMap;
  private FlushService flushService;

  private int runnerIteration;

  private TopicPartition validTp0;
  private TopicPartition validTp1;
  private TopicPartitionChannel validTpChannel0;
  private TopicPartitionChannel validTpChannel1;

  @Before
  public void before() throws InterruptedException {
    this.flushScheduler = mock(ScheduledExecutorService.class);
    this.flushExecutorPool = mock(ScheduledThreadPoolExecutor.class);
    this.topicPartitionsMap = new ConcurrentHashMap<>();
    this.flushService =
        FlushService.getFlushServiceForTests(
            this.flushScheduler, this.flushExecutorPool, this.topicPartitionsMap, this.isActive);

    // init test mocks
    this.validTp0 = new TopicPartition("validTopic0", 0);
    this.validTp1 = new TopicPartition("validTopic1", 1);
    this.validTpChannel0 = mock(TopicPartitionChannel.class);
    this.validTpChannel1 = mock(TopicPartitionChannel.class);
    this.runnerIteration = 0;
  }

  @After
  public void after() {
    try {
      this.flushService.shutdown();
    } catch (Exception ex) {
      System.out.println(
          "Exception in trying to shut down actual flush service, this is just a safety check, it"
              + " can be ignored. Exception: "
              + ex.getMessage());
    }
  }

  @Test
  public void testActivate() {
    // test init
    this.flushService.activateScheduledFlushing(this.isActive);

    // verify flushing was scheduled
    verify(this.flushScheduler, times(this.isActive ? 1 : 0))
        .scheduleAtFixedRate(
            any(),
            ArgumentMatchers.eq(FlushService.SCHEDULER_DELAY_MS),
            ArgumentMatchers.eq(FlushService.SCHEDULER_DELAY_MS),
            ArgumentMatchers.eq(TimeUnit.MILLISECONDS));
  }

  @Test
  public void testMultipleActivate() {
    // test multiple activates doesn't crash
    this.flushService.activateScheduledFlushing(this.isActive);
    this.flushService.activateScheduledFlushing(this.isActive);
    this.flushService.activateScheduledFlushing(this.isActive);

    // verify flushing was scheduled
    verify(this.flushScheduler, times(this.isActive ? 3 : 0))
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
    this.flushService.activateScheduledFlushing(this.isActive);

    // verify flushing was scheduled
    verify(this.flushScheduler, times(this.isActive ? this.runnerIteration : 0))
        .scheduleAtFixedRate(
            any(),
            ArgumentMatchers.eq(FlushService.SCHEDULER_DELAY_MS),
            ArgumentMatchers.eq(FlushService.SCHEDULER_DELAY_MS),
            ArgumentMatchers.eq(TimeUnit.MILLISECONDS));
  }

  @Test
  public void testShutdown() throws InterruptedException {
    this.testShutdownRunner(false, false, 1, 1);
    this.testShutdownRunner(true, false, 2, 1);
    this.testShutdownRunner(false, true, 3, 2);
    this.testShutdownRunner(true, true, 4, 2);
  }

  private void testShutdownRunner(
      boolean shouldFlushSchedulerThrow,
      boolean shouldFlushPoolExecutorThrow,
      int flushSchedulerCount,
      int flushPoolExecutorCount) {
    if (shouldFlushSchedulerThrow) {
      doThrow(new SecurityException()).when(this.flushScheduler).shutdown();
    } else {
      doNothing().when(this.flushScheduler).shutdown();
    }
    if (shouldFlushPoolExecutorThrow) {
      doThrow(new SecurityException()).when(this.flushExecutorPool).shutdown();
    } else {
      doNothing().when(this.flushExecutorPool).shutdown();
    }

    this.flushService.shutdown();

    verify(this.flushScheduler, times(flushSchedulerCount)).shutdown();
    verify(this.flushExecutorPool, times(flushPoolExecutorCount)).shutdown();
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
    this.flushService.removeTopicPartitionChannel(this.validTp1);

    assert this.flushService.getTopicPartitionsMap().size() == 1;
  }

  @Test
  public void testTryFlushPartitionChannels()
      throws ExecutionException, InterruptedException, TimeoutException {
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

    // threading mocks
    when(this.flushExecutorPool.getMaximumPoolSize()).thenReturn(0);
    Future future = mock(Future.class);
    when(this.flushExecutorPool.submit(any(Callable.class))).thenReturn(future);

    // test flush
    this.flushService =
        FlushService.getFlushServiceForTests(
            this.flushScheduler, this.flushExecutorPool, this.topicPartitionsMap, true);
    int flushCount = this.flushService.tryFlushTopicPartitionChannels();

    // verify
    assert flushCount == 2;

    verify(streamingBufferThreshold0, times(1)).shouldFlushOnBufferTime(previousFlushTime0);
    verify(this.validTpChannel0, times(1)).getStreamingBufferThreshold();
    verify(this.validTpChannel0, times(1)).getPreviousFlushTimeStampMs();

    verify(streamingBufferThreshold1, times(1)).shouldFlushOnBufferTime(previousFlushTime1);
    verify(this.validTpChannel1, times(1)).getStreamingBufferThreshold();
    verify(this.validTpChannel1, times(1)).getPreviousFlushTimeStampMs();

    verify(this.flushExecutorPool, times(2)).submit(any(Callable.class));
    verify(this.flushExecutorPool, times(1)).getMaximumPoolSize();
    verify(this.flushExecutorPool, times(1)).setMaximumPoolSize(2);
    verify(future, times(2)).get(FlushService.THREAD_TIMEOUT, FlushService.THREAD_TIMEOUT_UNIT);
  }

  @Test
  public void testTryFlushZeroPartitionChannels() {
    this.flushService =
        FlushService.getFlushServiceForTests(
            this.flushScheduler, this.flushExecutorPool, this.topicPartitionsMap, true);

    // test flush does nothing
    assert this.flushService.tryFlushTopicPartitionChannels() == 0;
  }

  @Test
  public void testTryFlushPartitionChannelsSubmitException() {
    // executorPool.submit may throw the following two exceptions
    this.testTryFlushingPartitionChannelsExceptionRunner(new RejectedExecutionException());
    this.testTryFlushingPartitionChannelsExceptionRunner(new NullPointerException());
  }

  private void testTryFlushingPartitionChannelsExceptionRunner(Exception submitException) {
    this.runnerIteration++;

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

    // threading mocks
    when(this.flushExecutorPool.getMaximumPoolSize()).thenReturn(0);
    when(this.flushExecutorPool.submit(any(Callable.class))).thenThrow(submitException);

    // test flush
    this.flushService =
        FlushService.getFlushServiceForTests(
            this.flushScheduler, this.flushExecutorPool, this.topicPartitionsMap, true);
    int flushCount = this.flushService.tryFlushTopicPartitionChannels();

    // verify, since buffer threshold mock is created in this method, dont use runner iteration
    assert flushCount == 0;

    verify(streamingBufferThreshold0, times(1)).shouldFlushOnBufferTime(previousFlushTime0);
    verify(this.validTpChannel0, times(this.runnerIteration)).getStreamingBufferThreshold();
    verify(this.validTpChannel0, times(this.runnerIteration)).getPreviousFlushTimeStampMs();

    verify(streamingBufferThreshold1, times(1)).shouldFlushOnBufferTime(previousFlushTime1);
    verify(this.validTpChannel1, times(this.runnerIteration)).getStreamingBufferThreshold();
    verify(this.validTpChannel1, times(this.runnerIteration)).getPreviousFlushTimeStampMs();

    verify(this.flushExecutorPool, times(2 * this.runnerIteration)).submit(any(Callable.class));
    verify(this.flushExecutorPool, times(this.runnerIteration)).getMaximumPoolSize();
    verify(this.flushExecutorPool, times(this.runnerIteration)).setMaximumPoolSize(2);
  }

  @Test
  public void testTryFlushPartitionChannelsFutureGetException()
      throws ExecutionException, InterruptedException, TimeoutException {
    // future.get may throw the following 2 exceptions, in addition to ExecutionException which is
    // out of scope
    this.testTryFlushPartitionChannelsFutureGetExceptionRunner(new InterruptedException());
    this.testTryFlushPartitionChannelsFutureGetExceptionRunner(new TimeoutException());
  }

  private void testTryFlushPartitionChannelsFutureGetExceptionRunner(Exception ex)
      throws ExecutionException, InterruptedException, TimeoutException {
    this.runnerIteration++;

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

    // threading mocks
    when(this.flushExecutorPool.getMaximumPoolSize()).thenReturn(0);
    Future future = mock(Future.class);
    when(future.get(FlushService.THREAD_TIMEOUT, FlushService.THREAD_TIMEOUT_UNIT)).thenThrow(ex);
    when(this.flushExecutorPool.submit(any(Callable.class))).thenReturn(future);

    // test flush
    this.flushService =
        FlushService.getFlushServiceForTests(
            this.flushScheduler, this.flushExecutorPool, this.topicPartitionsMap, true);
    int flushCount = this.flushService.tryFlushTopicPartitionChannels();

    // verify
    assert flushCount == 2;

    verify(streamingBufferThreshold0, times(1)).shouldFlushOnBufferTime(previousFlushTime0);
    verify(this.validTpChannel0, times(this.runnerIteration)).getStreamingBufferThreshold();
    verify(this.validTpChannel0, times(this.runnerIteration)).getPreviousFlushTimeStampMs();

    verify(streamingBufferThreshold1, times(1)).shouldFlushOnBufferTime(previousFlushTime1);
    verify(this.validTpChannel1, times(this.runnerIteration)).getStreamingBufferThreshold();
    verify(this.validTpChannel1, times(this.runnerIteration)).getPreviousFlushTimeStampMs();

    verify(this.flushExecutorPool, times(2 * this.runnerIteration)).submit(any(Callable.class));
    verify(this.flushExecutorPool, times(this.runnerIteration)).getMaximumPoolSize();
    verify(this.flushExecutorPool, times(this.runnerIteration)).setMaximumPoolSize(2);
    verify(future, times(2)).get(FlushService.THREAD_TIMEOUT, FlushService.THREAD_TIMEOUT_UNIT);
  }
}
