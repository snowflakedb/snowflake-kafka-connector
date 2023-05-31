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

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.snowflake.kafka.connector.internal.BufferThreshold;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.TopicPartition;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatchers;

public class FlushServiceTest {
  private ScheduledExecutorService flushExecutor;
  private ConcurrentMap<TopicPartition, TopicPartitionChannel> topicPartitionsMap;
  private FlushService flushService;

  private TopicPartition validTp0;
  private TopicPartition validTp1;
  private TopicPartitionChannel validTpChannel0;
  private TopicPartitionChannel validTpChannel1;

  @Before
  public void before() {
    this.flushExecutor = mock(ScheduledExecutorService.class);
    this.topicPartitionsMap = new ConcurrentHashMap<>();
    this.flushService =
        FlushService.getFlushServiceForTests(this.flushExecutor, this.topicPartitionsMap);

    // init test mocks
    this.validTp0 = new TopicPartition("validTopic0", 0);
    this.validTp1 = new TopicPartition("validTopic1", 1);
    this.validTpChannel0 = mock(TopicPartitionChannel.class);
    this.validTpChannel1 = mock(TopicPartitionChannel.class);
  }

  @After
  public void after() {
    this.flushService.shutdown();
    FlushService.getFlushServiceInstance().shutdown();
  }

  @Test
  public void testInit() {
    // test init
    this.flushService.init();

    // verify flushing was scheduled
    verify(this.flushExecutor, times(1))
        .scheduleAtFixedRate(
            ArgumentMatchers.any(),
            ArgumentMatchers.eq(FlushService.FLUSH_SERVICE_DELAY_MS),
            ArgumentMatchers.eq(FlushService.FLUSH_SERVICE_DELAY_MS),
            ArgumentMatchers.eq(TimeUnit.MILLISECONDS));
  }

  @Test
  public void testShutdown() {
    // setup with one channel
    this.topicPartitionsMap.put(this.validTp0, this.validTpChannel0);

    // test shutdown
    this.flushService.shutdown();

    // verify executer shutdown and tpmap clear
    verify(this.flushExecutor, times(1)).shutdown();
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
    // setup with two channels
    StreamingBufferThreshold streamingBufferThreshold0 = mock(StreamingBufferThreshold.class);
    when(streamingBufferThreshold0.shouldFlushOnBufferTime(anyLong())).thenReturn(true);
    when(this.validTpChannel0.getStreamingBufferThreshold()).thenReturn(streamingBufferThreshold0);
    when(this.validTpChannel0.tryFlushCurrentStreamingBuffer())
        .thenReturn(BufferThreshold.FlushReason.BUFFER_FLUSH_TIME);

    StreamingBufferThreshold streamingBufferThreshold1 = mock(StreamingBufferThreshold.class);
    when(streamingBufferThreshold1.shouldFlushOnBufferTime(anyLong())).thenReturn(true);
    when(this.validTpChannel1.getStreamingBufferThreshold()).thenReturn(streamingBufferThreshold1);
    when(this.validTpChannel1.tryFlushCurrentStreamingBuffer())
        .thenReturn(BufferThreshold.FlushReason.BUFFER_FLUSH_TIME);

    this.topicPartitionsMap.put(this.validTp0, this.validTpChannel0);
    this.topicPartitionsMap.put(this.validTp1, this.validTpChannel1);

    this.flushService =
        FlushService.getFlushServiceForTests(this.flushExecutor, this.topicPartitionsMap);

    // test flush
    int flushCount = this.flushService.tryFlushTopicPartitionChannels();

    // verify
    assert flushCount == 2;
    verify(this.validTpChannel0, times(1)).tryFlushCurrentStreamingBuffer();
    verify(this.validTpChannel1, times(1)).tryFlushCurrentStreamingBuffer();
  }
}
