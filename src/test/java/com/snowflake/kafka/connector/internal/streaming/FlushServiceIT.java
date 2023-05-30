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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import java.util.HashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.TopicPartition;
import org.junit.After;
import org.junit.Test;

public class FlushServiceIT {
  private FlushService flushService;

  @After
  public void after() {
    if (this.flushService != null) {
      this.flushService.shutdown();
    }
    FlushService.getFlushServiceInstance().shutdown();
  }

  // this test tests the flush time, so debugging it may cause different behavior
  @Test
  public void testBackgroundFlush() throws InterruptedException {
    // constants
    String catTableName = "catTable";

    String amarettoName = "amaretto";
    TopicPartition amarettoTp = new TopicPartition(amarettoName, 0);

    String scotchName = "scotch";
    TopicPartition scotchTp = new TopicPartition(scotchName, 0);

    // config aimed to hit flush time threshold
    long bufferFlushTimeSec = 4;
    StreamingBufferThreshold bufferThreshold =
        new StreamingBufferThreshold(bufferFlushTimeSec, 10000, 10000);

    // mocks to verify
    ScheduledExecutorService flushExecutor = spy(ScheduledExecutorService.class);

    // setup channels
    TopicPartitionChannel amarettoChannel = mock(TopicPartitionChannel.class);
    TopicPartitionChannel scotchChannel = mock(TopicPartitionChannel.class);

    // test executor service
    this.flushService = FlushService.getFlushServiceForTests(flushExecutor, new HashMap<>());
    this.flushService.init();

    // hit threshold with empty map
    Thread.sleep(TimeUnit.SECONDS.toMillis(bufferFlushTimeSec));

    this.flushService.shutdown();
  }
}
