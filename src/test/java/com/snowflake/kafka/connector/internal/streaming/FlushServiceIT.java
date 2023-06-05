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

import com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig;
import com.snowflake.kafka.connector.dlq.InMemoryKafkaRecordErrorReporter;
import com.snowflake.kafka.connector.internal.SnowflakeConnectionService;
import com.snowflake.kafka.connector.internal.SnowflakeSinkServiceFactory;
import com.snowflake.kafka.connector.internal.TestUtils;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class FlushServiceIT {
  // param to run test with or without flush service
  @Parameterized.Parameters(name = "isActive: {0}")
  public static Collection<Object[]> input() {
    return Arrays.asList(new Object[][] {{true}, {false}});
  }

  private final boolean isActive;

  public FlushServiceIT(boolean isActive) {
    this.isActive = isActive;
  }

  private final int WAIT_INTERVAL_SEC = 20;
  private final int MAX_RETRY = 5;

  private SnowflakeConnectionService conn = TestUtils.getConnectionServiceForStreaming();
  private Map<String, String> config;
  private String testTableName;
  private String catTopic;
  private Set<TopicPartition> catTpSet;

  private TopicPartition calicoCatTp;
  private TopicPartition orangeCatTp;

  @Before
  public void beforeEach() {
    // toggle flush service
    FlushService.getFlushServiceInstance().activateScheduledFlushing(this.isActive);

    this.config = TestUtils.getConfForStreaming();
    SnowflakeSinkConnectorConfig.setDefaultValues(this.config);

    // build table name
    this.testTableName = TestUtils.randomTableName();
    this.catTopic = "catTopic_" + this.testTableName;

    // create two tps pointing to table
    this.calicoCatTp = new TopicPartition(this.catTopic, 0);
    this.orangeCatTp = new TopicPartition(this.catTopic, 1);

    // add to set
    this.catTpSet = new HashSet<>();
    this.catTpSet.add(this.calicoCatTp);
    this.catTpSet.add(this.orangeCatTp);
  }

  @After
  public void afterEach() {
    TestUtils.dropTable(this.testTableName);
    FlushService.getFlushServiceInstance().shutdown();
  }

  @Test
  public void testBackgroundFlush() throws Exception {
    // Creates tpChannel with two partitions, sets buffer threshold to 5 records
    SnowflakeSinkServiceV2 sfSinkService =
        (SnowflakeSinkServiceV2)
            SnowflakeSinkServiceFactory.builder(
                    this.conn, IngestionMethodConfig.SNOWPIPE_STREAMING, this.config)
                .setRecordNumber(5)
                .setErrorReporter(new InMemoryKafkaRecordErrorReporter())
                .setSinkTaskContext(new InMemorySinkTaskContext(this.catTpSet))
                .addTask(this.testTableName, this.calicoCatTp)
                .addTask(this.testTableName, this.orangeCatTp)
                .build();

    // verify tpChannels were registered with flush service
    Map<TopicPartition, TopicPartitionChannel> registeredTpChannels =
        FlushService.getFlushServiceInstance().getTopicPartitionsMap();
    assert registeredTpChannels.size() == 2;
    assert registeredTpChannels.containsKey(this.calicoCatTp);
    assert registeredTpChannels.containsKey(this.orangeCatTp);

    // send 5 record to each partition, which should trigger flush
    final long initialNumRecords = 5;

    List<SinkRecord> initialCalicoCatRecords =
        TestUtils.createJsonStringSinkRecords(0, initialNumRecords, this.calicoCatTp.topic(), this.calicoCatTp.partition());
    List<SinkRecord> initialOrangeCatRecords =
        TestUtils.createJsonStringSinkRecords(0, initialNumRecords, this.orangeCatTp.topic(), this.orangeCatTp.partition());
    sfSinkService.insert(initialCalicoCatRecords);
    sfSinkService.insert(initialOrangeCatRecords);

    // send 1 record, under buffer threshold
    final long testNumRecords = 1;
    List<SinkRecord> testCalicoCatRecords =
        TestUtils.createJsonStringSinkRecords(initialNumRecords, testNumRecords, this.calicoCatTp.topic(), this.calicoCatTp.partition());
    List<SinkRecord> testOrangeCatRecords =
        TestUtils.createJsonStringSinkRecords(initialNumRecords, testNumRecords, this.orangeCatTp.topic(), this.orangeCatTp.partition());
    sfSinkService.insert(testCalicoCatRecords);
    sfSinkService.insert(testOrangeCatRecords);

    // if flush service is active, verify that data was committed. otherwise no additional data should be committed
    if (this.isActive) {
      TestUtils.assertWithRetry(
          () -> sfSinkService.getOffset(this.calicoCatTp) == initialNumRecords + testNumRecords, WAIT_INTERVAL_SEC, MAX_RETRY);
      TestUtils.assertWithRetry(
          () -> sfSinkService.getOffset(this.orangeCatTp) == initialNumRecords + testNumRecords, WAIT_INTERVAL_SEC, MAX_RETRY);
      FlushService.getFlushServiceInstance().shutdown();
    } else {
      // else wait max timeout to confirm that the offsets were never updated
      Thread.sleep(TimeUnit.SECONDS.toMillis(WAIT_INTERVAL_SEC * MAX_RETRY));
      assert sfSinkService.getOffset(this.calicoCatTp)
          == initialNumRecords;
      assert sfSinkService.getOffset(this.orangeCatTp)
          == initialNumRecords;
    }
  }
}
