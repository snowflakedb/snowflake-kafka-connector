package com.snowflake.kafka.connector.internal.streaming;

import static com.snowflake.kafka.connector.Utils.SF_DATABASE;
import static com.snowflake.kafka.connector.Utils.SF_SCHEMA;

import com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig;
import com.snowflake.kafka.connector.dlq.InMemoryKafkaRecordErrorReporter;
import com.snowflake.kafka.connector.internal.SnowflakeConnectionService;
import com.snowflake.kafka.connector.internal.SnowflakeSinkService;
import com.snowflake.kafka.connector.internal.SnowflakeSinkServiceFactory;
import com.snowflake.kafka.connector.internal.TestUtils;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class TopicPartitionChannelIT {

  private SnowflakeConnectionService conn = TestUtils.getConnectionServiceForStreamingIngest();
  private String testTableName;

  private static int PARTITION = 0;
  private String topic;
  private TopicPartition topicPartition;

  private String testChannelName, testDb, testSc;

  @Before
  public void beforeEach() {
    testTableName = TestUtils.randomTableName();
    topic = testTableName;
    topicPartition = new TopicPartition(topic, PARTITION);

    testChannelName = SnowflakeSinkServiceV2.partitionChannelKey(topic, PARTITION);
  }

  @After
  public void afterEach() {
    TestUtils.dropTableStreaming(testTableName);
  }

  @Ignore
  @Test
  public void testAutoChannelReopenOnSFException() throws Exception {
    Map<String, String> config = TestUtils.getConfForStreaming();
    SnowflakeSinkConnectorConfig.setDefaultValues(config);
    testDb = config.get(SF_DATABASE);
    testSc = config.get(SF_SCHEMA);

    InMemorySinkTaskContext inMemorySinkTaskContext =
        new InMemorySinkTaskContext(Collections.singleton(topicPartition));

    // This will automatically create a channel for topicPartition.
    SnowflakeSinkService service =
        SnowflakeSinkServiceFactory.builder(conn, IngestionMethodConfig.SNOWPIPE_STREAMING, config)
            .setRecordNumber(1)
            .setErrorReporter(new InMemoryKafkaRecordErrorReporter())
            .setSinkTaskContext(inMemorySinkTaskContext)
            .addTask(testTableName, topicPartition)
            .build();

    final long noOfRecords = 1;

    // send regular data
    List<SinkRecord> records =
        TestUtils.createJsonStringSinkRecords(0, noOfRecords, topic, PARTITION);

    service.insert(records);

    TestUtils.assertWithRetry(
        () -> service.getOffset(new TopicPartition(topic, PARTITION)) == noOfRecords, 20, 5);

    SnowflakeSinkServiceV2 snowflakeSinkServiceV2 = (SnowflakeSinkServiceV2) service;

    // Ctor of TopicPartitionChannel tries to open the channel.
    TopicPartitionChannel channel =
        new TopicPartitionChannel(
            snowflakeSinkServiceV2.getStreamingIngestClient(),
            topicPartition,
            testChannelName,
            testDb,
            testSc,
            testTableName,
            new InMemoryKafkaRecordErrorReporter(),
            new InMemorySinkTaskContext(Collections.singleton(topicPartition)));

    // since channel is updated, try to insert data again or may be call getOffsetToken
    // We will reopen the channel in since the older channel in service is stale because we
    // externally created a new channel but didnt update the partitionsToChannel cache.
    // This will retry three times, reopen the channel, replace the newly created channel in cache
    // and fetch the offset again.
    assert service.getOffset(new TopicPartition(topic, PARTITION)) == noOfRecords;
    assert inMemorySinkTaskContext.offsets().size() == 1;
    assert inMemorySinkTaskContext.offsets().get(topicPartition) == 1;
  }

  /* This will automatically open the channel. */
  @Ignore
  @Test
  public void testInsertRowsOnChannelClosed() throws Exception {
    Map<String, String> config = TestUtils.getConfForStreaming();
    SnowflakeSinkConnectorConfig.setDefaultValues(config);
    testDb = config.get(SF_DATABASE);
    testSc = config.get(SF_SCHEMA);

    InMemorySinkTaskContext inMemorySinkTaskContext =
        new InMemorySinkTaskContext(Collections.singleton(topicPartition));

    // This will automatically create a channel for topicPartition.
    SnowflakeSinkService service =
        SnowflakeSinkServiceFactory.builder(conn, IngestionMethodConfig.SNOWPIPE_STREAMING, config)
            .setRecordNumber(1)
            .setErrorReporter(new InMemoryKafkaRecordErrorReporter())
            .setSinkTaskContext(inMemorySinkTaskContext)
            .addTask(testTableName, topicPartition)
            .build();

    final long noOfRecords = 1;

    // send regular data
    List<SinkRecord> records =
        TestUtils.createJsonStringSinkRecords(0, noOfRecords, topic, PARTITION);

    service.insert(records);

    TestUtils.assertWithRetry(
        () -> service.getOffset(new TopicPartition(topic, PARTITION)) == noOfRecords, 20, 5);

    SnowflakeSinkServiceV2 snowflakeSinkServiceV2 = (SnowflakeSinkServiceV2) service;

    TopicPartitionChannel topicPartitionChannel =
        snowflakeSinkServiceV2.getTopicPartitionChannelFromCacheKey(testChannelName).get();

    Assert.assertNotNull(topicPartitionChannel);

    // close channel
    topicPartitionChannel.closeChannel();

    // verify channel is closed.
    Assert.assertTrue(topicPartitionChannel.isChannelClosed());

    // send offset 1
    records = TestUtils.createJsonStringSinkRecords(1, noOfRecords, topic, PARTITION);

    service.insert(records);

    TestUtils.assertWithRetry(
        () -> service.getOffset(new TopicPartition(topic, PARTITION)) == 2, 20, 5);
  }
}
