package com.snowflake.kafka.connector.internal.streaming;

import com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig;
import com.snowflake.kafka.connector.dlq.InMemoryKafkaRecordErrorReporter;
import com.snowflake.kafka.connector.internal.SnowflakeConnectionService;
import com.snowflake.kafka.connector.internal.SnowflakeSinkService;
import com.snowflake.kafka.connector.internal.SnowflakeSinkServiceFactory;
import com.snowflake.kafka.connector.internal.TestUtils;
import java.util.ArrayList;
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

  private static int PARTITION = 0, PARTITION_2 = 1;
  private String topic;
  private TopicPartition topicPartition, topicPartition2;
  private String testChannelName, testChannelName2;

  @Before
  public void beforeEach() {
    testTableName = TestUtils.randomTableName();
    topic = testTableName;
    topicPartition = new TopicPartition(topic, PARTITION);

    topicPartition2 = new TopicPartition(topic, PARTITION_2);

    testChannelName = SnowflakeSinkServiceV2.partitionChannelKey(topic, PARTITION);

    testChannelName2 = SnowflakeSinkServiceV2.partitionChannelKey(topic, PARTITION_2);
  }

  @After
  public void afterEach() {
    TestUtils.dropTableStreaming(testTableName);
  }

  @Ignore
  @Test
  public void testAutoChannelReopenOn_OffsetTokenSFException() throws Exception {
    Map<String, String> config = TestUtils.getConfForStreaming();
    SnowflakeSinkConnectorConfig.setDefaultValues(config);

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
            testTableName,
            new StreamingBufferThreshold(10, 10_000, 1),
            config,
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

  @Ignore
  @Test
  public void testAutoChannelReopen_InsertRowsSFException() throws Exception {
    Map<String, String> config = TestUtils.getConfForStreaming();
    SnowflakeSinkConnectorConfig.setDefaultValues(config);

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

    // Closing the channel for mimicking SFException in insertRows
    TopicPartitionChannel topicPartitionChannel =
        snowflakeSinkServiceV2.getTopicPartitionChannelFromCacheKey(testChannelName).get();

    Assert.assertNotNull(topicPartitionChannel);

    // close channel
    topicPartitionChannel.closeChannel();

    // verify channel is closed.
    Assert.assertTrue(topicPartitionChannel.isChannelClosed());

    // send offset 1 - 6
    final long anotherSetOfRecords = 5;
    records = TestUtils.createJsonStringSinkRecords(1, anotherSetOfRecords, topic, PARTITION);

    // It will reopen a channel since it was closed (startTask)
    service.insert(records);

    // Trying to insert same offsets, hoping it would be rejected (Not added to buffer)
    service.insert(records);

    TestUtils.assertWithRetry(
        () ->
            service.getOffset(new TopicPartition(topic, PARTITION))
                == anotherSetOfRecords + noOfRecords,
        20,
        5);

    assert TestUtils.getClientSequencerForChannelAndTable(testTableName, testChannelName) == 1;
    assert TestUtils.getOffsetTokenForChannelAndTable(testTableName, testChannelName)
        == (anotherSetOfRecords + noOfRecords - 1);
  }

  @Ignore
  @Test
  public void testAutoChannelReopen_MultiplePartitionsInsertRowsSFException() throws Exception {
    Map<String, String> config = TestUtils.getConfForStreaming();
    SnowflakeSinkConnectorConfig.setDefaultValues(config);

    InMemorySinkTaskContext inMemorySinkTaskContext =
        new InMemorySinkTaskContext(Collections.singleton(topicPartition));

    // This will automatically create a channel for topicPartition.
    SnowflakeSinkService service =
        SnowflakeSinkServiceFactory.builder(conn, IngestionMethodConfig.SNOWPIPE_STREAMING, config)
            .setRecordNumber(5)
            .setFlushTime(5)
            .setErrorReporter(new InMemoryKafkaRecordErrorReporter())
            .setSinkTaskContext(inMemorySinkTaskContext)
            .addTask(testTableName, topicPartition)
            .addTask(testTableName, topicPartition2)
            .build();

    final int recordsInPartition1 = 10;
    final int recordsInPartition2 = 10;
    List<SinkRecord> recordsPartition1 =
        TestUtils.createJsonStringSinkRecords(0, recordsInPartition1, topic, PARTITION);

    List<SinkRecord> recordsPartition2 =
        TestUtils.createJsonStringSinkRecords(0, recordsInPartition2, topic, PARTITION_2);

    List<SinkRecord> records = new ArrayList<>(recordsPartition1);
    records.addAll(recordsPartition2);

    service.insert(records);

    TestUtils.assertWithRetry(
        () -> service.getOffset(new TopicPartition(topic, PARTITION)) == recordsInPartition1,
        20,
        5);

    TestUtils.assertWithRetry(
        () -> service.getOffset(new TopicPartition(topic, PARTITION_2)) == recordsInPartition2,
        20,
        5);

    // send offset 10 - 19
    final long anotherSetOfRecords = 10;
    records =
        TestUtils.createJsonStringSinkRecords(
            anotherSetOfRecords, anotherSetOfRecords, topic, PARTITION);

    // It will reopen a channel since current channel has been in validated
    service.insert(records);

    records =
        TestUtils.createJsonStringSinkRecords(
            anotherSetOfRecords, anotherSetOfRecords, topic, PARTITION_2);
    // Trying to insert same offsets, hoping it would be rejected (Not added to buffer)
    service.insert(records);

    TestUtils.assertWithRetry(
        () ->
            service.getOffset(new TopicPartition(topic, PARTITION))
                == recordsInPartition1 + anotherSetOfRecords,
        20,
        5);
    TestUtils.assertWithRetry(
        () ->
            service.getOffset(new TopicPartition(topic, PARTITION_2))
                == recordsInPartition2 + anotherSetOfRecords,
        20,
        5);

    assert TestUtils.getClientSequencerForChannelAndTable(testTableName, testChannelName) == 0;
    assert TestUtils.getOffsetTokenForChannelAndTable(testTableName, testChannelName)
        == (recordsInPartition1 + anotherSetOfRecords - 1);

    assert TestUtils.getClientSequencerForChannelAndTable(testTableName, testChannelName2) == 0;
    assert TestUtils.getOffsetTokenForChannelAndTable(testTableName, testChannelName2)
        == (recordsInPartition2 + anotherSetOfRecords - 1);
  }
}
