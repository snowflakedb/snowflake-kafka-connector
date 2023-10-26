package com.snowflake.kafka.connector.internal.streaming;

import static com.snowflake.kafka.connector.internal.TestUtils.TEST_CONNECTOR_NAME;

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
import org.junit.Test;

/** IT test related to ChannelExistenceCheckerRequest which Makes server side Calls */
public class StreamingChannelExistenceCheckerTestIT {

  private SnowflakeConnectionService conn = TestUtils.getConnectionServiceForStreaming();

  private String testTableName;

  private static int PARTITION = 0, PARTITION_2 = 1;
  private String topic;
  private TopicPartition topicPartition, topicPartition2;
  private String testChannelName, testChannelName2;

  @Before
  public void beforeEach() {
    testTableName = TestUtils.randomTableName();
    topic = testTableName;
    topicPartition = new TopicPartition(testTableName, PARTITION);

    topicPartition2 = new TopicPartition(testTableName, PARTITION_2);

    testChannelName =
        SnowflakeSinkServiceV2.partitionChannelKey(TEST_CONNECTOR_NAME, testTableName, PARTITION);

    testChannelName2 =
        SnowflakeSinkServiceV2.partitionChannelKey(TEST_CONNECTOR_NAME, testTableName, PARTITION_2);
  }

  @After
  public void afterEach() {
    TestUtils.dropTable(testTableName);
  }

  @Test
  public void testChannelExistenceChecker_validChannel_ClientSequencerMatch() throws Exception {
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

    TestUtils.assertWithRetry(() -> service.getOffset(topicPartition) == noOfRecords, 20, 5);
    assert TestUtils.tableSize(testTableName) == noOfRecords
        : "expected: " + noOfRecords + " actual: " + TestUtils.tableSize(testTableName);

    // At this point we do have a channel.
    // Check if we can get an existence without opening a channel
    // This has default clientSequencer passed in API as 0
    StreamingChannelExistenceChecker streamingChannelExistenceChecker =
        new StreamingChannelExistenceChecker(testTableName, config);
    Assert.assertTrue(
        streamingChannelExistenceChecker.checkChannelExistence(
            SnowflakeSinkServiceV2.partitionChannelKey(TEST_CONNECTOR_NAME, topic, PARTITION)));

    streamingChannelExistenceChecker =
        new StreamingChannelExistenceChecker(testTableName, config, 2L);
    Assert.assertTrue(
        streamingChannelExistenceChecker.checkChannelExistence(
            SnowflakeSinkServiceV2.partitionChannelKey(TEST_CONNECTOR_NAME, topic, PARTITION)));

    service.closeAll();
  }

  @Test
  public void testChannelExistenceChecker_validChannel_ClientSequencerDoesntMatchOnServerSide()
      throws Exception {
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

    TestUtils.assertWithRetry(() -> service.getOffset(topicPartition) == noOfRecords, 20, 5);
    assert TestUtils.tableSize(testTableName) == noOfRecords
        : "expected: " + noOfRecords + " actual: " + TestUtils.tableSize(testTableName);

    SnowflakeSinkServiceV2 snowflakeSinkServiceV2 = (SnowflakeSinkServiceV2) service;
    // Ctor of TopicPartitionChannel tries to open the channel. so clientSequencer == 1
    TopicPartitionChannel channel =
        new TopicPartitionChannel(
            snowflakeSinkServiceV2.getStreamingIngestClient(),
            topicPartition,
            testChannelName,
            testTableName,
            new StreamingBufferThreshold(10, 10_000, 1),
            config,
            new InMemoryKafkaRecordErrorReporter(),
            new InMemorySinkTaskContext(Collections.singleton(topicPartition)),
            conn.getTelemetryClient());

    assert service.getOffset(new TopicPartition(topic, PARTITION)) == noOfRecords;
    assert inMemorySinkTaskContext.offsets().size() == 1;
    assert inMemorySinkTaskContext.offsets().get(topicPartition) == 1;
    assert TestUtils.tableSize(testTableName) == noOfRecords
        : "expected: " + noOfRecords + " actual: " + TestUtils.tableSize(testTableName);

    // At this point we do have a channel but channel sequencer is 1
    // Check if we can get an existence without opening a channel
    // This passes in clientSequencer = 0
    StreamingChannelExistenceChecker streamingChannelExistenceChecker =
        new StreamingChannelExistenceChecker(testTableName, config);
    Assert.assertTrue(
        streamingChannelExistenceChecker.checkChannelExistence(
            SnowflakeSinkServiceV2.partitionChannelKey(TEST_CONNECTOR_NAME, topic, PARTITION)));

    service.closeAll();
  }

  @Test
  public void testChannelExistenceChecker_InvalidChannelName() throws Exception {
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

    // No need to assert insertion/count in table

    StreamingChannelExistenceChecker streamingChannelExistenceChecker =
        new StreamingChannelExistenceChecker(testTableName, config);
    Assert.assertFalse(
        streamingChannelExistenceChecker.checkChannelExistence(
            SnowflakeSinkServiceV2.partitionChannelKey(TEST_CONNECTOR_NAME, topic, PARTITION_2)));

    service.closeAll();
  }
}
