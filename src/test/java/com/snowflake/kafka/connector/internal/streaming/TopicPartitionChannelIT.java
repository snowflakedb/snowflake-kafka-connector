package com.snowflake.kafka.connector.internal.streaming;

import static com.snowflake.kafka.connector.internal.TestUtils.TEST_CONNECTOR_NAME;

import com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig;
import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.dlq.InMemoryKafkaRecordErrorReporter;
import com.snowflake.kafka.connector.internal.SnowflakeConnectionService;
import com.snowflake.kafka.connector.internal.SnowflakeSinkService;
import com.snowflake.kafka.connector.internal.SnowflakeSinkServiceFactory;
import com.snowflake.kafka.connector.internal.TestUtils;
import com.snowflake.kafka.connector.internal.streaming.telemetry.SnowflakeTelemetryServiceV2;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import net.snowflake.ingest.streaming.OpenChannelRequest;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TopicPartitionChannelIT {

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
    topicPartition = new TopicPartition(topic, PARTITION);

    topicPartition2 = new TopicPartition(topic, PARTITION_2);

    testChannelName =
        SnowflakeSinkServiceV2.partitionChannelKey(TEST_CONNECTOR_NAME, topic, PARTITION);

    testChannelName2 =
        SnowflakeSinkServiceV2.partitionChannelKey(TEST_CONNECTOR_NAME, topic, PARTITION_2);
  }

  @After
  public void afterEach() {
    TestUtils.dropTable(testTableName);
  }

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
            new InMemorySinkTaskContext(Collections.singleton(topicPartition)),
            conn.getTelemetryClient());

    // since channel is updated, try to insert data again or may be call getOffsetToken
    // We will reopen the channel in since the older channel in service is stale because we
    // externally created a new channel but did not update the partitionsToChannel cache.
    // This will retry three times, reopen the channel, replace the newly created channel in cache
    // and fetch the offset again.
    assert service.getOffset(new TopicPartition(topic, PARTITION)) == noOfRecords;
    assert inMemorySinkTaskContext.offsets().size() == 1;
    assert inMemorySinkTaskContext.offsets().get(topicPartition) == 1;
    assert TestUtils.tableSize(testTableName) == noOfRecords
        : "expected: " + noOfRecords + " actual: " + TestUtils.tableSize(testTableName);
    service.closeAll();
  }

  /* This will automatically open the channel. */
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
    assert TestUtils.tableSize(testTableName) == noOfRecords + noOfRecords
        : "expected: "
            + (noOfRecords + noOfRecords)
            + " actual: "
            + TestUtils.tableSize(testTableName);
    service.closeAll();
  }

  /**
   * Insert data - Success
   *
   * <p>Fetch channel object and close the channel.
   *
   * <p>Insert New offsets -> The insert operation should automatically create a new channel and
   * insert data.
   */
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

    Assert.assertTrue(
        topicPartitionChannel.getTelemetryServiceV2() instanceof SnowflakeTelemetryServiceV2);

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
    assert topicPartitionChannel.fetchOffsetTokenWithRetry()
        == (anotherSetOfRecords + noOfRecords - 1);
    assert TestUtils.tableSize(testTableName) == noOfRecords + anotherSetOfRecords
        : "expected: "
            + (noOfRecords + anotherSetOfRecords)
            + " actual: "
            + TestUtils.tableSize(testTableName);
  }

  /**
   * Two partitions for a topic Partition 1 -> 10(0-9) records -> Success Partition 2 -> 10(0-9)
   * records -> Success
   *
   * <p>Partition 1 -> Channel 1 -> open with same client sequencer for channel 1 - 1
   *
   * <p>Partition 1 -> 10(10-19) records -> Failure -> reopen -> fetch offset token Client sequencer
   * for channel 1 - 2
   *
   * <p>Partition 2 -> 10(10-19) records -> Success
   *
   * <p>Eventually 40 records should be present in snowflake table
   */
  @Test
  public void testAutoChannelReopen_MultiplePartitionsInsertRowsSFException() throws Exception {
    Map<String, String> config = TestUtils.getConfForStreaming();
    SnowflakeSinkConnectorConfig.setDefaultValues(config);
    config.put(SnowflakeSinkConnectorConfig.ENABLE_STREAMING_CLIENT_OPTIMIZATION_CONFIG, "true");

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

    SnowflakeStreamingIngestClient client =
        ((SnowflakeSinkServiceV2) service).getStreamingIngestClient();
    OpenChannelRequest channelRequest =
        OpenChannelRequest.builder(testChannelName)
            .setDBName(config.get(Utils.SF_DATABASE))
            .setSchemaName(config.get(Utils.SF_SCHEMA))
            .setTableName(this.testTableName)
            .setOnErrorOption(OpenChannelRequest.OnErrorOption.CONTINUE)
            .build();

    // Open a channel with same name will bump up the client sequencer number for this channel
    client.openChannel(channelRequest);

    assert TestUtils.getClientSequencerForChannelAndTable(testTableName, testChannelName) == 1;

    // send offset 10 - 19 -> We should get insertRows failure and hence reopen channel would kick
    // in
    final long anotherSetOfRecords = 10;
    records =
        TestUtils.createJsonStringSinkRecords(
            anotherSetOfRecords, anotherSetOfRecords, topic, PARTITION);

    // InsertRows operation should fail since the internal channel object is still linked with
    // clientSequencer 0 and, we opened a new channel earlier which set the clientSequencer on
    // server side to 1
    service.insert(records);

    // Will need to retry which should succeed (Retry is mimicking the reset of kafka offsets, which
    // will send offsets from 10 since last committed offset in Snowflake is 9)
    service.insert(records);

    records =
        TestUtils.createJsonStringSinkRecords(
            anotherSetOfRecords, anotherSetOfRecords, topic, PARTITION_2);

    // Send records to partition 2 which should not be affected. (Since there were no overlapping
    // offsets in blob)
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

    assert TestUtils.getClientSequencerForChannelAndTable(testTableName, testChannelName) == 2;
    assert TestUtils.getOffsetTokenForChannelAndTable(testTableName, testChannelName)
        == (recordsInPartition1 + anotherSetOfRecords - 1);

    assert TestUtils.getClientSequencerForChannelAndTable(testTableName, testChannelName2) == 0;
    assert TestUtils.getOffsetTokenForChannelAndTable(testTableName, testChannelName2)
            == (recordsInPartition2 + anotherSetOfRecords - 1)
        : "expected: "
            + (recordsInPartition2 + anotherSetOfRecords - 1)
            + " actual: "
            + TestUtils.getOffsetTokenForChannelAndTable(testTableName, testChannelName2);

    assert TestUtils.tableSize(testTableName)
            == recordsInPartition1 + anotherSetOfRecords + recordsInPartition2 + anotherSetOfRecords
        : "expected: "
            + (recordsInPartition1
                + anotherSetOfRecords
                + recordsInPartition2
                + anotherSetOfRecords)
            + " actual: "
            + TestUtils.tableSize(testTableName);
    service.closeAll();
  }

  @Test
  public void testAutoChannelReopen_SinglePartitionsInsertRowsSFException() throws Exception {
    Map<String, String> config = TestUtils.getConfForStreaming();
    SnowflakeSinkConnectorConfig.setDefaultValues(config);
    config.put(SnowflakeSinkConnectorConfig.ENABLE_STREAMING_CLIENT_OPTIMIZATION_CONFIG, "true");

    InMemorySinkTaskContext inMemorySinkTaskContext =
        new InMemorySinkTaskContext(Collections.singleton(topicPartition));

    // This will automatically create a channel for topicPartition.
    SnowflakeSinkService service =
        SnowflakeSinkServiceFactory.builder(conn, IngestionMethodConfig.SNOWPIPE_STREAMING, config)
            .setRecordNumber(10)
            .setFlushTime(5)
            .setErrorReporter(new InMemoryKafkaRecordErrorReporter())
            .setSinkTaskContext(inMemorySinkTaskContext)
            .addTask(testTableName, topicPartition)
            .build();

    final int recordsInPartition1 = 10;
    List<SinkRecord> recordsPartition1 =
        TestUtils.createJsonStringSinkRecords(0, recordsInPartition1, topic, PARTITION);

    List<SinkRecord> records = new ArrayList<>(recordsPartition1);

    service.insert(records);

    TestUtils.assertWithRetry(
        () -> service.getOffset(new TopicPartition(topic, PARTITION)) == recordsInPartition1,
        20,
        5);

    SnowflakeStreamingIngestClient client =
        ((SnowflakeSinkServiceV2) service).getStreamingIngestClient();
    OpenChannelRequest channelRequest =
        OpenChannelRequest.builder(testChannelName)
            .setDBName(config.get(Utils.SF_DATABASE))
            .setSchemaName(config.get(Utils.SF_SCHEMA))
            .setTableName(this.testTableName)
            .setOnErrorOption(OpenChannelRequest.OnErrorOption.CONTINUE)
            .build();
    client.openChannel(channelRequest);
    Thread.sleep(5_000);

    // send offset 10 - 19
    final long anotherSetOfRecords = 10;
    records =
        TestUtils.createJsonStringSinkRecords(
            anotherSetOfRecords, anotherSetOfRecords, topic, PARTITION);

    // It will reopen a channel since current channel has been in validated
    service.insert(records);

    // Will need to retry
    service.insert(records);

    TestUtils.assertWithRetry(
        () ->
            service.getOffset(new TopicPartition(topic, PARTITION))
                == recordsInPartition1 + anotherSetOfRecords,
        20,
        5);

    assert TestUtils.getClientSequencerForChannelAndTable(testTableName, testChannelName) == 2;
    assert TestUtils.getOffsetTokenForChannelAndTable(testTableName, testChannelName)
        == (recordsInPartition1 + anotherSetOfRecords - 1);

    assert TestUtils.tableSize(testTableName) == recordsInPartition1 + anotherSetOfRecords
        : "expected: "
            + (recordsInPartition1 + anotherSetOfRecords)
            + " actual: "
            + TestUtils.tableSize(testTableName);
    service.closeAll();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSimpleInsertRowsFailureWithArrowBDECFormat() throws Exception {
    // add config which overrides the bdec file format
    Map<String, String> overriddenConfig = new HashMap<>(TestUtils.getConfForStreaming());
    overriddenConfig.put(SnowflakeSinkConnectorConfig.SNOWPIPE_STREAMING_FILE_VERSION, "1");

    InMemorySinkTaskContext inMemorySinkTaskContext =
        new InMemorySinkTaskContext(Collections.singleton(topicPartition));

    // This will automatically create a channel for topicPartition.
    SnowflakeSinkService service =
        SnowflakeSinkServiceFactory.builder(
                conn, IngestionMethodConfig.SNOWPIPE_STREAMING, overriddenConfig)
            .setRecordNumber(1)
            .setErrorReporter(new InMemoryKafkaRecordErrorReporter())
            .setSinkTaskContext(inMemorySinkTaskContext)
            .addTask(testTableName, topicPartition)
            .build();

    final long noOfRecords = 1;

    // send regular data
    List<SinkRecord> records =
        TestUtils.createJsonStringSinkRecords(0, noOfRecords, topic, PARTITION);

    // should throw because we don't take arrow version 1 anymore
    service.insert(records);
    service.closeAll();
  }

  @Test
  public void testPartialBatchChannelInvalidationIngestion_schematization() throws Exception {
    Map<String, String> config = TestUtils.getConfForStreaming();
    config.put(
        SnowflakeSinkConnectorConfig.BUFFER_COUNT_RECORDS, "500"); // we want to flush on record
    config.put(SnowflakeSinkConnectorConfig.BUFFER_FLUSH_TIME_SEC, "500000");
    config.put(SnowflakeSinkConnectorConfig.BUFFER_SIZE_BYTES, "500000");
    config.put(
        SnowflakeSinkConnectorConfig.ENABLE_SCHEMATIZATION_CONFIG,
        "true"); // using schematization to invalidate

    // setup
    InMemorySinkTaskContext inMemorySinkTaskContext =
        new InMemorySinkTaskContext(Collections.singleton(topicPartition));
    SnowflakeSinkService service =
        SnowflakeSinkServiceFactory.builder(conn, IngestionMethodConfig.SNOWPIPE_STREAMING, config)
            .setRecordNumber(1)
            .setErrorReporter(new InMemoryKafkaRecordErrorReporter())
            .setSinkTaskContext(inMemorySinkTaskContext)
            .addTask(testTableName, topicPartition)
            .build();

    final long firstBatchCount = 18;
    final long secondBatchCount = 500;

    // create 18 blank records that do not kick off schematization
    JsonConverter converter = new JsonConverter();
    HashMap<String, String> converterConfig = new HashMap<>();
    converterConfig.put("schemas.enable", "false");
    converter.configure(converterConfig, false);
    SchemaAndValue schemaInputValue = converter.toConnectData("test", null);

    List<SinkRecord> firstBatch = new ArrayList<>();
    for (int i = 0; i < firstBatchCount; i++) {
      firstBatch.add(
          new SinkRecord(
              topic,
              PARTITION,
              Schema.STRING_SCHEMA,
              "test",
              schemaInputValue.schema(),
              schemaInputValue.value(),
              i));
    }

    service.insert(firstBatch);

    // send batch with 500, should kick off a record based flush and schematization on record 19,
    // which will fail the batches
    List<SinkRecord> secondBatch =
        TestUtils.createNativeJsonSinkRecords(firstBatchCount, secondBatchCount, topic, PARTITION);
    service.insert(secondBatch);

    // resend batch 1 and 2 because 2 failed for schematization
    service.insert(firstBatch);
    service.insert(secondBatch);

    // ensure all data was ingested
    TestUtils.assertWithRetry(
        () ->
            service.getOffset(new TopicPartition(topic, PARTITION))
                == firstBatchCount + secondBatchCount,
        20,
        5);
    assert TestUtils.tableSize(testTableName) == firstBatchCount + secondBatchCount
        : "expected: "
            + firstBatchCount
            + secondBatchCount
            + " actual: "
            + TestUtils.tableSize(testTableName);

    service.closeAll();
  }
}
