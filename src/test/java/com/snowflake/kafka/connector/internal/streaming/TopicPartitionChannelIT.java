package com.snowflake.kafka.connector.internal.streaming;

import static com.snowflake.kafka.connector.internal.streaming.ChannelMigrationResponseCode.SUCCESS;
import static com.snowflake.kafka.connector.internal.streaming.ChannelMigrationResponseCode.isChannelMigrationResponseSuccessful;
import static com.snowflake.kafka.connector.internal.streaming.channel.TopicPartitionChannel.NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE;

import com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig;
import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.dlq.InMemoryKafkaRecordErrorReporter;
import com.snowflake.kafka.connector.internal.SnowflakeConnectionService;
import com.snowflake.kafka.connector.internal.SnowflakeSinkService;
import com.snowflake.kafka.connector.internal.SnowflakeSinkServiceFactory;
import com.snowflake.kafka.connector.internal.TestUtils;
import com.snowflake.kafka.connector.internal.streaming.channel.TopicPartitionChannel;
import com.snowflake.kafka.connector.internal.streaming.schemaevolution.InsertErrorMapper;
import com.snowflake.kafka.connector.internal.streaming.schemaevolution.snowflake.SnowflakeSchemaEvolutionService;
import com.snowflake.kafka.connector.internal.streaming.telemetry.SnowflakeTelemetryServiceV2;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import net.snowflake.ingest.streaming.OpenChannelRequest;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TopicPartitionChannelIT {

  private static final SnowflakeConnectionService conn =
      TestUtils.getConnectionServiceForStreaming();
  private String testTableName;

  private static final int PARTITION = 0, PARTITION_2 = 1;
  private String topic;
  private TopicPartition topicPartition, topicPartition2;
  private String testChannelName, testChannelName2;

  @BeforeEach
  public void beforeEach() {
    testTableName = TestUtils.randomTableName();
    topic = testTableName;
    topicPartition = new TopicPartition(topic, PARTITION);

    topicPartition2 = new TopicPartition(topic, PARTITION_2);

    testChannelName = SnowflakeSinkServiceV2.partitionChannelKey(topic, PARTITION);

    testChannelName2 = SnowflakeSinkServiceV2.partitionChannelKey(topic, PARTITION_2);
  }

  @AfterEach
  public void afterEach() {
    TestUtils.dropTable(testTableName);
  }

  @AfterAll
  public static void afterAll() {
    conn.close();
  }

  @Test
  public void testAutoChannelReopenOn_OffsetTokenSFException() throws Exception {
    Map<String, String> config = getConfForStreaming();
    SnowflakeSinkConnectorConfig.setDefaultValues(config);

    InMemorySinkTaskContext inMemorySinkTaskContext =
        new InMemorySinkTaskContext(Collections.singleton(topicPartition));

    // This will automatically create a channel for topicPartition.
    SnowflakeSinkService service =
        SnowflakeSinkServiceFactory.builder(conn, IngestionMethodConfig.SNOWPIPE_STREAMING, config)
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
        new DirectTopicPartitionChannel(
            snowflakeSinkServiceV2.getStreamingIngestClient(),
            topicPartition,
            testChannelName,
            testTableName,
            config,
            new InMemoryKafkaRecordErrorReporter(),
            new InMemorySinkTaskContext(Collections.singleton(topicPartition)),
            conn,
            conn.getTelemetryClient(),
            new SnowflakeSchemaEvolutionService(conn),
            new InsertErrorMapper());

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
    Map<String, String> config = getConfForStreaming();
    SnowflakeSinkConnectorConfig.setDefaultValues(config);

    InMemorySinkTaskContext inMemorySinkTaskContext =
        new InMemorySinkTaskContext(Collections.singleton(topicPartition));

    // This will automatically create a channel for topicPartition.
    SnowflakeSinkService service =
        SnowflakeSinkServiceFactory.builder(conn, IngestionMethodConfig.SNOWPIPE_STREAMING, config)
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

    Assertions.assertNotNull(topicPartitionChannel);

    // close channel
    topicPartitionChannel.closeChannel();

    // verify channel is closed.
    Assertions.assertTrue(topicPartitionChannel.isChannelClosed());

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
    Map<String, String> config = getConfForStreaming();
    SnowflakeSinkConnectorConfig.setDefaultValues(config);

    InMemorySinkTaskContext inMemorySinkTaskContext =
        new InMemorySinkTaskContext(Collections.singleton(topicPartition));

    // This will automatically create a channel for topicPartition.
    SnowflakeSinkService service =
        SnowflakeSinkServiceFactory.builder(conn, IngestionMethodConfig.SNOWPIPE_STREAMING, config)
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

    Assertions.assertNotNull(topicPartitionChannel);

    Assertions.assertTrue(
        topicPartitionChannel.getTelemetryServiceV2() instanceof SnowflakeTelemetryServiceV2);

    // close channel
    topicPartitionChannel.closeChannel();

    // verify channel is closed.
    Assertions.assertTrue(topicPartitionChannel.isChannelClosed());

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
    Map<String, String> config = getConfForStreaming();
    SnowflakeSinkConnectorConfig.setDefaultValues(config);
    config.put(SnowflakeSinkConnectorConfig.ENABLE_STREAMING_CLIENT_OPTIMIZATION_CONFIG, "true");

    InMemorySinkTaskContext inMemorySinkTaskContext =
        new InMemorySinkTaskContext(Collections.singleton(topicPartition));

    // This will automatically create a channel for topicPartition.
    SnowflakeSinkService service =
        SnowflakeSinkServiceFactory.builder(conn, IngestionMethodConfig.SNOWPIPE_STREAMING, config)
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
    Map<String, String> config = getConfForStreaming();
    SnowflakeSinkConnectorConfig.setDefaultValues(config);
    config.put(SnowflakeSinkConnectorConfig.ENABLE_STREAMING_CLIENT_OPTIMIZATION_CONFIG, "true");

    InMemorySinkTaskContext inMemorySinkTaskContext =
        new InMemorySinkTaskContext(Collections.singleton(topicPartition));

    // This will automatically create a channel for topicPartition.
    SnowflakeSinkService service =
        SnowflakeSinkServiceFactory.builder(conn, IngestionMethodConfig.SNOWPIPE_STREAMING, config)
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

  @Test
  public void testPartialBatchChannelInvalidationIngestion_schematization() throws Exception {
    Map<String, String> config = getConfForStreaming();
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
            .setErrorReporter(new InMemoryKafkaRecordErrorReporter())
            .setSinkTaskContext(inMemorySinkTaskContext)
            .addTask(testTableName, topicPartition)
            .build();

    final long firstBatchCount = 18;
    final long secondBatchCount = 500;

    // create 18 blank records that do not kick off schematization
    List<SinkRecord> firstBatch =
        TestUtils.createBlankJsonSinkRecords(0, firstBatchCount, topic, PARTITION);

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

  @Test
  public void testChannelMigrateOffsetTokenSystemFunction_NonNullOffsetTokenForSourceChannel()
      throws Exception {
    Map<String, String> config = getConfForStreaming();
    SnowflakeSinkConnectorConfig.setDefaultValues(config);

    InMemorySinkTaskContext inMemorySinkTaskContext =
        new InMemorySinkTaskContext(Collections.singleton(topicPartition));

    // This will automatically create a channel for topicPartition.
    SnowflakeSinkService service =
        SnowflakeSinkServiceFactory.builder(conn, IngestionMethodConfig.SNOWPIPE_STREAMING, config)
            .setErrorReporter(new InMemoryKafkaRecordErrorReporter())
            .setSinkTaskContext(inMemorySinkTaskContext)
            .addTask(testTableName, topicPartition)
            .build();

    TopicPartitionChannel topicPartitionChannel =
        ((SnowflakeSinkServiceV2) service)
            .getTopicPartitionChannelFromCacheKey(testChannelName)
            .get();
    // Channel does exist
    Assertions.assertNotNull(topicPartitionChannel);

    // get the corresponding V2 format for above topic partition channel
    final String channelNameFormatV2 =
        TopicPartitionChannel.generateChannelNameFormatV2(testChannelName, conn.getConnectorName());

    // create a channel with new format and ingest few rows
    // Ctor of TopicPartitionChannel tries to open the channel (new format) for same partition
    TopicPartitionChannel topicPartitionChannelForFormatV2 =
        new DirectTopicPartitionChannel(
            ((SnowflakeSinkServiceV2) service).getStreamingIngestClient(),
            topicPartition,
            channelNameFormatV2,
            testTableName,
            config,
            new InMemoryKafkaRecordErrorReporter(),
            new InMemorySinkTaskContext(Collections.singleton(topicPartition)),
            conn,
            conn.getTelemetryClient(),
            new SnowflakeSchemaEvolutionService(conn),
            new InsertErrorMapper());

    // insert few records via new channel
    final int noOfRecords = 5;
    // Since record 0 was not able to ingest, all records in this batch will not be added into the
    // buffer.
    List<SinkRecord> records =
        TestUtils.createJsonStringSinkRecords(0, noOfRecords, testTableName, PARTITION);

    for (int idx = 0; idx < records.size(); idx++) {
      topicPartitionChannelForFormatV2.insertRecord(records.get(idx), idx == 0);
    }
    TestUtils.assertWithRetry(
        () -> topicPartitionChannelForFormatV2.getOffsetSafeToCommitToKafka() == noOfRecords, 5, 5);

    // we migrate the offset from new channel format to old channel format
    ChannelMigrateOffsetTokenResponseDTO channelMigrateOffsetTokenResponseDTO =
        conn.migrateStreamingChannelOffsetToken(
            testTableName, channelNameFormatV2, testChannelName);
    Assertions.assertTrue(
        isChannelMigrationResponseSuccessful(channelMigrateOffsetTokenResponseDTO));
    Assertions.assertEquals(
        SUCCESS.getStatusCode(), channelMigrateOffsetTokenResponseDTO.getResponseCode());

    // Fetch offsetToken from API should now give you same as other channel
    TestUtils.assertWithRetry(
        () -> service.getOffset(new TopicPartition(topic, PARTITION)) == noOfRecords, 5, 5);

    // add few more records
    records =
        TestUtils.createJsonStringSinkRecords(noOfRecords, noOfRecords, testTableName, PARTITION);
    service.insert(records);
    TestUtils.assertWithRetry(
        () -> service.getOffset(new TopicPartition(topic, PARTITION)) == noOfRecords + noOfRecords,
        5,
        5);

    service.closeAll();
  }

  @Test
  public void testChannelMigrateOffsetTokenSystemFunction_NullOffsetTokenInFormatV2()
      throws Exception {
    Map<String, String> config = getConfForStreaming();
    SnowflakeSinkConnectorConfig.setDefaultValues(config);

    InMemorySinkTaskContext inMemorySinkTaskContext =
        new InMemorySinkTaskContext(Collections.singleton(topicPartition));

    // This will automatically create a channel for topicPartition.
    SnowflakeSinkService service =
        SnowflakeSinkServiceFactory.builder(conn, IngestionMethodConfig.SNOWPIPE_STREAMING, config)
            .setErrorReporter(new InMemoryKafkaRecordErrorReporter())
            .setSinkTaskContext(inMemorySinkTaskContext)
            .addTask(testTableName, topicPartition)
            .build();

    TopicPartitionChannel topicPartitionChannel =
        ((SnowflakeSinkServiceV2) service)
            .getTopicPartitionChannelFromCacheKey(testChannelName)
            .get();
    // Channel does exist
    Assertions.assertNotNull(topicPartitionChannel);

    final int recordsInPartition1 = 10;
    List<SinkRecord> recordsPartition1 =
        TestUtils.createJsonStringSinkRecords(0, recordsInPartition1, topic, PARTITION);

    List<SinkRecord> records = new ArrayList<>(recordsPartition1);

    service.insert(records);

    TestUtils.assertWithRetry(
        () -> service.getOffset(new TopicPartition(topic, PARTITION)) == recordsInPartition1, 5, 5);

    // get the corresponding V2 format for above topic partition channel
    final String channelNameFormatV2 =
        TopicPartitionChannel.generateChannelNameFormatV2(testChannelName, conn.getConnectorName());

    // create a channel with new format and dont ingest anything
    // Ctor of TopicPartitionChannel tries to open the channel (new format) for same partition
    TopicPartitionChannel topicPartitionChannelForFormatV2 =
        new DirectTopicPartitionChannel(
            ((SnowflakeSinkServiceV2) service).getStreamingIngestClient(),
            topicPartition,
            channelNameFormatV2,
            testTableName,
            config,
            new InMemoryKafkaRecordErrorReporter(),
            new InMemorySinkTaskContext(Collections.singleton(topicPartition)),
            conn,
            conn.getTelemetryClient(),
            new SnowflakeSchemaEvolutionService(conn),
            new InsertErrorMapper());

    // close the partition and open the partition to mimic migration
    service.close(Collections.singletonList(topicPartition));

    Map<String, String> topic2Table = new HashMap<>();
    topic2Table.put(topic, testTableName);
    service.startPartitions(Collections.singletonList(topicPartition), topic2Table);

    // this instance has changed since we removed it from cache and loaded it again.
    TopicPartitionChannel topicPartitionChannelAfterCloseAndStartPartition =
        ((SnowflakeSinkServiceV2) service)
            .getTopicPartitionChannelFromCacheKey(testChannelName)
            .get();

    // Fetch offsetToken from API should now give you same as other channel
    TestUtils.assertWithRetry(
        () ->
            topicPartitionChannelAfterCloseAndStartPartition.fetchOffsetTokenWithRetry()
                == NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE,
        5,
        5);

    recordsPartition1 =
        TestUtils.createJsonStringSinkRecords(
            recordsInPartition1, recordsInPartition1, topic, PARTITION);

    records = new ArrayList<>(recordsPartition1);

    service.insert(records);

    TestUtils.assertWithRetry(
        () ->
            service.getOffset(new TopicPartition(topic, PARTITION))
                == recordsInPartition1 + recordsInPartition1,
        5,
        5);

    service.closeAll();
  }

  @Test
  public void testInsertRowsWithGaps_schematization() throws Exception {
    testInsertRowsWithGaps(true);
  }

  @Test
  public void testInsertRowsWithGaps_nonSchematization() throws Exception {
    testInsertRowsWithGaps(false);
  }

  private void testInsertRowsWithGaps(boolean withSchematization) throws Exception {
    // setup
    Map<String, String> config = getConfForStreaming();
    SnowflakeSinkConnectorConfig.setDefaultValues(config);
    config.put(
        SnowflakeSinkConnectorConfig.ENABLE_SCHEMATIZATION_CONFIG,
        Boolean.toString(withSchematization));

    // create tpChannel
    SnowflakeSinkService service =
        SnowflakeSinkServiceFactory.builder(conn, IngestionMethodConfig.SNOWPIPE_STREAMING, config)
            .setErrorReporter(new InMemoryKafkaRecordErrorReporter())
            .setSinkTaskContext(new InMemorySinkTaskContext(Collections.singleton(topicPartition)))
            .addTask(testTableName, topicPartition)
            .build();

    // insert blank records that do not evolve schema: 0, 1
    List<SinkRecord> blankRecords = TestUtils.createBlankJsonSinkRecords(0, 2, topic, PARTITION);

    // Insert another two records with offset gap that requires evolution: 300, 301
    List<SinkRecord> gapRecords = TestUtils.createNativeJsonSinkRecords(300, 2, topic, PARTITION);

    List<SinkRecord> mergedList = new ArrayList<>(blankRecords);
    mergedList.addAll(gapRecords);
    // mergedList' offsets  -> [0, 1, 300, 301]
    service.insert(mergedList);
    // With schematization, we need to resend a new batch should succeed even if there is an offset
    // gap from the previous committed offset
    if (withSchematization) {
      service.insert(mergedList);
    }

    TestUtils.assertWithRetry(
        () -> service.getOffset(new TopicPartition(topic, PARTITION)) == 302, 20, 5);

    assert TestUtils.tableSize(testTableName) == 4
        : "expected: " + 4 + " actual: " + TestUtils.tableSize(testTableName);
    service.closeAll();
  }

  private Map<String, String> getConfForStreaming() {
    return TestUtils.getConfForStreaming();
  }
}
