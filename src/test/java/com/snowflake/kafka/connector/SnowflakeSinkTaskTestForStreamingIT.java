package com.snowflake.kafka.connector;

import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.BUFFER_COUNT_RECORDS;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.INGESTION_METHOD_OPT;

import com.snowflake.kafka.connector.internal.TestUtils;
import com.snowflake.kafka.connector.internal.ingestsdk.IngestSdkProvider;
import com.snowflake.kafka.connector.internal.streaming.InMemorySinkTaskContext;
import com.snowflake.kafka.connector.internal.streaming.IngestionMethodConfig;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.core.JsonProcessingException;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.JsonNode;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Sink Task IT test which uses {@link
 * com.snowflake.kafka.connector.internal.streaming.SnowflakeSinkServiceV2}
 */
public class SnowflakeSinkTaskTestForStreamingIT {
  private int taskId;
  private int partitionCount;
  private String topicName;
  private Map<String, String> config;
  private List<TopicPartition> topicPartitions;
  private SnowflakeSinkTask sinkTask;
  private InMemorySinkTaskContext sinkTaskContext;
  private List<SinkRecord> records;
  private Map<TopicPartition, OffsetAndMetadata> offsetMap;

  // sets up default objects for normal testing
  // NOTE: everything defaults to having one of each (ex: # topic partitions, # tasks, # clients,
  // etc)
  @Before
  public void setup() throws Exception {
    this.taskId = 0;
    this.partitionCount = 1;
    this.topicName = "topicName";
    this.config = this.getConfig(this.taskId);
    this.topicPartitions = getTopicPartitions(this.topicName, this.partitionCount);
    this.sinkTask = new SnowflakeSinkTask();
    this.sinkTaskContext =
        new InMemorySinkTaskContext(Collections.singleton(this.topicPartitions.get(0)));
    this.records = TestUtils.createJsonStringSinkRecords(0, 1, this.topicName, 0);
    this.offsetMap = new HashMap<>();
    this.offsetMap.put(this.topicPartitions.get(0), new OffsetAndMetadata(10000));

    IngestSdkProvider.getStreamingClientManager()
        .createAllStreamingClients(this.config, "testkcid", 1, 1);
    assert IngestSdkProvider.getStreamingClientManager().getClientCount() == 1;
  }

  @After
  public void after() throws Exception {
    this.sinkTask.close(this.topicPartitions);
    this.sinkTask.stop();
    TestUtils.dropTable(topicName);
    IngestSdkProvider.setStreamingClientManager(
        TestUtils.resetAndGetEmptyStreamingClientManager()); // reset to clean initial manager
  }

  @Test
  public void testSinkTask() throws Exception {
    // Inits the sinktaskcontext
    this.sinkTask.initialize(this.sinkTaskContext);
    this.sinkTask.start(this.config);
    this.sinkTask.open(this.topicPartitions);

    // send regular data
    this.sinkTask.put(this.records);

    // commit offset
    TestUtils.assertWithRetry(() -> this.sinkTask.preCommit(this.offsetMap).size() == 1, 20, 5);

    // verify offset
    TestUtils.assertWithRetry(
        () ->
            this.sinkTask.preCommit(this.offsetMap).get(this.topicPartitions.get(0)).offset() == 1,
        20,
        5);

    // cleanup
    this.sinkTask.close(this.topicPartitions);
    this.sinkTask.stop();
  }

  // test two tasks map to one client behaves as expected
  @Test
  public void testTaskToClientMapping() throws Exception {
    // setup two tasks pointing to one client
    IngestSdkProvider.setStreamingClientManager(TestUtils.resetAndGetEmptyStreamingClientManager());
    IngestSdkProvider.getStreamingClientManager()
        .createAllStreamingClients(this.config, "kcid", 2, 2);
    assert IngestSdkProvider.getStreamingClientManager().getClientCount() == 1;

    // setup task0, not strictly necessary but makes test more readable
    Map<String, String> config0 = this.config;
    List<TopicPartition> topicPartitions0 = this.topicPartitions;
    SnowflakeSinkTask sinkTask0 = this.sinkTask;
    InMemorySinkTaskContext sinkTaskContext0 = this.sinkTaskContext;
    List<SinkRecord> records0 = this.records;
    Map<TopicPartition, OffsetAndMetadata> offsetMap0 = this.offsetMap;

    // setup task1
    int taskId1 = 1;
    String topicName1 = "topicName1";
    Map<String, String> config1 = this.getConfig(taskId1);
    List<TopicPartition> topicPartitions1 = getTopicPartitions(topicName1, 1);
    SnowflakeSinkTask sinkTask1 = new SnowflakeSinkTask();
    InMemorySinkTaskContext sinkTaskContext1 =
        new InMemorySinkTaskContext(Collections.singleton(topicPartitions1.get(0)));
    List<SinkRecord> records1 = TestUtils.createJsonStringSinkRecords(0, 1, topicName1, 0);
    Map<TopicPartition, OffsetAndMetadata> offsetMap1 = new HashMap<>();
    offsetMap1.put(topicPartitions1.get(0), new OffsetAndMetadata(10000));

    // start init and open tasks
    sinkTask0.initialize(sinkTaskContext0);
    sinkTask1.initialize(sinkTaskContext1);
    sinkTask0.start(config0);
    sinkTask1.start(config1);
    sinkTask0.open(topicPartitions0);
    sinkTask1.open(topicPartitions1);

    // send data to both tasks
    sinkTask0.put(records0);
    sinkTask1.put(records1);

    // verify that data was ingested
    TestUtils.assertWithRetry(() -> sinkTask0.preCommit(offsetMap0).size() == 1, 20, 5);
    TestUtils.assertWithRetry(() -> sinkTask1.preCommit(offsetMap1).size() == 1, 20, 5);

    TestUtils.assertWithRetry(
        () -> sinkTask0.preCommit(offsetMap0).get(topicPartitions0.get(0)).offset() == 1, 20, 5);
    TestUtils.assertWithRetry(
        () -> sinkTask1.preCommit(offsetMap1).get(topicPartitions1.get(0)).offset() == 1, 20, 5);

    // clean up tasks
    sinkTask0.close(topicPartitions0);
    sinkTask1.close(topicPartitions1);
    sinkTask0.stop();
    sinkTask1.stop();
  }

  @Test
  public void testSinkTaskWithMultipleOpenClose() throws Exception {
    final long noOfRecords = 1l;
    final long lastOffsetNo = noOfRecords - 1;

    this.sinkTask.initialize(this.sinkTaskContext);
    this.sinkTask.start(this.config);
    this.sinkTask.open(this.topicPartitions);

    List<SinkRecord> recordsPart0 = this.records;
    List<SinkRecord> recordsPart1 = TestUtils.createJsonStringSinkRecords(0, 1, this.topicName, 1);

    // send regular data to partition 0, verify data was committed
    this.sinkTask.put(recordsPart0);
    TestUtils.assertWithRetry(
        () -> this.sinkTask.preCommit(this.offsetMap).size() == noOfRecords, 20, 5);
    TestUtils.assertWithRetry(
        () ->
            this.sinkTask.preCommit(this.offsetMap).get(this.topicPartitions.get(0)).offset()
                == noOfRecords,
        20,
        5);

    this.sinkTask.close(this.topicPartitions);

    // Add one more partition and open last partition
    this.partitionCount++;
    this.topicPartitions = this.getTopicPartitions(this.topicName, this.partitionCount);
    this.sinkTask.open(this.topicPartitions);

    // trying to put records to partition 0 and 1
    this.sinkTask.put(recordsPart0);
    this.sinkTask.put(recordsPart1);

    // Adding to offsetMap so that this gets into precommit
    this.offsetMap.put(this.topicPartitions.get(1), new OffsetAndMetadata(lastOffsetNo));

    // verify precommit for task and each partition
    TestUtils.assertWithRetry(() -> this.sinkTask.preCommit(this.offsetMap).size() == 2, 20, 5);
    TestUtils.assertWithRetry(
        () ->
            this.sinkTask.preCommit(this.offsetMap).get(this.topicPartitions.get(0)).offset() == 1,
        20,
        5);
    TestUtils.assertWithRetry(
        () -> sinkTask.preCommit(offsetMap).get(topicPartitions.get(1)).offset() == 1, 20, 5);

    // clean up
    sinkTask.close(topicPartitions);
    sinkTask.stop();

    // verify content and metadata
    ResultSet resultSet = TestUtils.showTable(topicName);
    LinkedList<String> contentResult = new LinkedList<>();
    LinkedList<String> metadataResult = new LinkedList<>();

    while (resultSet.next()) {
      contentResult.add(resultSet.getString("RECORD_CONTENT"));
      metadataResult.add(resultSet.getString("RECORD_METADATA"));
    }
    resultSet.close();
    assert metadataResult.size() == 2;
    assert contentResult.size() == 2;
    ObjectMapper mapper = new ObjectMapper();

    Set<Long> partitionsInTable = new HashSet<>();
    metadataResult.forEach(
        s -> {
          try {
            JsonNode metadata = mapper.readTree(s);
            metadata.get("offset").asText().equals("0");
            partitionsInTable.add(metadata.get("partition").asLong());
          } catch (JsonProcessingException e) {
            Assert.fail();
          }
        });

    assert partitionsInTable.size() == 2;
  }

  public static Map<String, String> getConfig(int taskId) {
    Map<String, String> config = TestUtils.getConfForStreaming();
    config.put(BUFFER_COUNT_RECORDS, "1"); // override
    config.put(INGESTION_METHOD_OPT, IngestionMethodConfig.SNOWPIPE_STREAMING.toString());
    SnowflakeSinkConnectorConfig.setDefaultValues(config);
    config.put(Utils.TASK_ID, taskId + "");

    return config;
  }

  public static ArrayList<TopicPartition> getTopicPartitions(String topicName, int numPartitions) {
    ArrayList<TopicPartition> topicPartitions = new ArrayList<>();
    for (int i = 0; i < numPartitions; i++) {
      topicPartitions.add(new TopicPartition(topicName, i));
    }

    return topicPartitions;
  }
}
