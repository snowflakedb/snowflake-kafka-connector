package com.snowflake.kafka.connector;

import com.snowflake.kafka.connector.internal.LoggerHandler;
import com.snowflake.kafka.connector.internal.SnowflakeConnectionService;
import com.snowflake.kafka.connector.internal.TestUtils;
import com.snowflake.kafka.connector.records.SnowflakeJsonSchema;
import com.snowflake.kafka.connector.records.SnowflakeRecordContent;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.BUFFER_COUNT_RECORDS_DEFAULT;
import static com.snowflake.kafka.connector.internal.TestUtils.TEST_CONNECTOR_NAME;

public class SinkTaskIT {
  private String topicName;
  private SnowflakeConnectionService snowflakeConnectionService;
  private static int partition = 0;

  @Before
  public void setup() {
    topicName = TestUtils.randomTableName();

    snowflakeConnectionService = TestUtils.getConnectionService();
  }

  @After
  public void after() {
    TestUtils.dropTable(topicName);
    snowflakeConnectionService.dropStage(Utils.stageName(TEST_CONNECTOR_NAME, topicName));
    snowflakeConnectionService.dropPipe(Utils.pipeName(TEST_CONNECTOR_NAME, topicName, partition));
  }

  @Test
  public void testPreCommit() {
    SnowflakeSinkTask sinkTask = new SnowflakeSinkTask();
    Map<TopicPartition, OffsetAndMetadata> offsetMap = new HashMap<>();

    sinkTask.preCommit(offsetMap);
    System.out.println("PreCommit test success");
  }

  @Test
  public void testSinkTask() throws Exception {
    Map<String, String> config = TestUtils.getConf();
    SnowflakeSinkConnectorConfig.setDefaultValues(config);
    SnowflakeSinkTask sinkTask = new SnowflakeSinkTask();

    sinkTask.start(config);
    ArrayList<TopicPartition> topicPartitions = new ArrayList<>();
    topicPartitions.add(new TopicPartition(topicName, partition));
    sinkTask.open(topicPartitions);

    // send regular data
    ArrayList<SinkRecord> records = new ArrayList<>();
    String json = "{ \"f1\" : \"v1\" } ";
    ObjectMapper objectMapper = new ObjectMapper();
    Schema snowflakeSchema = new SnowflakeJsonSchema();
    SnowflakeRecordContent content = new SnowflakeRecordContent(objectMapper.readTree(json));
    for (int i = 0; i < BUFFER_COUNT_RECORDS_DEFAULT; ++i) {
      records.add(
          new SinkRecord(
              topicName,
              partition,
              snowflakeSchema,
              content,
              snowflakeSchema,
              content,
              i,
              System.currentTimeMillis(),
              TimestampType.CREATE_TIME));
    }
    sinkTask.put(records);

    // send broken data
    String brokenJson = "{ broken json";
    records = new ArrayList<>();
    content = new SnowflakeRecordContent(brokenJson.getBytes());
    records.add(
        new SinkRecord(
            topicName,
            partition,
            snowflakeSchema,
            content,
            snowflakeSchema,
            content,
            10000,
            System.currentTimeMillis(),
            TimestampType.CREATE_TIME));
    sinkTask.put(records);

    // commit offset
    Map<TopicPartition, OffsetAndMetadata> offsetMap = new HashMap<>();
    offsetMap.put(topicPartitions.get(0), new OffsetAndMetadata(0));
    offsetMap = sinkTask.preCommit(offsetMap);

    sinkTask.close(topicPartitions);
    sinkTask.stop();
    assert offsetMap.get(topicPartitions.get(0)).offset() == BUFFER_COUNT_RECORDS_DEFAULT;
  }

  @Test
  public void testSinkTaskNegative() throws Exception {
    Map<String, String> config = TestUtils.getConf();
    SnowflakeSinkConnectorConfig.setDefaultValues(config);
    SnowflakeSinkTask sinkTask = new SnowflakeSinkTask();

    sinkTask.start(config);
    sinkTask.start(config);
    assert sinkTask.version() == Utils.VERSION;
    ArrayList<TopicPartition> topicPartitions = new ArrayList<>();
    topicPartitions.add(new TopicPartition(topicName, partition));
    // Test put and precommit without open

    // commit offset
    Map<TopicPartition, OffsetAndMetadata> offsetMap = new HashMap<>();
    offsetMap.put(topicPartitions.get(0), new OffsetAndMetadata(0));
    offsetMap = sinkTask.preCommit(offsetMap);

    sinkTask.close(topicPartitions);

    // send regular data
    ArrayList<SinkRecord> records = new ArrayList<>();
    String json = "{ \"f1\" : \"v1\" } ";
    ObjectMapper objectMapper = new ObjectMapper();
    Schema snowflakeSchema = new SnowflakeJsonSchema();
    SnowflakeRecordContent content = new SnowflakeRecordContent(objectMapper.readTree(json));
    for (int i = 0; i < BUFFER_COUNT_RECORDS_DEFAULT; ++i) {
      records.add(
          new SinkRecord(
              topicName,
              partition,
              snowflakeSchema,
              content,
              snowflakeSchema,
              content,
              i,
              System.currentTimeMillis(),
              TimestampType.CREATE_TIME));
    }
    sinkTask.put(records);

    // send broken data
    String brokenJson = "{ broken json";
    records = new ArrayList<>();
    content = new SnowflakeRecordContent(brokenJson.getBytes());
    records.add(
        new SinkRecord(
            topicName,
            partition,
            snowflakeSchema,
            content,
            snowflakeSchema,
            content,
            10000,
            System.currentTimeMillis(),
            TimestampType.CREATE_TIME));
    sinkTask.put(records);

    // commit offset
    sinkTask.preCommit(offsetMap);

    sinkTask.close(topicPartitions);
    sinkTask.stop();

    sinkTask.logWarningForPutAndPrecommit(System.currentTimeMillis() - 400 * 1000, 1, "put");
  }

  @Mock(name="DYNAMIC_LOGGER")
  LoggerHandler loggerHandler = Mockito.mock(LoggerHandler.class);
  @InjectMocks private SnowflakeSinkTask sinkTask1 = new SnowflakeSinkTask();
  @InjectMocks private SnowflakeSinkTask sinkTask2 = new SnowflakeSinkTask();

  @Test
  public void testMultipleSinkTasks() throws Exception {

    Map<String, String> config = TestUtils.getConf();
    SnowflakeSinkConnectorConfig.setDefaultValues(config);

    MockitoAnnotations.initMocks(this);

    String logTag1 = "logTag1";
    String logStart1 = "task1 started";
    Mockito.doCallRealMethod().when(loggerHandler).info(logStart1);
    sinkTask1.start(config);
    Mockito.verify(loggerHandler, Mockito.times(1)).setLoggerInstanceIdTag(logTag1);
    Mockito.verify(loggerHandler, Mockito.times(1)).info(logStart1);

    ArrayList<TopicPartition> topicPartitions1 = new ArrayList<>();
    topicPartitions1.add(new TopicPartition(topicName, partition));
    sinkTask1.open(topicPartitions1);

    sinkTask2.start(config);
    ArrayList<TopicPartition> topicPartitions2 = new ArrayList<>();
    topicPartitions2.add(new TopicPartition(topicName, partition));
    sinkTask2.open(topicPartitions2);

    // send regular data
    ArrayList<SinkRecord> records = new ArrayList<>();
    String json = "{ \"f1\" : \"v1\" } ";
    ObjectMapper objectMapper = new ObjectMapper();
    Schema snowflakeSchema = new SnowflakeJsonSchema();
    SnowflakeRecordContent content = new SnowflakeRecordContent(objectMapper.readTree(json));
    for (int i = 0; i < BUFFER_COUNT_RECORDS_DEFAULT; ++i) {
      records.add(
        new SinkRecord(
          topicName,
          partition,
          snowflakeSchema,
          content,
          snowflakeSchema,
          content,
          i,
          System.currentTimeMillis(),
          TimestampType.CREATE_TIME));
    }

    sinkTask1.put(records);
    sinkTask2.put(records);

    // send broken data
    String brokenJson = "{ broken json";
    records = new ArrayList<>();
    content = new SnowflakeRecordContent(brokenJson.getBytes());
    records.add(
      new SinkRecord(
        topicName,
        partition,
        snowflakeSchema,
        content,
        snowflakeSchema,
        content,
        10000,
        System.currentTimeMillis(),
        TimestampType.CREATE_TIME));
    sinkTask1.put(records);

    // commit offset
    Map<TopicPartition, OffsetAndMetadata> offsetMap1 = new HashMap<>();
    offsetMap1.put(topicPartitions1.get(0), new OffsetAndMetadata(0));
    offsetMap1 = sinkTask1.preCommit(offsetMap1);

    Map<TopicPartition, OffsetAndMetadata> offsetMap2 = new HashMap<>();
    offsetMap2.put(topicPartitions1.get(0), new OffsetAndMetadata(0));
    offsetMap2 = sinkTask1.preCommit(offsetMap2);

    sinkTask1.close(topicPartitions1);
    sinkTask1.stop();

    sinkTask2.close(topicPartitions2);
    sinkTask2.stop();
    assert offsetMap1.get(topicPartitions1.get(0)).offset() == BUFFER_COUNT_RECORDS_DEFAULT;
    assert offsetMap2.get(topicPartitions2.get(0)).offset() == BUFFER_COUNT_RECORDS_DEFAULT;
  }
}
