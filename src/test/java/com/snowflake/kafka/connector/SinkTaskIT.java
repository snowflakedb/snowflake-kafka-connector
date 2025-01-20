package com.snowflake.kafka.connector;

import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.BUFFER_COUNT_RECORDS_DEFAULT;
import static com.snowflake.kafka.connector.internal.TestUtils.TEST_CONNECTOR_NAME;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.snowflake.kafka.connector.internal.KCLogger;
import com.snowflake.kafka.connector.internal.SnowflakeConnectionService;
import com.snowflake.kafka.connector.internal.TestUtils;
import com.snowflake.kafka.connector.records.SnowflakeJsonSchema;
import com.snowflake.kafka.connector.records.SnowflakeRecordContent;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
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
import org.mockito.Spy;
import org.slf4j.Logger;

public class SinkTaskIT {
  private String topicName;
  private SnowflakeConnectionService snowflakeConnectionService;
  private static int partition = 0;

  @Mock Logger logger = Mockito.mock(Logger.class);

  @InjectMocks @Spy
  private KCLogger kcLogger = Mockito.spy(new KCLogger(this.getClass().getName()));

  @InjectMocks private SnowflakeSinkTask task1 = new SnowflakeSinkTask();

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

    // commit offset should skip when offset=0
    Map<TopicPartition, OffsetAndMetadata> offsetMap = new HashMap<>();
    offsetMap.put(topicPartitions.get(0), new OffsetAndMetadata(0));
    offsetMap = sinkTask.preCommit(offsetMap);
    assert offsetMap.size() == 0;

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
  }

  @Test
  public void testMultipleSinkTasksWithLogs() throws Exception {
    // setup log mocking for task1
    MockitoAnnotations.initMocks(this);
    Mockito.when(logger.isInfoEnabled()).thenReturn(true);
    Mockito.when(logger.isDebugEnabled()).thenReturn(true);
    Mockito.when(logger.isWarnEnabled()).thenReturn(true);

    // setup tasks
    String task0Id = "0";
    Map<String, String> task0Config = TestUtils.getConf();
    SnowflakeSinkConnectorConfig.setDefaultValues(task0Config);
    task0Config.put(Utils.TASK_ID, task0Id);
    SnowflakeSinkTask task0 = new SnowflakeSinkTask();

    String task1Id = "1";
    Map<String, String> task1Config = TestUtils.getConf();
    SnowflakeSinkConnectorConfig.setDefaultValues(task1Config);
    task1Config.put(Utils.TASK_ID, task1Id);

    // start tasks
    task0.start(task0Config);
    task1.start(task1Config);

    // verify task1 start logs
    Mockito.verify(logger, Mockito.times(2)).info(Mockito.contains("start"));

    // open tasks
    ArrayList<TopicPartition> topicPartitions0 = new ArrayList<>();
    topicPartitions0.add(new TopicPartition(topicName, partition));
    task0.open(topicPartitions0);

    ArrayList<TopicPartition> topicPartitions1 = new ArrayList<>();
    topicPartitions1.add(new TopicPartition(topicName, partition));
    task1.open(topicPartitions1);

    // verify task1 open logs
    Mockito.verify(logger, Mockito.times(1)).info(Mockito.contains("open"));

    // put regular data to tasks
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

    task0.put(records);
    task1.put(records);

    // verify task1 put logs
    Mockito.verify(logger, Mockito.times(2)).debug(Mockito.contains("PUT"));

    // send broken data to task1
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
    task1.put(records);

    // verify task1 broken put logs, 4 bc in addition to last call
    Mockito.verify(logger, Mockito.times(4)).debug(Mockito.contains("PUT"));

    // commit offset
    Map<TopicPartition, OffsetAndMetadata> offsetMap0 = new HashMap<>();
    offsetMap0.put(topicPartitions0.get(0), new OffsetAndMetadata(0));
    offsetMap0 = task0.preCommit(offsetMap0);

    Map<TopicPartition, OffsetAndMetadata> offsetMap1 = new HashMap<>();
    offsetMap1.put(topicPartitions1.get(0), new OffsetAndMetadata(0));
    offsetMap1 = task1.preCommit(offsetMap1);

    // verify task1 precommit logs
    Mockito.verify(logger, Mockito.times(1)).info(Mockito.contains("PRECOMMIT"));

    // close tasks
    task0.close(topicPartitions0);
    task1.close(topicPartitions1);

    // verify task1 close logs
    Mockito.verify(logger, Mockito.times(1)).info(Mockito.contains("closed"));
    // stop tasks
    task0.stop();
    task1.stop();

    // verify task1 stop logs
    Mockito.verify(logger, Mockito.times(1)).info(Mockito.contains("stop"));

    assert offsetMap1.get(topicPartitions0.get(0)).offset() == BUFFER_COUNT_RECORDS_DEFAULT;
    assert offsetMap0.get(topicPartitions1.get(0)).offset() == BUFFER_COUNT_RECORDS_DEFAULT;
  }

  @Test
  public void testTopicToTableRegex() {
    Map<String, String> config = TestUtils.getConf();
    SnowflakeSinkConnectorConfig.setDefaultValues(config);

    SnowflakeSinkTaskForStreamingIT.testTopicToTableRegexMain(config);
  }
}
