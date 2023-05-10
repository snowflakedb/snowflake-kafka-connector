package com.snowflake.kafka.connector;

import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.BUFFER_COUNT_RECORDS_DEFAULT;
import static com.snowflake.kafka.connector.internal.TestUtils.TEST_CONNECTOR_NAME;

import com.snowflake.kafka.connector.internal.KCLogger;
import com.snowflake.kafka.connector.internal.SnowflakeConnectionService;
import com.snowflake.kafka.connector.internal.SnowflakeErrors;
import com.snowflake.kafka.connector.internal.SnowflakeSinkService;
import com.snowflake.kafka.connector.internal.TestUtils;
import com.snowflake.kafka.connector.records.SnowflakeJsonSchema;
import com.snowflake.kafka.connector.records.SnowflakeRecordContent;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.AdditionalMatchers;
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
    int task1OpenCount = 0;
    Map<String, String> task1Config = TestUtils.getConf();
    SnowflakeSinkConnectorConfig.setDefaultValues(task1Config);
    task1Config.put(Utils.TASK_ID, task1Id);

    // set up task1 logging tag
    String expectedTask1Tag =
        TestUtils.getExpectedLogTagWithoutCreationCount(task1Id, task1OpenCount);

    // start tasks
    task0.start(task0Config);
    task1.start(task1Config);

    // verify task1 start logs
    Mockito.verify(kcLogger, Mockito.times(1))
        .setLoggerInstanceTag(Mockito.contains(expectedTask1Tag));
    Mockito.verify(logger, Mockito.times(2))
        .debug(
            AdditionalMatchers.and(Mockito.contains(expectedTask1Tag), Mockito.contains("start")));

    // open tasks
    ArrayList<TopicPartition> topicPartitions0 = new ArrayList<>();
    topicPartitions0.add(new TopicPartition(topicName, partition));
    task0.open(topicPartitions0);

    ArrayList<TopicPartition> topicPartitions1 = new ArrayList<>();
    topicPartitions1.add(new TopicPartition(topicName, partition));
    task1.open(topicPartitions1);
    task1OpenCount++;
    expectedTask1Tag = TestUtils.getExpectedLogTagWithoutCreationCount(task1Id, task1OpenCount);

    // verify task1 open logs
    Mockito.verify(logger, Mockito.times(1))
        .debug(
            AdditionalMatchers.and(Mockito.contains(expectedTask1Tag), Mockito.contains("open")));

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
    Mockito.verify(logger, Mockito.times(1))
        .debug(AdditionalMatchers.and(Mockito.contains(expectedTask1Tag), Mockito.contains("put")));

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
    Mockito.verify(logger, Mockito.times(2))
        .debug(AdditionalMatchers.and(Mockito.contains(expectedTask1Tag), Mockito.contains("put")));

    // commit offset
    Map<TopicPartition, OffsetAndMetadata> offsetMap0 = new HashMap<>();
    offsetMap0.put(topicPartitions0.get(0), new OffsetAndMetadata(0));
    offsetMap0 = task0.preCommit(offsetMap0);

    Map<TopicPartition, OffsetAndMetadata> offsetMap1 = new HashMap<>();
    offsetMap1.put(topicPartitions1.get(0), new OffsetAndMetadata(0));
    offsetMap1 = task1.preCommit(offsetMap1);

    // verify task1 precommit logs
    Mockito.verify(logger, Mockito.times(1))
        .debug(
            AdditionalMatchers.and(
                Mockito.contains(expectedTask1Tag), Mockito.contains("precommit")));

    // close tasks
    task0.close(topicPartitions0);
    task1.close(topicPartitions1);

    // verify task1 close logs
    Mockito.verify(logger, Mockito.times(1))
        .debug(
            AdditionalMatchers.and(Mockito.contains(expectedTask1Tag), Mockito.contains("closed")));
    // stop tasks
    task0.stop();
    task1.stop();

    // verify task1 stop logs
    Mockito.verify(logger, Mockito.times(1))
        .debug(
            AdditionalMatchers.and(Mockito.contains(expectedTask1Tag), Mockito.contains("stop")));

    assert offsetMap1.get(topicPartitions0.get(0)).offset() == BUFFER_COUNT_RECORDS_DEFAULT;
    assert offsetMap0.get(topicPartitions1.get(0)).offset() == BUFFER_COUNT_RECORDS_DEFAULT;
  }

  @Test
  public void testTopicToTableMapParseAndCreation() {
    // constants
    String catTable = "cat_table";
    String catTopicRegex = ".*_cat";
    String catTopicStr1 = "calico_cat";
    String catTopicStr2 = "orange_cat";

    String bigCatTable = "big_cat_table";
    String bigCatTopicRegex = "big.*_.*_cat";
    String bigCatTopicStr1 = "big_calico_cat";
    String bigCatTopicStr2 = "biggest_orange_cat";

    String dogTable = "dog_table";
    String dogTopicRegex = ".*_dog";
    String dogTopicStr1 = "corgi_dog";

    String catchallTable = "animal_table";
    String catchAllRegex = ".*";
    String birdTopicStr1 = "bird";

    // test two regexes. bird should create its own table
    String twoRegexConfig =
        Utils.formatString("{}:{}, {}:{}", bigCatTopicRegex, bigCatTable, dogTopicRegex, dogTable);
    List<String> twoRegexPartitionStrs = Arrays.asList(bigCatTopicStr1, bigCatTopicStr2, dogTopicStr1, birdTopicStr1);
    Map<String, String> twoRegexExpected = new HashMap<>();
    twoRegexExpected.put(bigCatTopicStr1, bigCatTable);
    twoRegexExpected.put(bigCatTopicStr2, bigCatTable);
    twoRegexExpected.put(dogTopicStr1, dogTable);
    twoRegexExpected.put(birdTopicStr1, birdTopicStr1);
    this.topicToTableRunner(twoRegexConfig, twoRegexPartitionStrs, twoRegexExpected);

    // test two regexes with catchall. bird should point to catchall table
    String twoRegexCatchAllConfig =
        Utils.formatString("{}:{}, {}:{},{}:{}", catTopicRegex, catTable, dogTopicRegex, dogTable, catchAllRegex, catchallTable);
    List<String> twoRegexCatchAllPartitionStrs = Arrays.asList(catTopicStr1, catTopicStr2, dogTopicStr1, birdTopicStr1);
    Map<String, String> twoRegexCatchAllExpected = new HashMap<>();
    twoRegexCatchAllExpected.put(catTopicStr1, catTable);
    twoRegexCatchAllExpected.put(catTopicStr2, catTable);
    twoRegexCatchAllExpected.put(dogTopicStr1, dogTable);
    twoRegexCatchAllExpected.put(birdTopicStr1, catchallTable);
    this.topicToTableRunner(twoRegexCatchAllConfig, twoRegexCatchAllPartitionStrs, twoRegexCatchAllExpected);

    // test invalid overlapping regexes
    String invalidTwoRegexConfig =
        Utils.formatString("{}:{}, {}:{}", catTopicRegex, catTable, bigCatTopicRegex, bigCatTable);
    List<String> invalidTwoRegexPartitionStrs = Arrays.asList(catTopicStr1, catTopicStr2, dogTopicStr1, birdTopicStr1);
    Map<String, String> invalidTwoRegexExpected = new HashMap<>();
    assert TestUtils.assertError(SnowflakeErrors.ERROR_0021, () -> this.topicToTableRunner(invalidTwoRegexConfig, invalidTwoRegexPartitionStrs, invalidTwoRegexExpected));

    // test catchall regex
    String catchAllConfig = Utils.formatString("{}:{}", catchAllRegex, catchallTable);
    List<String> catchAllPartitionStrs = Arrays.asList(catTopicStr1, catTopicStr2, dogTopicStr1, birdTopicStr1);
    Map<String, String> catchAllExpected = new HashMap<>();
    catchAllExpected.put(catTopicStr1, catchallTable);
    catchAllExpected.put(catTopicStr2, catchallTable);
    catchAllExpected.put(dogTopicStr1, catchallTable);
    catchAllExpected.put(birdTopicStr1, catchallTable);
    this.topicToTableRunner(catchAllConfig, catchAllPartitionStrs, catchAllExpected);
  }

  private void topicToTableRunner(String topic2tableRegex, List<String> partitionStrList, Map<String, String> expectedTopic2TableConfig) {
    // setup
    Map<String, String> config = TestUtils.getConf();
    SnowflakeSinkConnectorConfig.setDefaultValues(config);
    config.put(SnowflakeSinkConnectorConfig.TOPICS_TABLES_MAP, topic2tableRegex);

    // setup partitions
    List<TopicPartition> testPartitions = new ArrayList<>();
    for (int i = 0; i < partitionStrList.size(); i++) {
      testPartitions.add(new TopicPartition(partitionStrList.get(i), i));
    }

    // mocks
    SnowflakeSinkService serviceSpy = Mockito.spy(SnowflakeSinkService.class);
    SnowflakeConnectionService connSpy = Mockito.spy(SnowflakeConnectionService.class);
    Map<String, String> parsedConfig = SnowflakeSinkTask.getTopicToTableMap(config);

    SnowflakeSinkTask sinkTask = new SnowflakeSinkTask(serviceSpy, connSpy, parsedConfig);

    // test topics were mapped correctly
    sinkTask.open(testPartitions);

    // verify expected num tasks opened
    Mockito.verify(serviceSpy, Mockito.times(expectedTopic2TableConfig.size()))
        .startTask(Mockito.anyString(), Mockito.any(TopicPartition.class));

    for (String topicStr : expectedTopic2TableConfig.keySet()) {
      TopicPartition topic = null;
      String table = expectedTopic2TableConfig.get(topicStr);
      for (TopicPartition currTp : testPartitions) {
        if (currTp.topic().equals(topicStr)) {
          topic = currTp;
          Mockito.verify(serviceSpy, Mockito.times(1)).startTask(table, topic);
        }
      }
      Assert.assertNotNull("Expected topic partition was not opened by the tast", topic);
    }
  }
}
