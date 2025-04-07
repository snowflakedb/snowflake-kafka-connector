package com.snowflake.kafka.connector;

import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.BUFFER_COUNT_RECORDS;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.INGESTION_METHOD_OPT;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.snowflake.kafka.connector.internal.KCLogger;
import com.snowflake.kafka.connector.internal.SnowflakeConnectionService;
import com.snowflake.kafka.connector.internal.SnowflakeErrors;
import com.snowflake.kafka.connector.internal.SnowflakeSinkService;
import com.snowflake.kafka.connector.internal.TestUtils;
import com.snowflake.kafka.connector.internal.streaming.InMemorySinkTaskContext;
import com.snowflake.kafka.connector.internal.streaming.IngestionMethodConfig;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.Spy;
import org.slf4j.Logger;

/**
 * Sink Task IT test which uses {@link
 * com.snowflake.kafka.connector.internal.streaming.SnowflakeSinkServiceV2}
 */
public class SnowflakeSinkTaskForStreamingIT {

  private String topicName;
  private static int partition = 0;
  private TopicPartition topicPartition;

  @Mock Logger logger = Mockito.mock(Logger.class);

  @InjectMocks @Spy
  private KCLogger kcLogger = Mockito.spy(new KCLogger(this.getClass().getName()));

  @InjectMocks private SnowflakeSinkTask sinkTask1 = new SnowflakeSinkTask();

  @BeforeEach
  public void beforeEach() {
    topicName = TestUtils.randomTableName();
    topicPartition = new TopicPartition(topicName, partition);
  }

  @AfterEach
  public void afterEach() {
    TestUtils.dropTable(topicName);
  }

  private static Stream<Arguments> oAuthAndSingleBufferParameters() {
    return Stream.of(Arguments.of(false, false), Arguments.of(false, true));
    // OAuth tests are temporary disabled
    // return TestUtils.nBooleanProduct(2);
  }

  @ParameterizedTest(name = "useOAuth: {0}")
  @ValueSource(booleans = {true, false})
  public void testSinkTask(boolean useOAuth) throws Exception {
    Map<String, String> config = getConfig(useOAuth);
    SnowflakeSinkConnectorConfig.setDefaultValues(config);
    config.put(BUFFER_COUNT_RECORDS, "1"); // override

    config.put(INGESTION_METHOD_OPT, IngestionMethodConfig.SNOWPIPE_STREAMING.toString());

    SnowflakeSinkTask sinkTask = new SnowflakeSinkTask();

    // Inits the sinktaskcontext
    sinkTask.initialize(new InMemorySinkTaskContext(Collections.singleton(topicPartition)));
    sinkTask.start(config);
    ArrayList<TopicPartition> topicPartitions = new ArrayList<>();
    topicPartitions.add(new TopicPartition(topicName, partition));
    sinkTask.open(topicPartitions);

    // commit offset
    final Map<TopicPartition, OffsetAndMetadata> offsetMap = new HashMap<>();
    offsetMap.put(topicPartitions.get(0), new OffsetAndMetadata(0));
    TestUtils.assertWithRetry(() -> sinkTask.preCommit(offsetMap).size() == 0, 20, 5);

    // send regular data
    List<SinkRecord> records = TestUtils.createJsonStringSinkRecords(0, 1, topicName, partition);
    sinkTask.put(records);

    // commit offset
    offsetMap.clear();
    offsetMap.put(topicPartitions.get(0), new OffsetAndMetadata(10000));

    TestUtils.assertWithRetry(() -> sinkTask.preCommit(offsetMap).size() == 1, 20, 5);

    TestUtils.assertWithRetry(
        () -> sinkTask.preCommit(offsetMap).get(topicPartitions.get(0)).offset() == 1, 20, 5);

    sinkTask.close(topicPartitions);
    sinkTask.stop();
  }

  @ParameterizedTest(name = "useOAuth: {0}")
  @ValueSource(booleans = {true, false})
  public void testSinkTaskWithMultipleOpenClose(boolean useOAuth) throws Exception {
    Map<String, String> config = getConfig(useOAuth);
    SnowflakeSinkConnectorConfig.setDefaultValues(config);
    config.put(BUFFER_COUNT_RECORDS, "1"); // override

    config.put(INGESTION_METHOD_OPT, IngestionMethodConfig.SNOWPIPE_STREAMING.toString());

    SnowflakeSinkTask sinkTask = new SnowflakeSinkTask();
    // Inits the sinktaskcontext
    sinkTask.initialize(new InMemorySinkTaskContext(Collections.singleton(topicPartition)));

    sinkTask.start(config);
    ArrayList<TopicPartition> topicPartitions = new ArrayList<>();
    topicPartitions.add(new TopicPartition(topicName, partition));
    sinkTask.open(topicPartitions);

    final long noOfRecords = 1l;
    final long lastOffsetNo = noOfRecords - 1;

    // send regular data
    List<SinkRecord> records =
        TestUtils.createJsonStringSinkRecords(0, noOfRecords, topicName, partition);
    sinkTask.put(records);

    // commit offset
    final Map<TopicPartition, OffsetAndMetadata> offsetMap = new HashMap<>();
    offsetMap.put(topicPartitions.get(0), new OffsetAndMetadata(lastOffsetNo));

    TestUtils.assertWithRetry(() -> sinkTask.preCommit(offsetMap).size() == 1, 20, 5);

    // precommit is one more than offset last inserted
    TestUtils.assertWithRetry(
        () -> sinkTask.preCommit(offsetMap).get(topicPartitions.get(0)).offset() == noOfRecords,
        20,
        5);

    sinkTask.close(topicPartitions);

    // Add one more partition
    topicPartitions.add(new TopicPartition(topicName, partition + 1));

    sinkTask.open(topicPartitions);

    // trying to put same records
    sinkTask.put(records);

    List<SinkRecord> recordsWithAnotherPartition =
        TestUtils.createJsonStringSinkRecords(0, noOfRecords, topicName, partition + 1);
    sinkTask.put(recordsWithAnotherPartition);

    // Adding to offsetMap so that this gets into precommit
    offsetMap.put(topicPartitions.get(1), new OffsetAndMetadata(lastOffsetNo));

    TestUtils.assertWithRetry(() -> sinkTask.preCommit(offsetMap).size() == 2, 20, 5);

    TestUtils.assertWithRetry(
        () -> sinkTask.preCommit(offsetMap).get(topicPartitions.get(0)).offset() == 1, 20, 5);

    TestUtils.assertWithRetry(
        () -> sinkTask.preCommit(offsetMap).get(topicPartitions.get(1)).offset() == 1, 20, 5);

    sinkTask.close(topicPartitions);

    sinkTask.stop();

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
            Assertions.fail();
          }
        });

    assert partitionsInTable.size() == 2;
  }

  @ParameterizedTest(name = "useOAuth: {0}")
  @ValueSource(booleans = {true, false})
  public void testTopicToTableRegex(boolean useOAuth) {
    Map<String, String> config = getConfig(useOAuth);

    testTopicToTableRegexMain(config);
  }

  // runner for topic to table regex testing, used to test both streaming and snowpipe scenarios.
  // Unfortunately cannot be moved to test utils due to the scope of some static variables
  public static void testTopicToTableRegexMain(Map<String, String> config) {
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
    List<String> twoRegexPartitionStrs =
        Arrays.asList(bigCatTopicStr1, bigCatTopicStr2, dogTopicStr1, birdTopicStr1);
    Map<String, String> twoRegexExpected = new HashMap<>();
    twoRegexExpected.put(bigCatTopicStr1, bigCatTable);
    twoRegexExpected.put(bigCatTopicStr2, bigCatTable);
    twoRegexExpected.put(dogTopicStr1, dogTable);
    twoRegexExpected.put(birdTopicStr1, birdTopicStr1);
    testTopicToTableRegexRunner(config, twoRegexConfig, twoRegexPartitionStrs, twoRegexExpected);

    // test two regexes with catchall. catchall should overlap both regexes and fail the test
    String twoRegexCatchAllConfig =
        Utils.formatString(
            "{}:{}, {}:{},{}:{}",
            catchAllRegex,
            catchallTable,
            catTopicRegex,
            catTable,
            dogTopicRegex,
            dogTable);
    List<String> twoRegexCatchAllPartitionStrs =
        Arrays.asList(catTopicStr1, catTopicStr2, bigCatTopicStr1, dogTopicStr1, birdTopicStr1);
    Map<String, String> twoRegexCatchAllExpected = new HashMap<>();
    assert TestUtils.assertError(
        SnowflakeErrors.ERROR_0021,
        () ->
            testTopicToTableRegexRunner(
                config,
                twoRegexCatchAllConfig,
                twoRegexCatchAllPartitionStrs,
                twoRegexCatchAllExpected));

    // test invalid overlapping regexes
    String invalidTwoRegexConfig =
        Utils.formatString("{}:{}, {}:{}", catTopicRegex, catTable, bigCatTopicRegex, bigCatTable);
    List<String> invalidTwoRegexPartitionStrs =
        Arrays.asList(catTopicStr1, catTopicStr2, dogTopicStr1, birdTopicStr1);
    Map<String, String> invalidTwoRegexExpected = new HashMap<>();
    assert TestUtils.assertError(
        SnowflakeErrors.ERROR_0021,
        () ->
            testTopicToTableRegexRunner(
                config,
                invalidTwoRegexConfig,
                invalidTwoRegexPartitionStrs,
                invalidTwoRegexExpected));

    // test catchall regex
    String catchAllConfig = Utils.formatString("{}:{}", catchAllRegex, catchallTable);
    List<String> catchAllPartitionStrs =
        Arrays.asList(catTopicStr1, catTopicStr2, dogTopicStr1, birdTopicStr1);
    Map<String, String> catchAllExpected = new HashMap<>();
    catchAllExpected.put(catTopicStr1, catchallTable);
    catchAllExpected.put(catTopicStr2, catchallTable);
    catchAllExpected.put(dogTopicStr1, catchallTable);
    catchAllExpected.put(birdTopicStr1, catchallTable);
    testTopicToTableRegexRunner(config, catchAllConfig, catchAllPartitionStrs, catchAllExpected);
  }

  private static void testTopicToTableRegexRunner(
      Map<String, String> connectorBaseConfig,
      String topic2tableRegex,
      List<String> partitionStrList,
      Map<String, String> expectedTopic2TableConfig) {
    // setup
    connectorBaseConfig.put(SnowflakeSinkConnectorConfig.TOPICS_TABLES_MAP, topic2tableRegex);

    // setup partitions
    List<TopicPartition> testPartitions = new ArrayList<>();
    for (int i = 0; i < partitionStrList.size(); i++) {
      testPartitions.add(new TopicPartition(partitionStrList.get(i), i));
    }

    // mocks
    SnowflakeSinkService serviceSpy = Mockito.spy(SnowflakeSinkService.class);
    SnowflakeConnectionService connSpy = Mockito.spy(SnowflakeConnectionService.class);
    Map<String, String> parsedConfig = SnowflakeSinkTask.getTopicToTableMap(connectorBaseConfig);

    SnowflakeSinkTask sinkTask = new SnowflakeSinkTask(serviceSpy, connSpy, parsedConfig);

    // test topics were mapped correctly
    sinkTask.open(testPartitions);

    // verify expected num tasks opened
    Mockito.verify(serviceSpy, Mockito.times(1))
        .startPartitions(Mockito.anyCollection(), Mockito.anyMap());

    for (String topicStr : expectedTopic2TableConfig.keySet()) {
      TopicPartition topic = null;
      String table = expectedTopic2TableConfig.get(topicStr);
      for (TopicPartition currTp : testPartitions) {
        if (currTp.topic().equals(topicStr)) {
          topic = currTp;
        }
      }
      Assertions.assertNotNull(topic, "Expected topic partition was not opened by the tast");
    }
  }

  private Map<String, String> getConfig(boolean useOAuth) {
    if (!useOAuth) {
      return TestUtils.getConfForStreaming();
    } else {
      return TestUtils.getConfForStreamingWithOAuth();
    }
  }
}
