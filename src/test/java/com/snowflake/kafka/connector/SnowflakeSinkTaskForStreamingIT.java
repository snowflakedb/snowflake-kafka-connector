package com.snowflake.kafka.connector;

import static com.snowflake.kafka.connector.internal.TestUtils.getConnectionServiceWithEncryptedKey;
import static java.lang.String.format;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams;
import com.snowflake.kafka.connector.internal.SnowflakeConnectionService;
import com.snowflake.kafka.connector.internal.SnowflakeErrors;
import com.snowflake.kafka.connector.internal.SnowflakeSinkService;
import com.snowflake.kafka.connector.internal.TestUtils;
import com.snowflake.kafka.connector.internal.streaming.InMemorySinkTaskContext;
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
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * Sink Task IT test which uses {@link
 * com.snowflake.kafka.connector.internal.streaming.SnowflakeSinkServiceV2}
 */
public class SnowflakeSinkTaskForStreamingIT {

  private String topicName;
  private static final int partition = 0;
  private TopicPartition topicPartition;

  @BeforeEach
  public void beforeEach() {
    topicName = TestUtils.randomTableName();
    topicPartition = new TopicPartition(topicName, partition);
    getConnectionServiceWithEncryptedKey()
        .executeQueryWithParameters(
            format("create or replace table %s (record_metadata variant, f1 varchar)", topicName));
  }

  @AfterEach
  public void afterEach() {
    TestUtils.dropTable(topicName);
  }

  @Test
  public void testSinkTask() throws Exception {
    Map<String, String> config = getConfig();
    ConnectorConfigTools.setDefaultValues(config);

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
    TestUtils.assertWithRetry(() -> sinkTask.preCommit(offsetMap).size() == 0, 5, 20);

    // send regular data
    List<SinkRecord> records = TestUtils.createJsonStringSinkRecords(0, 1, topicName, partition);
    sinkTask.put(records);

    // commit offset
    offsetMap.clear();
    offsetMap.put(topicPartitions.get(0), new OffsetAndMetadata(10000));

    TestUtils.assertWithRetry(() -> sinkTask.preCommit(offsetMap).size() == 1, 5, 20);

    TestUtils.assertWithRetry(
        () -> sinkTask.preCommit(offsetMap).get(topicPartitions.get(0)).offset() == 1, 5, 20);

    sinkTask.close(topicPartitions);
    sinkTask.stop();
  }

  @Test
  public void testSinkTaskWithMultipleOpenClose() throws Exception {
    Map<String, String> config = getConfig();
    ConnectorConfigTools.setDefaultValues(config);

    SnowflakeSinkTask sinkTask = new SnowflakeSinkTask();
    // Inits the sinktaskcontext
    sinkTask.initialize(new InMemorySinkTaskContext(Collections.singleton(topicPartition)));

    sinkTask.start(config);
    ArrayList<TopicPartition> topicPartitions = new ArrayList<>();
    topicPartitions.add(new TopicPartition(topicName, partition));
    sinkTask.open(topicPartitions);

    final long noOfRecords = 1L;
    final long lastOffsetNo = noOfRecords - 1;

    // send regular data
    List<SinkRecord> records =
        TestUtils.createJsonStringSinkRecords(0, noOfRecords, topicName, partition);
    sinkTask.put(records);

    // commit offset
    final Map<TopicPartition, OffsetAndMetadata> offsetMap = new HashMap<>();
    offsetMap.put(topicPartitions.get(0), new OffsetAndMetadata(lastOffsetNo));

    TestUtils.assertWithRetry(() -> sinkTask.preCommit(offsetMap).size() == 1, 5, 20);

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

    TestUtils.assertWithRetry(() -> sinkTask.preCommit(offsetMap).size() == 2, 5, 20);

    TestUtils.assertWithRetry(
        () -> sinkTask.preCommit(offsetMap).get(topicPartitions.get(0)).offset() == 1, 5, 20);

    TestUtils.assertWithRetry(
        () -> sinkTask.preCommit(offsetMap).get(topicPartitions.get(1)).offset() == 1, 5, 20);

    sinkTask.close(topicPartitions);

    sinkTask.stop();

    ResultSet resultSet = TestUtils.showTable(topicName);
    LinkedList<String> contentResult = new LinkedList<>();
    LinkedList<String> metadataResult = new LinkedList<>();

    while (resultSet.next()) {
      contentResult.add(resultSet.getString("F1"));
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

  @Test
  public void testTopicToTableRegex() {
    Map<String, String> config = getConfig();

    testTopicToTableRegexMain(config);
  }

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
    connectorBaseConfig.put(
        KafkaConnectorConfigParams.SNOWFLAKE_TOPICS2TABLE_MAP, topic2tableRegex);

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
    Mockito.verify(serviceSpy, Mockito.times(1)).startPartitions(Mockito.anyCollection());

    for (String topicStr : expectedTopic2TableConfig.keySet()) {
      TopicPartition topic = null;
      for (TopicPartition currTp : testPartitions) {
        if (currTp.topic().equals(topicStr)) {
          topic = currTp;
        }
      }
      Assertions.assertNotNull(topic, "Expected topic partition was not opened by the tast");
    }
  }

  @Test
  public void testQuotedCaseSensitiveTableNames() throws Exception {
    // Use a unique quoted table name for this test
    String quotedTableName = "\"Test-Quoted-Table-" + System.currentTimeMillis() + "\"";
    TopicPartition tp = new TopicPartition(topicName, partition);

    Map<String, String> config = getConfig();
    ConnectorConfigTools.setDefaultValues(config);
    config.put(
        KafkaConnectorConfigParams.SNOWFLAKE_TOPICS2TABLE_MAP, topicName + ":" + quotedTableName);

    SnowflakeSinkTask sinkTask = new SnowflakeSinkTask();
    sinkTask.initialize(new InMemorySinkTaskContext(Collections.singleton(tp)));
    sinkTask.start(config);

    ArrayList<TopicPartition> topicPartitions = new ArrayList<>();
    topicPartitions.add(tp);
    sinkTask.open(topicPartitions);

    // Send records
    List<SinkRecord> records = TestUtils.createJsonStringSinkRecords(0, 3, topicName, partition);
    sinkTask.put(records);

    // Verify offset committed (data was ingested)
    final Map<TopicPartition, OffsetAndMetadata> offsetMap = new HashMap<>();
    offsetMap.put(tp, new OffsetAndMetadata(10000));
    TestUtils.assertWithRetry(() -> sinkTask.preCommit(offsetMap).size() == 1, 5, 20);

    // Verify the table was created with the exact quoted name
    // Note: tableExist() uses DESC TABLE identifier(?) which handles quoted names correctly.
    // tableSize() uses SHOW TABLES LIKE which doesn't support quoted identifiers, so we use
    // showTable() (SELECT *) and count rows instead.
    Assertions.assertTrue(
        getConnectionServiceWithEncryptedKey().tableExist(quotedTableName),
        "Table should exist with quoted name: " + quotedTableName);
    TestUtils.assertWithRetry(
        () -> {
          ResultSet rs = TestUtils.showTable(quotedTableName);
          int count = 0;
          while (rs.next()) count++;
          rs.close();
          return count == 3;
        },
        5,
        20);

    sinkTask.close(topicPartitions);
    sinkTask.stop();

    // Cleanup the quoted table
    TestUtils.dropTable(quotedTableName);
  }

  @Test
  public void testAutoGeneratedQuotedTableNames() throws Exception {
    // Use a topic name with dashes that would need quoting
    String topicWithDashes = "test-auto-quoted-" + System.currentTimeMillis();
    TopicPartition tp = new TopicPartition(topicWithDashes, partition);

    Map<String, String> config = getConfig();
    ConnectorConfigTools.setDefaultValues(config);
    // Enable quoted identifiers flag
    config.put(
        KafkaConnectorConfigParams.SNOWFLAKE_ENABLE_QUOTED_IDENTIFIERS_FOR_AUTOGENERATED, "true");
    // Override topics to use our topic-with-dashes
    config.put(KafkaConnectorConfigParams.TOPICS, topicWithDashes);

    SnowflakeSinkTask sinkTask = new SnowflakeSinkTask();
    sinkTask.initialize(new InMemorySinkTaskContext(Collections.singleton(tp)));
    sinkTask.start(config);

    ArrayList<TopicPartition> topicPartitions = new ArrayList<>();
    topicPartitions.add(tp);
    sinkTask.open(topicPartitions);

    // Send records
    List<SinkRecord> records =
        TestUtils.createJsonStringSinkRecords(0, 3, topicWithDashes, partition);
    sinkTask.put(records);

    // Verify offset committed (data was ingested)
    final Map<TopicPartition, OffsetAndMetadata> offsetMap = new HashMap<>();
    offsetMap.put(tp, new OffsetAndMetadata(10000));
    TestUtils.assertWithRetry(() -> sinkTask.preCommit(offsetMap).size() == 1, 5, 20);

    // Verify the table was created with the auto-generated quoted name (not sanitized)
    String expectedTableName = "\"" + topicWithDashes + "\"";
    Assertions.assertTrue(
        getConnectionServiceWithEncryptedKey().tableExist(expectedTableName),
        "Table should exist with quoted name: " + expectedTableName);
    TestUtils.assertWithRetry(
        () -> {
          ResultSet rs = TestUtils.showTable(expectedTableName);
          int count = 0;
          while (rs.next()) count++;
          rs.close();
          return count == 3;
        },
        5,
        20);

    sinkTask.close(topicPartitions);
    sinkTask.stop();

    // Cleanup the auto-generated quoted table
    TestUtils.dropTable(expectedTableName);
  }

  private Map<String, String> getConfig() {

    return TestUtils.getConnectorConfigurationForStreaming(false);
  }
}
