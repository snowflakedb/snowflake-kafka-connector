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
    // Drop the associated streaming pipe to prevent account-level pipe limit errors
    TestUtils.dropPipe(topicName + "-STREAMING");
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

  private Map<String, String> getConfig() {

    return TestUtils.getConnectorConfigurationForStreaming(false);
  }

  @Test
  public void testSanitizationEnabledAutoGenerated() throws Exception {
    // Topic with valid identifier that needs uppercasing
    // Use uppercase letters to avoid hash generation
    String topicName = "TestTopic" + System.currentTimeMillis();
    TopicPartition topicPartition = new TopicPartition(topicName, 0);

    Map<String, String> config = TestUtils.getConnectorConfigurationForStreaming(false);
    config.put(KafkaConnectorConfigParams.SNOWFLAKE_ENABLE_TABLE_NAME_SANITIZATION, "true");
    config.put(KafkaConnectorConfigParams.TOPICS, topicName);

    SnowflakeSinkTask task = new SnowflakeSinkTask();
    task.start(config);

    // Create and send records
    List<SinkRecord> records = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      records.add(new SinkRecord(topicName, 0, null, null, null, "{\"f1\":\"value" + i + "\"}", i));
    }

    task.put(records);
    task.close(Collections.singleton(topicPartition));

    // Wait a bit for the table to be created
    Thread.sleep(2000);

    // When sanitization is enabled, valid identifiers are uppercased
    String expectedTableName = topicName.toUpperCase();

    SnowflakeConnectionService conn = getConnectionServiceWithEncryptedKey();

    // Check if the uppercased table exists
    boolean tableExists = conn.tableExist(expectedTableName);
    Assertions.assertTrue(tableExists, "Should find uppercased table: " + expectedTableName);

    // Verify the table name is fully uppercased
    Assertions.assertTrue(
        expectedTableName.matches("^[A-Z_0-9]+$"),
        "Table name should be fully uppercased with only alphanumeric and underscore characters");

    // Verify data
    ResultSet data = TestUtils.showTable(expectedTableName);
    int count = 0;
    while (data.next()) {
      count++;
    }
    Assertions.assertEquals(5, count, "Should have 5 rows");

    // Cleanup table and pipe
    String pipeName = expectedTableName + "-STREAMING";
    TestUtils.dropTable(expectedTableName);
    TestUtils.dropPipe(pipeName);
  }

  @Test
  public void testSanitizationDisabledQuotedMap() throws Exception {
    // Use a quoted identifier in topic2table map with sanitization disabled
    String topicName = "myTopic_" + System.currentTimeMillis();
    String quotedTableName = "\"My-Test-Table-" + System.currentTimeMillis() + "\"";
    TopicPartition topicPartition = new TopicPartition(topicName, 0);

    Map<String, String> config = TestUtils.getConnectorConfigurationForStreaming(false);
    config.put(KafkaConnectorConfigParams.SNOWFLAKE_ENABLE_TABLE_NAME_SANITIZATION, "false");
    config.put(
        KafkaConnectorConfigParams.SNOWFLAKE_TOPICS2TABLE_MAP, topicName + ":" + quotedTableName);
    config.put(KafkaConnectorConfigParams.TOPICS, topicName);

    // Manually create the table with quoted name
    // Use the quoted name directly in the SQL since identifier(?) doesn't work with special chars
    String tableNameWithoutQuotes = quotedTableName.substring(1, quotedTableName.length() - 1);
    String createTableSql =
        String.format(
            "create or replace table %s (record_metadata variant, f1 varchar)", quotedTableName);
    getConnectionServiceWithEncryptedKey().executeQueryWithParameters(createTableSql);

    SnowflakeSinkTask task = new SnowflakeSinkTask();
    task.start(config);

    // Create and send records
    List<SinkRecord> records = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      records.add(new SinkRecord(topicName, 0, null, null, null, "{\"f1\":\"value" + i + "\"}", i));
    }

    task.put(records);
    task.close(Collections.singleton(topicPartition));

    // Wait for data to be written
    Thread.sleep(2000);

    // Verify data - use quotedTableName directly since it already has quotes
    // This will fail if the table doesn't exist or has wrong case
    ResultSet data = TestUtils.showTable(quotedTableName);
    int count = 0;
    while (data.next()) {
      count++;
    }
    Assertions.assertEquals(
        5, count, "Should have 5 rows in case-sensitive table " + quotedTableName);

    // Cleanup table and pipe
    String pipeName = tableNameWithoutQuotes + "-STREAMING";
    TestUtils.dropTable(quotedTableName);
    TestUtils.dropPipe(pipeName);
  }
}
