package com.snowflake.kafka.connector;

import static com.snowflake.kafka.connector.internal.streaming.channel.TopicPartitionChannel.NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

import com.snowflake.kafka.connector.internal.SnowflakeConnectionService;
import com.snowflake.kafka.connector.internal.TestUtils;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SinkTaskIT {
  private static final int PARTITION = 0;
  private static final int RECORD_COUNT = 10000;

  private String topicName;
  private SnowflakeConnectionService snowflakeConnectionService;

  @BeforeEach
  public void setup() {
    topicName = TestUtils.randomTableName();
    snowflakeConnectionService = TestUtils.getConnectionService();
    snowflakeConnectionService.createTableWithMetadataColumn(topicName);
  }

  @AfterEach
  public void after() {
    TestUtils.dropTable(topicName);
  }

  @Test
  public void testPreCommit() {
    SnowflakeSinkTask sinkTask = new SnowflakeSinkTask();
    Map<TopicPartition, OffsetAndMetadata> offsetMap = new HashMap<>();

    sinkTask.preCommit(offsetMap);
  }

  @Test
  public void testSinkTask() throws Exception {
    Map<String, String> config = TestUtils.transformProfileFileToConnectorConfiguration(true);
    ConnectorConfigTools.setDefaultValues(config);
    config.put(Utils.TASK_ID, "0");
    SnowflakeSinkTask sinkTask = new SnowflakeSinkTask();

    sinkTask.start(config);
    ArrayList<TopicPartition> topicPartitions = new ArrayList<>();
    final TopicPartition topicPartition = new TopicPartition(topicName, PARTITION);
    topicPartitions.add(topicPartition);
    sinkTask.open(topicPartitions);

    // commit offset should skip when offset=0
    Map<TopicPartition, OffsetAndMetadata> offsetMap = new HashMap<>();
    offsetMap.put(topicPartitions.get(0), new OffsetAndMetadata(0));
    offsetMap = sinkTask.preCommit(offsetMap);
    assertThat(offsetMap).isEmpty();

    // send regular data
    List<SinkRecord> records = createSinkRecords(PARTITION, RECORD_COUNT);
    sinkTask.put(records);

    // commit offset
    offsetMap.put(topicPartitions.get(0), new OffsetAndMetadata(0));
    await()
        .atMost(30, TimeUnit.SECONDS)
        .until(
            () ->
                sinkTask.getSink().getOffset(topicPartition)
                    != NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE);

    offsetMap = sinkTask.preCommit(offsetMap);

    sinkTask.close(topicPartitions);
    sinkTask.stop();
    assertThat(offsetMap.get(topicPartitions.get(0)).offset()).isEqualTo(RECORD_COUNT);
  }

  @Test
  public void testSinkTaskNegative() throws Exception {
    Map<String, String> config = TestUtils.transformProfileFileToConnectorConfiguration(true);
    ConnectorConfigTools.setDefaultValues(config);
    config.put(Utils.TASK_ID, "0");
    SnowflakeSinkTask sinkTask = new SnowflakeSinkTask();

    sinkTask.start(config);
    sinkTask.start(config);
    assertThat(sinkTask.version()).isEqualTo(Utils.VERSION);
    ArrayList<TopicPartition> topicPartitions = new ArrayList<>();
    topicPartitions.add(new TopicPartition(topicName, PARTITION));
    // Test put and precommit without open

    // commit offset
    Map<TopicPartition, OffsetAndMetadata> offsetMap = new HashMap<>();
    offsetMap.put(topicPartitions.get(0), new OffsetAndMetadata(0));
    offsetMap = sinkTask.preCommit(offsetMap);

    sinkTask.close(topicPartitions);

    // send regular data
    List<SinkRecord> records = createSinkRecords(PARTITION, RECORD_COUNT);
    sinkTask.put(records);

    // commit offset
    sinkTask.preCommit(offsetMap);

    sinkTask.close(topicPartitions);
    sinkTask.stop();
  }

  /**
   * Tests that multiple sink tasks can concurrently process data for different partitions of the
   * same topic. Each task handles its own partition and should correctly track offsets.
   */
  @Test
  public void testMultipleSinkTasks() throws Exception {
    final int partition0 = 0;
    final int partition1 = 1;

    SnowflakeSinkTask task0 = new SnowflakeSinkTask();
    SnowflakeSinkTask task1 = new SnowflakeSinkTask();

    List<TopicPartition> topicPartitions0 = List.of(new TopicPartition(topicName, partition0));
    List<TopicPartition> topicPartitions1 = List.of(new TopicPartition(topicName, partition1));

    try {
      // Start both tasks
      Map<String, String> task0Config =
          TestUtils.transformProfileFileToConnectorConfiguration(false);
      ConnectorConfigTools.setDefaultValues(task0Config);
      task0Config.put(Utils.TASK_ID, "0");
      task0.start(task0Config);

      Map<String, String> task1Config =
          TestUtils.transformProfileFileToConnectorConfiguration(false);
      ConnectorConfigTools.setDefaultValues(task1Config);
      task1Config.put(Utils.TASK_ID, "1");
      task1.start(task1Config);

      // Open partitions
      task0.open(topicPartitions0);
      task1.open(topicPartitions1);

      // Put records to both tasks
      task0.put(createSinkRecords(partition0, RECORD_COUNT));
      task1.put(createSinkRecords(partition1, RECORD_COUNT));

      // Wait for offsets to be committed and verify
      TopicPartition tp0 = topicPartitions0.get(0);
      TopicPartition tp1 = topicPartitions1.get(0);

      await()
          .atMost(60, TimeUnit.SECONDS)
          .untilAsserted(
              () -> {
                Map<TopicPartition, OffsetAndMetadata> offsetMap0 =
                    task0.preCommit(Map.of(tp0, new OffsetAndMetadata(0)));
                assertThat(offsetMap0)
                    .containsKey(tp0)
                    .extractingByKey(tp0)
                    .satisfies(offset -> assertThat(offset.offset()).isEqualTo(RECORD_COUNT));
              });

      await()
          .atMost(60, TimeUnit.SECONDS)
          .untilAsserted(
              () -> {
                Map<TopicPartition, OffsetAndMetadata> offsetMap1 =
                    task1.preCommit(Map.of(tp1, new OffsetAndMetadata(0)));
                assertThat(offsetMap1)
                    .containsKey(tp1)
                    .extractingByKey(tp1)
                    .satisfies(offset -> assertThat(offset.offset()).isEqualTo(RECORD_COUNT));
              });
    } finally {
      // Always cleanup even if test fails
      task0.close(topicPartitions0);
      task1.close(topicPartitions1);
      task0.stop();
      task1.stop();
    }
  }

  @Test
  public void testTopicToTableRegex() {
    Map<String, String> config = TestUtils.transformProfileFileToConnectorConfiguration(false);
    ConnectorConfigTools.setDefaultValues(config);

    SnowflakeSinkTaskForStreamingIT.testTopicToTableRegexMain(config);
  }

  private List<SinkRecord> createSinkRecords(int partition, int count) {
    JsonConverter jsonConverter = new JsonConverter();
    jsonConverter.configure(Map.of("schemas.enable", "false"), false);
    String json = "{ \"f1\" : \"v1\" }";
    SchemaAndValue schemaAndValue =
        jsonConverter.toConnectData(topicName, json.getBytes(StandardCharsets.UTF_8));

    List<SinkRecord> records = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      records.add(
          new SinkRecord(
              topicName,
              partition,
              null,
              null,
              schemaAndValue.schema(),
              schemaAndValue.value(),
              i,
              System.currentTimeMillis(),
              TimestampType.CREATE_TIME));
    }
    return records;
  }
}
