package com.snowflake.kafka.connector;

import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.BUFFER_COUNT_RECORDS_DEFAULT;
import static com.snowflake.kafka.connector.internal.TestUtils.TEST_CONNECTOR_NAME;

import com.snowflake.kafka.connector.internal.SnowflakeConnectionService;
import com.snowflake.kafka.connector.internal.TestUtils;
import com.snowflake.kafka.connector.records.SnowflakeJsonSchema;
import com.snowflake.kafka.connector.records.SnowflakeRecordContent;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

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
}
