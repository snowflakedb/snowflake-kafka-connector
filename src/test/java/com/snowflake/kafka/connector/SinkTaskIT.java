package com.snowflake.kafka.connector;

import com.snowflake.kafka.connector.internal.SnowflakeConnectionService;
import com.snowflake.kafka.connector.internal.SnowflakeKafkaConnectorException;
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
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.BUFFER_COUNT_RECORDS_DEFAULT;
import static com.snowflake.kafka.connector.internal.TestUtils.TEST_CONNECTOR_NAME;

public class SinkTaskIT {
  private String topicName = TestUtils.randomTableName();
  private SnowflakeConnectionService conn = TestUtils.getConnectionService();
  private static int partition = 0;

  @Test
  public void testPreCommit()
  {
    SnowflakeSinkTask sinkTask = new SnowflakeSinkTask();
    Map<TopicPartition, OffsetAndMetadata> offsetMap = new HashMap<>();

    sinkTask.preCommit(offsetMap);
    System.out.println("PreCommit test success");
  }

  @Test(expected = SnowflakeKafkaConnectorException.class)
  public void testSinkTaskProxyConfigMock()
  {
    Map<String, String> config = TestUtils.getConf();
    SnowflakeSinkConnectorConfig.setDefaultValues(config);

    config.put(SnowflakeSinkConnectorConfig.JVM_PROXY_HOST, "wronghost");
    config.put(SnowflakeSinkConnectorConfig.JVM_PROXY_PORT, "wrongport");
    config.put(SnowflakeSinkConnectorConfig.JVM_PROXY_USERNAME, "user");
    config.put(SnowflakeSinkConnectorConfig.JVM_PROXY_PASSWORD, "password");
    SnowflakeSinkTask sinkTask = new SnowflakeSinkTask();
    try
    {
      sinkTask.start(config);
    } catch (SnowflakeKafkaConnectorException e)
    {
      assert System.getProperty(Utils.HTTP_USE_PROXY).equals("true");
      assert System.getProperty(Utils.HTTP_PROXY_HOST).equals("wronghost");
      assert System.getProperty(Utils.HTTP_PROXY_PORT).equals("wrongport");
      assert System.getProperty(Utils.HTTPS_PROXY_HOST).equals("wronghost");
      assert System.getProperty(Utils.HTTPS_PROXY_PORT).equals("wrongport");
      assert System.getProperty(Utils.JDK_HTTP_AUTH_TUNNELING).isEmpty();
      assert System.getProperty(Utils.HTTP_PROXY_USER).equals("user");
      assert System.getProperty(Utils.HTTP_PROXY_PASSWORD).equals("password");
      assert System.getProperty(Utils.HTTPS_PROXY_USER).equals("user");
      assert System.getProperty(Utils.HTTPS_PROXY_PASSWORD).equals("password");

      System.setProperty(Utils.HTTP_USE_PROXY, "");
      System.setProperty(Utils.HTTP_PROXY_HOST, "");
      System.setProperty(Utils.HTTP_PROXY_PORT, "");
      System.setProperty(Utils.HTTPS_PROXY_HOST, "");
      System.setProperty(Utils.HTTPS_PROXY_PORT, "");
      System.setProperty(Utils.JDK_HTTP_AUTH_TUNNELING, "");
      System.setProperty(Utils.HTTP_PROXY_USER, "");
      System.setProperty(Utils.HTTP_PROXY_PASSWORD, "");
      System.setProperty(Utils.HTTPS_PROXY_USER, "");
      System.setProperty(Utils.HTTPS_PROXY_PASSWORD, "");
      throw e;
    }
  }

  @Test
  public void testSinkTask() throws Exception
  {
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
    for (int i = 0 ; i < BUFFER_COUNT_RECORDS_DEFAULT; ++i)
    {
      records.add(new SinkRecord(topicName, partition, snowflakeSchema, content,
        snowflakeSchema, content, i, System.currentTimeMillis(), TimestampType.CREATE_TIME));
    }
    sinkTask.put(records);

    // send broken data
    String brokenJson = "{ broken json";
    records = new ArrayList<>();
    content = new SnowflakeRecordContent(brokenJson.getBytes());
    records.add(new SinkRecord(topicName, partition, snowflakeSchema, content,
      snowflakeSchema, content, 10000, System.currentTimeMillis(), TimestampType.CREATE_TIME));
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
  public void testSinkTaskNegative() throws Exception
  {
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
    for (int i = 0 ; i < BUFFER_COUNT_RECORDS_DEFAULT; ++i)
    {
      records.add(new SinkRecord(topicName, partition, snowflakeSchema, content,
        snowflakeSchema, content, i, System.currentTimeMillis(), TimestampType.CREATE_TIME));
    }
    sinkTask.put(records);

    // send broken data
    String brokenJson = "{ broken json";
    records = new ArrayList<>();
    content = new SnowflakeRecordContent(brokenJson.getBytes());
    records.add(new SinkRecord(topicName, partition, snowflakeSchema, content,
      snowflakeSchema, content, 10000, System.currentTimeMillis(), TimestampType.CREATE_TIME));
    sinkTask.put(records);

    // commit offset
    sinkTask.preCommit(offsetMap);

    sinkTask.close(topicPartitions);
    sinkTask.stop();

    sinkTask.logWarningForPutAndPrecommit(System.currentTimeMillis() - 400 * 1000, 1, "put");
  }

  @After
  public void after()
  {
    TestUtils.dropTable(topicName);
    conn.dropStage(Utils.stageName(TEST_CONNECTOR_NAME, topicName));
    conn.dropPipe(Utils.pipeName(TEST_CONNECTOR_NAME, topicName, partition));
  }

  /**
   * This test is skipped because it is manually tested. To run this test, spin up a http/https proxy
   * at 127.0.0.1:3128 and set authentication as required. 
   */
  @Ignore
  public void testSinkTaskProxyConfig()
  {
    Map<String, String> config = TestUtils.getConf();
    SnowflakeSinkConnectorConfig.setDefaultValues(config);

    config.put(SnowflakeSinkConnectorConfig.JVM_PROXY_HOST, "127.0.0.1");
    config.put(SnowflakeSinkConnectorConfig.JVM_PROXY_PORT, "3128");
    config.put(SnowflakeSinkConnectorConfig.JVM_PROXY_USERNAME, "test");
    config.put(SnowflakeSinkConnectorConfig.JVM_PROXY_PASSWORD, "kafkaTestPassword");
    SnowflakeSinkTask sinkTask = new SnowflakeSinkTask();

    sinkTask.start(config);

    assert System.getProperty(Utils.HTTP_USE_PROXY).equals("true");
    assert System.getProperty(Utils.HTTP_PROXY_HOST).equals("127.0.0.1");
    assert System.getProperty(Utils.HTTP_PROXY_PORT).equals("3128");
    assert System.getProperty(Utils.HTTPS_PROXY_HOST).equals("127.0.0.1");
    assert System.getProperty(Utils.HTTPS_PROXY_PORT).equals("3128");
    assert System.getProperty(Utils.JDK_HTTP_AUTH_TUNNELING).isEmpty();
    assert System.getProperty(Utils.HTTP_PROXY_USER).equals("test");
    assert System.getProperty(Utils.HTTP_PROXY_PASSWORD).equals("kafkaTestPassword");
    assert System.getProperty(Utils.HTTPS_PROXY_USER).equals("test");
    assert System.getProperty(Utils.HTTPS_PROXY_PASSWORD).equals("kafkaTestPassword");
  }
}