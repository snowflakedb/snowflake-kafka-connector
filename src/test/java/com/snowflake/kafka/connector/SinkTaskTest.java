package com.snowflake.kafka.connector;

import com.snowflake.kafka.connector.internal.SnowflakeKafkaConnectorException;
import com.snowflake.kafka.connector.internal.TestUtils;
import com.snowflake.kafka.connector.records.SnowflakeMetadataConfig;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.Ignore;
import org.junit.Test;

import java.util.*;

public class SinkTaskTest {

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

    config.put(SnowflakeSinkConnectorConfig.JVM_PROXY_HOST, "127.0.0.1");
    config.put(SnowflakeSinkConnectorConfig.JVM_PROXY_PORT, "3128");
    config.put(SnowflakeSinkConnectorConfig.JVM_PROXY_USERNAME, "user");
    config.put(SnowflakeSinkConnectorConfig.JVM_PROXY_PASSWORD, "password");
    SnowflakeSinkTask sinkTask = new SnowflakeSinkTask();
    try
    {
      sinkTask.start(config);
    } catch (SnowflakeKafkaConnectorException e)
    {
      assert System.getProperty(Utils.HTTP_USE_PROXY).equals("true");
      assert System.getProperty(Utils.HTTP_PROXY_HOST).equals("127.0.0.1");
      assert System.getProperty(Utils.HTTP_PROXY_PORT).equals("3128");
      assert System.getProperty(Utils.HTTPS_PROXY_HOST).equals("127.0.0.1");
      assert System.getProperty(Utils.HTTPS_PROXY_PORT).equals("3128");
      assert System.getProperty(Utils.JDK_HTTP_AUTH_TUNNELING).isEmpty();
      assert System.getProperty(Utils.HTTP_PROXY_USER).equals("user");
      assert System.getProperty(Utils.HTTP_PROXY_PASSWORD).equals("password");
      assert System.getProperty(Utils.HTTPS_PROXY_USER).equals("user");
      assert System.getProperty(Utils.HTTPS_PROXY_PASSWORD).equals("password");
      throw e;
    }
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