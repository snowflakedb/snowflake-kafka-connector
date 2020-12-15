package com.snowflake.kafka.connector;

import com.snowflake.kafka.connector.internal.SnowflakeKafkaConnectorException;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class ConnectorConfigTest
{
  static Map<String, String> getConfig()
  {
    Map<String, String> config = new HashMap<>();
    config.put(SnowflakeSinkConnectorConfig.NAME, "test");
    config.put(SnowflakeSinkConnectorConfig.TOPICS, "topic1,topic2");
    config.put(SnowflakeSinkConnectorConfig.SNOWFLAKE_URL,
      "https://testaccount.snowflake.com:443");
    config.put(SnowflakeSinkConnectorConfig.SNOWFLAKE_USER, "userName");
    config.put(SnowflakeSinkConnectorConfig.SNOWFLAKE_PRIVATE_KEY,
      "fdsfsdfsdfdsfdsrqwrwewrwrew42314424");
    config.put(SnowflakeSinkConnectorConfig.SNOWFLAKE_SCHEMA,"testSchema");
    config.put(SnowflakeSinkConnectorConfig.SNOWFLAKE_DATABASE, "testDatabase");
    config.put(SnowflakeSinkConnectorConfig.BUFFER_COUNT_RECORDS,
      SnowflakeSinkConnectorConfig.BUFFER_COUNT_RECORDS_DEFAULT + "");
    config.put(SnowflakeSinkConnectorConfig.BUFFER_SIZE_BYTES,
      SnowflakeSinkConnectorConfig.BUFFER_SIZE_BYTES_DEFAULT + "");
    config.put(SnowflakeSinkConnectorConfig.BUFFER_FLUSH_TIME_SEC,
      SnowflakeSinkConnectorConfig.BUFFER_FLUSH_TIME_SEC_DEFAULT + "");
    return config;
  }


  @Test
  public void testConfig()
  {
    Map<String, String> config = getConfig();
    Utils.validateConfig(config);
  }

  @Test(expected = SnowflakeKafkaConnectorException.class)
  public void testEmptyFlushTime(){
    Map<String, String> config = getConfig();
    config.remove(SnowflakeSinkConnectorConfig.BUFFER_FLUSH_TIME_SEC);
    Utils.validateConfig(config);
  }

  @Test(expected = SnowflakeKafkaConnectorException.class)
  public void testFlushTimeSmall(){
    Map<String, String> config = getConfig();
    config.put(SnowflakeSinkConnectorConfig.BUFFER_FLUSH_TIME_SEC,
      (SnowflakeSinkConnectorConfig.BUFFER_FLUSH_TIME_SEC_MIN - 1) + "");
    Utils.validateConfig(config);
  }

  @Test(expected = SnowflakeKafkaConnectorException.class)
  public void testFlushTimeNotNumber(){
    Map<String, String> config = getConfig();
    config.put(SnowflakeSinkConnectorConfig.BUFFER_FLUSH_TIME_SEC, "fdas");
    Utils.validateConfig(config);
  }

  @Test(expected = SnowflakeKafkaConnectorException.class)
  public void testEmptyName()
  {
    Map<String, String> config = getConfig();
    config.remove(SnowflakeSinkConnectorConfig.NAME);
    Utils.validateConfig(config);
  }

  @Test(expected = SnowflakeKafkaConnectorException.class)
  public void testURL()
  {
    Map<String, String> config = getConfig();
    config.remove(SnowflakeSinkConnectorConfig.SNOWFLAKE_URL);
    Utils.validateConfig(config);
  }

  @Test(expected = SnowflakeKafkaConnectorException.class)
  public void testEmptyUser()
  {
    Map<String, String> config = getConfig();
    config.remove(SnowflakeSinkConnectorConfig.SNOWFLAKE_USER);
    Utils.validateConfig(config);
  }

  @Test(expected = SnowflakeKafkaConnectorException.class)
  public void testEmptyDatabase()
  {
    Map<String, String> config = getConfig();
    config.remove(SnowflakeSinkConnectorConfig.SNOWFLAKE_DATABASE);
    Utils.validateConfig(config);
  }

  @Test(expected = SnowflakeKafkaConnectorException.class)
  public void testEmptySchema()
  {
    Map<String, String> config = getConfig();
    config.remove(SnowflakeSinkConnectorConfig.SNOWFLAKE_SCHEMA);
    Utils.validateConfig(config);
  }

  @Test(expected = SnowflakeKafkaConnectorException.class)
  public void testEmptyPrivateKey()
  {
    Map<String, String> config = getConfig();
    config.remove(SnowflakeSinkConnectorConfig.SNOWFLAKE_PRIVATE_KEY);
    Utils.validateConfig(config);
  }

  @Test
  public void testCorrectProxyHost()
  {
    Map<String, String> config = getConfig();
    config.put(SnowflakeSinkConnectorConfig.JVM_PROXY_HOST, "127.0.0.1");
    config.put(SnowflakeSinkConnectorConfig.JVM_PROXY_PORT, "3128");
    Utils.validateConfig(config);
  }

  @Test(expected = SnowflakeKafkaConnectorException.class)
  public void testEmptyPort()
  {
    Map<String, String> config = getConfig();
    config.put(SnowflakeSinkConnectorConfig.JVM_PROXY_HOST, "127.0.0.1");
    Utils.validateConfig(config);
  }

  @Test(expected = SnowflakeKafkaConnectorException.class)
  public void testEmptyHost()
  {
    Map<String, String> config = getConfig();
    config.put(SnowflakeSinkConnectorConfig.JVM_PROXY_PORT, "3128");
    Utils.validateConfig(config);
  }

  @Test(expected = SnowflakeKafkaConnectorException.class)
  public void testIllegalTopicMap()
  {
    Map<String, String> config = getConfig();
    config.put(SnowflakeSinkConnectorConfig.TOPICS_TABLES_MAP,"$@#$#@%^$12312");
    Utils.validateConfig(config);
  }

  @Test(expected = SnowflakeKafkaConnectorException.class)
  public void testIllegalTableName()
  {
    Map<String, String> config = getConfig();
    config.put(SnowflakeSinkConnectorConfig.TOPICS_TABLES_MAP,"topic1:!@#@!#!@");
    Utils.validateConfig(config);
  }

  @Test(expected = SnowflakeKafkaConnectorException.class)
  public void testDuplicatedTopic()
  {
    Map<String, String> config = getConfig();
    config.put(SnowflakeSinkConnectorConfig.TOPICS_TABLES_MAP,"topic1:table1,topic1:table2");
    Utils.validateConfig(config);
  }

  @Test(expected = SnowflakeKafkaConnectorException.class)
  public void testDuplicatedTableName()
  {
    Map<String, String> config = getConfig();
    config.put(SnowflakeSinkConnectorConfig.TOPICS_TABLES_MAP,"topic1:table1,topic2:table1");
    Utils.validateConfig(config);
  }

  @Test
  public void testNameMapCovered()
  {
    Map<String, String> config = getConfig();
    config.put(SnowflakeSinkConnectorConfig.TOPICS,"!@#,$%^,test");
    config.put(SnowflakeSinkConnectorConfig.TOPICS_TABLES_MAP,"!@#:table1,$%^:table2");
    Utils.validateConfig(config);
  }

  @Test(expected = SnowflakeKafkaConnectorException.class)
  public void testBufferSizeRange()
  {
    Map<String, String> config = getConfig();
    config.put(SnowflakeSinkConnectorConfig.BUFFER_SIZE_BYTES,
      SnowflakeSinkConnectorConfig.BUFFER_SIZE_BYTES_MIN - 1 + "");
    Utils.validateConfig(config);
  }

  @Test(expected = SnowflakeKafkaConnectorException.class)
  public void testBufferSizeValue()
  {
    Map<String, String> config = getConfig();
    config.put(SnowflakeSinkConnectorConfig.BUFFER_SIZE_BYTES, "afdsa");
    Utils.validateConfig(config);
  }

  @Test(expected = SnowflakeKafkaConnectorException.class)
  public void testEmptyBufferSize()
  {
    Map<String, String> config = getConfig();
    config.remove(SnowflakeSinkConnectorConfig.BUFFER_SIZE_BYTES);
    Utils.validateConfig(config);
  }

  @Test(expected = SnowflakeKafkaConnectorException.class)
  public void testEmptyBufferCount()
  {
    Map<String, String> config = getConfig();
    config.remove(SnowflakeSinkConnectorConfig.BUFFER_COUNT_RECORDS);
    Utils.validateConfig(config);
  }

  @Test(expected = SnowflakeKafkaConnectorException.class)
  public void testEmptyBufferCountNegative()
  {
    Map<String, String> config = getConfig();
    config.put(SnowflakeSinkConnectorConfig.BUFFER_COUNT_RECORDS, "-1");
    Utils.validateConfig(config);
  }

  @Test(expected = SnowflakeKafkaConnectorException.class)
  public void testBufferCountValue()
  {
    Map<String, String> config = getConfig();
    config.put(SnowflakeSinkConnectorConfig.BUFFER_COUNT_RECORDS, "adssadsa");
    Utils.validateConfig(config);
  }

}
