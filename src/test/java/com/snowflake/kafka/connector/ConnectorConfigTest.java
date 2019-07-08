package com.snowflake.kafka.connector;

import com.snowflake.kafka.connector.internal.SnowflakeKafkaConnectorException;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class ConnectorConfigTest
{
  private static Map<String, String> getConfig()
  {
    Map<String, String> config = new HashMap<>();
    config.put(SnowflakeSinkConnectorConfig.NAME, "test");
    config.put(SnowflakeSinkConnectorConfig.TOPICS, "topic1,topic2");
    config.put(SnowflakeSinkConnectorConfig.SNOWFLAKE_URL, "https://testaccount.snowflake.com:443");
    config.put(SnowflakeSinkConnectorConfig.SNOWFLAKE_USER, "userName");
    config.put(SnowflakeSinkConnectorConfig.SNOWFLAKE_PRIVATE_KEY, "fdsfsdfsdfdsfdsrqwrwewrwrew42314424");
    config.put(SnowflakeSinkConnectorConfig.SNOWFLAKE_SCHEMA,"testSchema");
    config.put(SnowflakeSinkConnectorConfig.SNOWFLAKE_DATABASE, "testDatabase");
    return config;
  }


  @Test
  public void testConfig()
  {
    Map<String, String> config = getConfig();
    Utils.validateConfig(config);
  }

  //Empty parameters

  @Test(expected = SnowflakeKafkaConnectorException.class)
  public void testEmptyName()
  {
    Map<String, String> config = getConfig();
    config.remove(SnowflakeSinkConnectorConfig.NAME);
    Utils.validateConfig(config);
  }

  @Test(expected = SnowflakeKafkaConnectorException.class)
  public void testEmptyTopics()
  {
    Map<String, String> config = getConfig();
    config.remove(SnowflakeSinkConnectorConfig.TOPICS);
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
  public void testEmptyProxyHost()
  {
    Map<String, String> config = getConfig();
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
  public void testEmptyAuthSource()
  {
    Map<String, String> config = getConfig();
    config.put(SnowflakeSinkConnectorConfig.SCHEMA_REGISTRY_AUTH_USER_INFO, "test:test");
    Utils.validateConfig(config);
  }

  @Test(expected = SnowflakeKafkaConnectorException.class)
  public void testEmptyUserInfo()
  {
    Map<String, String> config = getConfig();
    config.put(SnowflakeSinkConnectorConfig.SCHEMA_REGISTRY_AUTH_CREDENTIALS_SOURCE, "USER_INFO");
    Utils.validateConfig(config);
  }

  @Test
  public void testAuthentication()
  {
    Map<String, String> config = getConfig();
    config.put(SnowflakeSinkConnectorConfig.SCHEMA_REGISTRY_AUTH_CREDENTIALS_SOURCE, "USER_INFO");
    config.put(SnowflakeSinkConnectorConfig.SCHEMA_REGISTRY_AUTH_USER_INFO, "test:test");
    Utils.validateConfig(config);
  }

  @Test(expected = SnowflakeKafkaConnectorException.class)
  public void testIllegalTopicName()
  {
    Map<String, String> config = getConfig();
    config.put(SnowflakeSinkConnectorConfig.TOPICS,"$@#$#@%^$12312");
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
  public void testNonexistentTopic()
  {
    Map<String, String> config = getConfig();
    config.put(SnowflakeSinkConnectorConfig.TOPICS_TABLES_MAP,"topic:table1");
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

  @Test(expected = SnowflakeKafkaConnectorException.class)
  public void testNameMapNotCovered()
  {
    Map<String, String> config = getConfig();
    config.put(SnowflakeSinkConnectorConfig.TOPICS,"!@#,$%^,&*(");
    config.put(SnowflakeSinkConnectorConfig.TOPICS_TABLES_MAP,"!@#:table1,$%^:table2");
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



}
