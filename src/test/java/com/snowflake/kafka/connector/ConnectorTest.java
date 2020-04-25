package com.snowflake.kafka.connector;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class ConnectorTest {

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
  public void testValidate()
  {

  }
}
