package com.snowflake.kafka.connector.internal;

import static org.junit.Assert.assertEquals;

import java.util.Properties;
import org.junit.Test;

public class JdbcPropertiesTest {

  @Test
  public void shouldCombineProperties() {
    // given
    SnowflakeURL url = TestUtils.getUrl();
    Properties connection = InternalUtils.createProperties(TestUtils.getConf(), url);

    Properties proxy = new Properties();
    proxy.put("useProxy", "true");

    Properties jdbcMap = new Properties();
    jdbcMap.put("insecureMode", "true");
    // when
    JdbcProperties jdbcProperties = JdbcProperties.create(connection, proxy, jdbcMap);
    // then
    int givenPropertiesSize = connection.size() + proxy.size() + jdbcMap.size();
    int mergedPropertiesSize = jdbcProperties.getProperties().size();

    assertEquals(givenPropertiesSize, mergedPropertiesSize);
  }

  @Test
  public void shouldThrowWhen_jdbcMap_overridesConnection() {
    Properties connection = new Properties();
    connection.put("user", "test_user1");

    Properties proxy = new Properties();

    Properties jdbcMap = new Properties();
    jdbcMap.put("user", "test_user2");
    jdbcMap.put("insecureMode", "true");
    // expect
    assert TestUtils.assertError(
        SnowflakeErrors.ERROR_0031, () -> JdbcProperties.create(connection, proxy, jdbcMap));
  }

  @Test
  public void shouldThrowWhen_jdbcMap_overridesProxy() {
    Properties connection = new Properties();
    connection.put("user", "test_user1");

    Properties proxy = new Properties();
    proxy.put("useProxy", "true");

    Properties jdbcMap = new Properties();
    jdbcMap.put("useProxy", "true");
    jdbcMap.put("insecureMode", "false");
    // expect
    assert TestUtils.assertError(
        SnowflakeErrors.ERROR_0031, () -> JdbcProperties.create(connection, proxy, jdbcMap));
  }
}
