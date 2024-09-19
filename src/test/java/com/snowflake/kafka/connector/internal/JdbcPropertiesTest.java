package com.snowflake.kafka.connector.internal;

import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Properties;
import org.junit.jupiter.api.Test;

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
    assertThatThrownBy(() -> JdbcProperties.create(connection, proxy, jdbcMap))
        .isInstanceOfSatisfying(
            SnowflakeKafkaConnectorException.class,
            ex -> {
              // property key is printed not value
              assertTrue(ex.getMessage().contains("user"));
              assertEquals("0031", ex.getCode());
            });
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
    assertThatThrownBy(() -> JdbcProperties.create(connection, proxy, jdbcMap))
        .isInstanceOfSatisfying(
            SnowflakeKafkaConnectorException.class,
            ex -> {
              // property key is printed not value
              assertTrue(ex.getMessage().contains("useProxy"));
              assertEquals("0031", ex.getCode());
            });
  }
}
