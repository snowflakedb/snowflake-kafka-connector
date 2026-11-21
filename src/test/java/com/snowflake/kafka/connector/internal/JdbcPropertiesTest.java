package com.snowflake.kafka.connector.internal;

import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.snowflake.kafka.connector.config.SinkTaskConfig;
import com.snowflake.kafka.connector.config.SinkTaskConfigTestBuilder;
import java.util.Base64;
import java.util.Properties;
import org.apache.kafka.common.config.types.Password;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class JdbcPropertiesTest {

  @Test
  public void shouldCombineProperties() {
    // given
    SnowflakeURL url = TestUtils.getUrl();
    SinkTaskConfig parsedConfig =
        SinkTaskConfig.from(TestUtils.transformProfileFileToConnectorConfiguration(false), true);
    Properties connection = InternalUtils.makeJdbcDriverProperties(parsedConfig, url);

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

  @Test
  public void shouldThrowWhen_jdbcMap_overridesConnection_withDifferentCase() {
    Properties connection = new Properties();
    connection.put("user", "test_user1");

    Properties jdbcMap = new Properties();
    jdbcMap.put("USER", "test_user2");
    // expect
    assertThatThrownBy(() -> JdbcProperties.create(connection, new Properties(), jdbcMap))
        .isInstanceOfSatisfying(
            SnowflakeKafkaConnectorException.class,
            ex -> {
              assertTrue(ex.getMessage().contains("USER"));
              assertEquals("0031", ex.getCode());
            });
  }

  @Test
  public void shouldThrowWhen_jdbcMap_containsSameKeyWithDifferentCase() {
    Properties connection = new Properties();
    connection.put("user", "test_user1");

    Properties jdbcMap = new Properties();
    jdbcMap.put("queryTag", "a");
    jdbcMap.put("QUERYTAG", "b");
    // expect
    assertThatThrownBy(() -> JdbcProperties.create(connection, new Properties(), jdbcMap))
        .isInstanceOfSatisfying(
            SnowflakeKafkaConnectorException.class, ex -> assertEquals("0031", ex.getCode()));
  }

  /**
   * Goes through the same parsing path as the connector: snowflake.jdbc.map is parsed from its
   * string form and merged with the connector-owned driver properties.
   */
  @ParameterizedTest
  @ValueSource(
      strings = {
        "MULTI_STATEMENT_COUNT:0",
        "multi_statement_count:0",
        "Multi_Statement_Count:2",
        "  MULTI_STATEMENT_COUNT  :  0  ",
        "insecureMode:true, multi_statement_count:0",
        "MULTI_STATEMENT_COUNT:1",
      })
  public void shouldThrowWhen_jdbcMap_setsMultiStatementCount(String jdbcMap) {
    SnowflakeURL url = new SnowflakeURL("https://testaccount.snowflakecomputing.com:443");
    String pemKey = Base64.getEncoder().encodeToString(TestUtils.generatePrivateKey().getEncoded());
    SinkTaskConfig config =
        SinkTaskConfigTestBuilder.builder()
            .connectorName("test-connector")
            .taskId("0")
            .snowflakeDatabase("MY_DB")
            .snowflakeSchema("MY_SCHEMA")
            .snowflakeUser("MY_USER")
            .snowflakePrivateKey(new Password(pemKey))
            .snowflakeUrl(url.getFullUrl())
            .jdbcMap(jdbcMap)
            .build();
    Properties connection = InternalUtils.makeJdbcDriverProperties(config, url);
    Properties jdbcMapProperties = InternalUtils.parseJdbcPropertiesMap(config);
    // expect
    assertThatThrownBy(() -> JdbcProperties.create(connection, new Properties(), jdbcMapProperties))
        .isInstanceOfSatisfying(
            SnowflakeKafkaConnectorException.class,
            ex -> {
              assertTrue(ex.getMessage().toUpperCase().contains("MULTI_STATEMENT_COUNT"));
              assertEquals("0031", ex.getCode());
            });
  }
}
