package com.snowflake.kafka.connector.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams;
import com.snowflake.kafka.connector.config.SinkTaskConfig;
import com.snowflake.kafka.connector.config.SinkTaskConfigTestBuilder;
import com.snowflake.kafka.connector.mock.MockResultSetForSizeTest;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.common.config.types.Password;
import org.junit.jupiter.api.Test;

public class InternalUtilsTest {
  @Test
  public void testPrivateKey() {
    assert TestUtils.assertError(
        SnowflakeErrors.ERROR_0002, () -> PrivateKeyTool.parsePrivateKey("adfsfsaff", null));

    Map<String, String> connectorConfiguration =
        TestUtils.transformProfileFileToConnectorConfiguration(true);
    String privateKey =
        connectorConfiguration.get(KafkaConnectorConfigParams.SNOWFLAKE_PRIVATE_KEY);
    String pass =
        connectorConfiguration.get(KafkaConnectorConfigParams.SNOWFLAKE_PRIVATE_KEY_PASSPHRASE);
    // no exception
    PrivateKeyTool.parsePrivateKey(privateKey, pass);
    StringBuilder builder = new StringBuilder();
    builder.append("-----BEGIN RSA PRIVATE KEY-----\n");
    for (int i = 0; i < privateKey.length(); i++) {
      builder.append(privateKey.charAt(i));
      if ((i + 1) % 64 == 0) {
        builder.append("\n");
      }
    }
    builder.append("\n-----END RSA PRIVATE KEY-----");
    String originalKey = builder.toString();
    // no exception
    PrivateKeyTool.parsePrivateKey(originalKey, pass);
  }

  @Test
  public void testTimestampToDateConversion() {
    long t = 1563492758649L;
    assert InternalUtils.timestampToDate(t).equals("2019-07-18T23:32:38Z");
  }

  @Test
  public void testAssertNotEmpty() {
    InternalUtils.assertNotEmpty("tableName", "name");
    assert TestUtils.assertError(
        SnowflakeErrors.ERROR_0005, () -> InternalUtils.assertNotEmpty("TABLENAME", null));
    assert TestUtils.assertError(
        SnowflakeErrors.ERROR_0005, () -> InternalUtils.assertNotEmpty("tableName", ""));
    assert TestUtils.assertError(
        SnowflakeErrors.ERROR_0006, () -> InternalUtils.assertNotEmpty("pipeName", null));
    assert TestUtils.assertError(
        SnowflakeErrors.ERROR_0006, () -> InternalUtils.assertNotEmpty("pipeName", ""));
    assert TestUtils.assertError(
        SnowflakeErrors.ERROR_0001, () -> InternalUtils.assertNotEmpty("conf", null));
    assert TestUtils.assertError(
        SnowflakeErrors.ERROR_0003, () -> InternalUtils.assertNotEmpty("sfdsfdsfd", null));
    assert TestUtils.assertError(
        SnowflakeErrors.ERROR_0003, () -> InternalUtils.assertNotEmpty("zxcxzcx", ""));
  }

  @Test
  public void testMakeJdbcDriverProperties() {
    Map<String, String> config = TestUtils.transformProfileFileToConnectorConfiguration(true);
    SnowflakeURL url = TestUtils.getUrl();
    SinkTaskConfig parsedConfig = SinkTaskConfig.from(config, true);
    Properties prop = InternalUtils.makeJdbcDriverProperties(parsedConfig, url);
    assert prop.containsKey(InternalUtils.JDBC_DATABASE);
    assert prop.containsKey(InternalUtils.JDBC_PRIVATE_KEY);
    assert prop.containsKey(InternalUtils.JDBC_SCHEMA);
    assert prop.containsKey(InternalUtils.JDBC_USER);
    assert prop.containsKey(InternalUtils.JDBC_SESSION_KEEP_ALIVE);
    assert prop.containsKey(InternalUtils.JDBC_SSL);

    assert prop.getProperty(InternalUtils.JDBC_SESSION_KEEP_ALIVE).equals("true");
    if (url.sslEnabled()) {
      assert prop.getProperty(InternalUtils.JDBC_SSL).equals("on");
    } else {
      assert prop.getProperty(InternalUtils.JDBC_SSL).equals("off");
    }

    assert TestUtils.assertError(
        SnowflakeErrors.ERROR_0013,
        () -> {
          Map<String, String> t = new HashMap<>(config);
          t.remove(KafkaConnectorConfigParams.SNOWFLAKE_PRIVATE_KEY);
          InternalUtils.makeJdbcDriverProperties(SinkTaskConfig.from(t, true), url);
        });

    assert TestUtils.assertError(
        SnowflakeErrors.ERROR_0014,
        () -> {
          Map<String, String> t = new HashMap<>(config);
          t.remove(KafkaConnectorConfigParams.SNOWFLAKE_SCHEMA_NAME);
          InternalUtils.makeJdbcDriverProperties(SinkTaskConfig.from(t, true), url);
        });

    assert TestUtils.assertError(
        SnowflakeErrors.ERROR_0015,
        () -> {
          Map<String, String> t = new HashMap<>(config);
          t.remove(KafkaConnectorConfigParams.SNOWFLAKE_DATABASE_NAME);
          InternalUtils.makeJdbcDriverProperties(SinkTaskConfig.from(t, true), url);
        });

    assert TestUtils.assertError(
        SnowflakeErrors.ERROR_0016,
        () -> {
          Map<String, String> t = new HashMap<>(config);
          t.remove(KafkaConnectorConfigParams.SNOWFLAKE_USER_NAME);
          InternalUtils.makeJdbcDriverProperties(SinkTaskConfig.from(t, true), url);
        });
  }

  /**
   * Regression test for SNOW-3029864: snowflake.role.name must be propagated to the JDBC connection
   * properties so that DDL operations (table creation, schema checks) run under the configured role
   * rather than the user's default role.
   */
  @Test
  public void testMakeJdbcDriverProperties_shouldIncludeRoleName() {
    // given
    Map<String, String> config = TestUtils.transformProfileFileToConnectorConfiguration(true);
    String expectedRole = config.get(KafkaConnectorConfigParams.SNOWFLAKE_ROLE_NAME);
    SnowflakeURL url = TestUtils.getUrl();

    // when
    Properties props =
        InternalUtils.makeJdbcDriverProperties(SinkTaskConfig.from(config, true), url);

    // then — the role from connector config must appear in the JDBC properties
    String rolePropertyKey = JdbcPropertyKeys.ROLE;
    assertTrue(
        props.containsKey(rolePropertyKey),
        "JDBC properties must contain the role property (key='"
            + rolePropertyKey
            + "'), but found keys: "
            + props.keySet());
    assertEquals(
        expectedRole,
        props.getProperty(rolePropertyKey),
        "JDBC role property must match the configured snowflake.role.name");
  }

  @Test
  public void testResultSize() throws SQLException {
    ResultSet resultSet = new MockResultSetForSizeTest(0);
    assert InternalUtils.resultSize(resultSet) == 0;
    resultSet = new MockResultSetForSizeTest(100);
    assert InternalUtils.resultSize(resultSet) == 100;
  }

  @Test
  public void parseJdbcPropertiesMapTest() {
    String input =
        "isInsecureMode:true,  disableSamlURLCheck:false, passcodeInPassword:on, foo:bar,"
            + " networkTimeout:100";
    SinkTaskConfig config =
        SinkTaskConfigTestBuilder.builder()
            .connectorName("test")
            .taskId("0")
            .jdbcMap(input)
            .build();
    // when
    Properties jdbcPropertiesMap = InternalUtils.parseJdbcPropertiesMap(config);
    // then
    assertEquals(jdbcPropertiesMap.size(), 5);
  }

  @Test
  public void makeJdbcDriverProperties_setsAllFields() {
    String pemKey = Base64.getEncoder().encodeToString(TestUtils.generatePrivateKey().getEncoded());
    SnowflakeURL url = new SnowflakeURL("https://testaccount.snowflakecomputing.com:443");

    SinkTaskConfig taskConfig =
        SinkTaskConfigTestBuilder.builder()
            .connectorName("test-connector")
            .taskId("0")
            .snowflakeDatabase("MY_DB")
            .snowflakeSchema("MY_SCHEMA")
            .snowflakeUser("MY_USER")
            .snowflakePrivateKey(new Password(pemKey))
            .snowflakeRole("MY_ROLE")
            .snowflakeUrl(url.getFullUrl())
            .build();

    Properties props = InternalUtils.makeJdbcDriverProperties(taskConfig, url);

    assertEquals("MY_DB", props.getProperty(InternalUtils.JDBC_DATABASE));
    assertEquals("MY_SCHEMA", props.getProperty(InternalUtils.JDBC_SCHEMA));
    assertEquals("MY_USER", props.getProperty(InternalUtils.JDBC_USER));
    assertEquals("MY_ROLE", props.getProperty(JdbcPropertyKeys.ROLE));
    assertTrue(props.containsKey(InternalUtils.JDBC_PRIVATE_KEY));
    assertEquals("on", props.getProperty(InternalUtils.JDBC_SSL));
    assertEquals("true", props.getProperty(InternalUtils.JDBC_SESSION_KEEP_ALIVE));
    assertEquals("json", props.getProperty(InternalUtils.JDBC_QUERY_RESULT_FORMAT));
  }

  @Test
  public void makeJdbcDriverProperties_missingPrivateKey_throws() {
    SnowflakeURL url = new SnowflakeURL("https://testaccount.snowflakecomputing.com:443");

    SinkTaskConfig taskConfig =
        SinkTaskConfigTestBuilder.builder()
            .connectorName("test-connector")
            .taskId("0")
            .snowflakeDatabase("MY_DB")
            .snowflakeSchema("MY_SCHEMA")
            .snowflakeUser("MY_USER")
            .snowflakeRole("MY_ROLE")
            .snowflakeUrl(url.getFullUrl())
            .build();

    SnowflakeKafkaConnectorException exception =
        assertThrows(
            SnowflakeKafkaConnectorException.class,
            () -> InternalUtils.makeJdbcDriverProperties(taskConfig, url));
    assertEquals("0013", exception.getCode());
  }

  @Test
  public void makeJdbcDriverProperties_noRole_omitsRoleProperty() {
    String pemKey = Base64.getEncoder().encodeToString(TestUtils.generatePrivateKey().getEncoded());
    SnowflakeURL url = new SnowflakeURL("https://testaccount.snowflakecomputing.com:443");

    SinkTaskConfig taskConfig =
        SinkTaskConfigTestBuilder.builder()
            .connectorName("test-connector")
            .taskId("0")
            .snowflakeDatabase("MY_DB")
            .snowflakeSchema("MY_SCHEMA")
            .snowflakeUser("MY_USER")
            .snowflakePrivateKey(new Password(pemKey))
            .snowflakeUrl(url.getFullUrl())
            .build();

    Properties props = InternalUtils.makeJdbcDriverProperties(taskConfig, url);

    assertFalse(
        props.containsKey(JdbcPropertyKeys.ROLE),
        "JDBC properties should not contain role when role is blank");
  }

  @Test
  public void makeJdbcDriverProperties_emptyStringRole_omitsRoleProperty() {
    String pemKey = Base64.getEncoder().encodeToString(TestUtils.generatePrivateKey().getEncoded());
    SnowflakeURL url = new SnowflakeURL("https://testaccount.snowflakecomputing.com:443");

    SinkTaskConfig taskConfig =
        SinkTaskConfigTestBuilder.builder()
            .connectorName("test-connector")
            .taskId("0")
            .snowflakeDatabase("MY_DB")
            .snowflakeSchema("MY_SCHEMA")
            .snowflakeUser("MY_USER")
            .snowflakePrivateKey(new Password(pemKey))
            .snowflakeUrl(url.getFullUrl())
            .snowflakeRole("")
            .build();

    Properties props = InternalUtils.makeJdbcDriverProperties(taskConfig, url);

    assertFalse(
        props.containsKey(JdbcPropertyKeys.ROLE),
        "JDBC properties should not contain role when role is empty string");
  }

  @Test
  public void makeJdbcDriverProperties_whitespaceRole_omitsRoleProperty() {
    String pemKey = Base64.getEncoder().encodeToString(TestUtils.generatePrivateKey().getEncoded());
    SnowflakeURL url = new SnowflakeURL("https://testaccount.snowflakecomputing.com:443");

    SinkTaskConfig taskConfig =
        SinkTaskConfigTestBuilder.builder()
            .connectorName("test-connector")
            .taskId("0")
            .snowflakeDatabase("MY_DB")
            .snowflakeSchema("MY_SCHEMA")
            .snowflakeUser("MY_USER")
            .snowflakePrivateKey(new Password(pemKey))
            .snowflakeUrl(url.getFullUrl())
            .snowflakeRole("   ")
            .build();

    Properties props = InternalUtils.makeJdbcDriverProperties(taskConfig, url);

    assertFalse(
        props.containsKey(JdbcPropertyKeys.ROLE),
        "JDBC properties should not contain role when role is whitespace");
  }
}
