package com.snowflake.kafka.connector.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.mock.MockResultSetForSizeTest;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import net.snowflake.ingest.connection.IngestStatus;
import org.junit.jupiter.api.Test;

public class InternalUtilsTest {
  @Test
  public void testPrivateKey() {
    assert TestUtils.assertError(
        SnowflakeErrors.ERROR_0002, () -> PrivateKeyTool.parsePrivateKey("adfsfsaff", null));

    Map<String, String> connectorConfiguration = TestUtils.transformProfileFileToConnectorConfiguration(true);
    String privateKey = connectorConfiguration.get(Utils.SF_PRIVATE_KEY);
    String pass = connectorConfiguration.get(Utils.SF_PRIVATE_KEY_PASSPHRASE);
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
  public void testIngestStatusConversion() {
    assert InternalUtils.convertIngestStatus(IngestStatus.LOADED)
        == InternalUtils.IngestedFileStatus.LOADED;
    assert InternalUtils.convertIngestStatus(IngestStatus.LOAD_IN_PROGRESS)
        == InternalUtils.IngestedFileStatus.LOAD_IN_PROGRESS;
    assert InternalUtils.convertIngestStatus(IngestStatus.PARTIALLY_LOADED)
        == InternalUtils.IngestedFileStatus.PARTIALLY_LOADED;
    assert InternalUtils.convertIngestStatus(IngestStatus.LOAD_FAILED)
        == InternalUtils.IngestedFileStatus.FAILED;
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
        SnowflakeErrors.ERROR_0004, () -> InternalUtils.assertNotEmpty("stagename", null));
    assert TestUtils.assertError(
        SnowflakeErrors.ERROR_0004, () -> InternalUtils.assertNotEmpty("stageName", ""));
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
  public void testMakeJdbcDriverPropertiesFromConnectorConfiguration() {
    Map<String, String> config = TestUtils.transformProfileFileToConnectorConfiguration(true);
    SnowflakeURL url = TestUtils.getUrl();
    Properties prop = InternalUtils.makeJdbcDriverPropertiesFromConnectorConfiguration(config, url);
    assert prop.containsKey(InternalUtils.JDBC_DATABASE);
    assert prop.containsKey(InternalUtils.JDBC_PRIVATE_KEY);
    assert prop.containsKey(InternalUtils.JDBC_SCHEMA);
    assert prop.containsKey(InternalUtils.JDBC_USER);
    assert prop.containsKey(InternalUtils.JDBC_WAREHOUSE);
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
          t.remove(Utils.SF_PRIVATE_KEY);
          InternalUtils.makeJdbcDriverPropertiesFromConnectorConfiguration(t, url);
        });

    assert TestUtils.assertError(
        SnowflakeErrors.ERROR_0014,
        () -> {
          Map<String, String> t = new HashMap<>(config);
          t.remove(Utils.SF_SCHEMA);
          InternalUtils.makeJdbcDriverPropertiesFromConnectorConfiguration(t, url);
        });

    assert TestUtils.assertError(
        SnowflakeErrors.ERROR_0015,
        () -> {
          Map<String, String> t = new HashMap<>(config);
          t.remove(Utils.SF_DATABASE);
          InternalUtils.makeJdbcDriverPropertiesFromConnectorConfiguration(t, url);
        });

    assert TestUtils.assertError(
        SnowflakeErrors.ERROR_0016,
        () -> {
          Map<String, String> t = new HashMap<>(config);
          t.remove(Utils.SF_USER);
          InternalUtils.makeJdbcDriverPropertiesFromConnectorConfiguration(t, url);
        });
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
    String key = "snowflake.jdbc.map";
    String input =
        "isInsecureMode:true,  disableSamlURLCheck:false, passcodeInPassword:on, foo:bar,"
            + " networkTimeout:100";
    Map<String, String> config = new HashMap<>();
    config.put(key, input);
    // when
    Properties jdbcPropertiesMap = InternalUtils.parseJdbcPropertiesMap(config);
    // then
    assertEquals(jdbcPropertiesMap.size(), 5);
  }
}
