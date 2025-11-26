package com.snowflake.kafka.connector.internal;

import static com.snowflake.kafka.connector.internal.TestUtils.TEST_CONNECTOR_NAME;

import com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig;
import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.internal.streaming.telemetry.SnowflakeTelemetryServiceV2;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import org.bouncycastle.operator.OperatorCreationException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class ConnectionServiceIT {
  private final SnowflakeConnectionService conn = TestUtils.getConnectionService();

  private final String tableName = TestUtils.randomTableName();
  private final String tableName1 = TestUtils.randomTableName();
  private final String topicName = TestUtils.randomTopicName();

  @Test
  void testEncryptedKey() throws IOException, OperatorCreationException {
    // no exception
    SnowflakeConnectionServiceFactory.builder()
        .setProperties(TestUtils.transformProfileFileToConnectorConfiguration(true))
        .build();
  }

  @Test
  void testOAuthAZ() {
    Map<String, String> confWithOAuth = TestUtils.getConfWithOAuth();
    assert confWithOAuth.containsKey(Utils.SF_OAUTH_CLIENT_ID);
    SnowflakeConnectionServiceFactory.builder().setProperties(TestUtils.getConfWithOAuth()).build();
  }

  @Test
  void testSetSSLProperties() {
    Map<String, String> testConfig = TestUtils.transformProfileFileToConnectorConfiguration(false);
    testConfig.put(Utils.SF_URL, "https://sfctest0.snowflakecomputing.com");
    assert SnowflakeConnectionServiceFactory.builder()
        .setProperties(testConfig)
        .getProperties()
        .getProperty(InternalUtils.JDBC_SSL)
        .equals("on");
    testConfig.put(Utils.SF_URL, "sfctest0.snowflakecomputing.com");
    assert SnowflakeConnectionServiceFactory.builder()
        .setProperties(testConfig)
        .getProperties()
        .getProperty(InternalUtils.JDBC_SSL)
        .equals("on");
    testConfig.put(Utils.SF_URL, "http://sfctest0.snowflakecomputing.com:400");
    assert SnowflakeConnectionServiceFactory.builder()
        .setProperties(testConfig)
        .getProperties()
        .getProperty(InternalUtils.JDBC_SSL)
        .equals("off");
  }

  @Test
  void createConnectionService_SnowpipeStreaming() {

    Map<String, String> config = TestUtils.getConnectorConfigurationForStreaming(false);
    SnowflakeSinkConnectorConfig.setDefaultValues(config);
    SnowflakeConnectionService service =
        SnowflakeConnectionServiceFactory.builder().setProperties(config).build();

    assert service.getConnectorName().equals(TEST_CONNECTOR_NAME);

    assert service.getTelemetryClient() instanceof SnowflakeTelemetryServiceV2;
  }

  @AfterEach
  void afterEach() {
    TestUtils.dropTable(tableName);
    TestUtils.dropTable(tableName1);
  }

  @Test
  void testTableFunctions() throws SQLException {
    // table doesn't exist
    assert !conn.tableExist(tableName);
    // create table
    conn.createTable(tableName);
    // table exists
    assert conn.tableExist(tableName);
    // insert some value
    TestUtils.executeQuery("insert into " + tableName + " values(123)");
    ResultSet resultSet = TestUtils.showTable(tableName);
    // value inserted
    assert InternalUtils.resultSize(resultSet) == 1;
    // create table if not exists
    conn.createTable(tableName);
    resultSet = TestUtils.showTable(tableName);
    // table hasn't been overwritten
    assert InternalUtils.resultSize(resultSet) == 1;
    // overwrite table
    conn.createTable(tableName, true);
    resultSet = TestUtils.showTable(tableName);
    // new table
    assert InternalUtils.resultSize(resultSet) == 0;
    // table is compatible
    assert conn.isTableCompatible(tableName);
    TestUtils.dropTable(tableName);
    // dropped table
    assert !conn.tableExist(tableName);
    // create incompatible table
    TestUtils.executeQuery("create table " + tableName + " (num int)");
    assert !conn.isTableCompatible(tableName);
    TestUtils.dropTable(tableName);
  }

  @Test
  void testConnectionFunction() {
    SnowflakeConnectionService service = TestUtils.getConnectionService();
    assert !service.isClosed();
    service.close();
    assert service.isClosed();
  }
}
