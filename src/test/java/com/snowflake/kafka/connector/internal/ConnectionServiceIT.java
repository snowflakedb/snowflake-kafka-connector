package com.snowflake.kafka.connector.internal;

import static com.snowflake.kafka.connector.internal.TestUtils.TEST_CONNECTOR_NAME;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import com.snowflake.kafka.connector.ConnectorConfigTools;
import com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams;
import com.snowflake.kafka.connector.internal.telemetry.SnowflakeTelemetryService;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class ConnectionServiceIT {
  private final SnowflakeConnectionService conn = buildNoCachingConnection();

  private static SnowflakeConnectionService buildNoCachingConnection() {
    Map<String, String> config = TestUtils.transformProfileFileToConnectorConfiguration(false);
    config.put(KafkaConnectorConfigParams.CACHE_TABLE_EXISTS, "false");
    config.put(KafkaConnectorConfigParams.CACHE_PIPE_EXISTS, "false");
    return SnowflakeConnectionServiceFactory.builder().setProperties(config).build();
  }

  private final String tableName = TestUtils.randomTableName();
  private final String tableName1 = TestUtils.randomTableName();

  @Test
  void testEncryptedKey() {
    // no exception
    SnowflakeConnectionServiceFactory.builder()
        .setProperties(TestUtils.transformProfileFileToConnectorConfiguration(true))
        .build();
  }

  @Test
  void testSetSSLProperties() {
    Map<String, String> testConfig = TestUtils.transformProfileFileToConnectorConfiguration(false);
    testConfig.put(
        KafkaConnectorConfigParams.SNOWFLAKE_URL_NAME, "https://sfctest0.snowflakecomputing.com");
    assert SnowflakeConnectionServiceFactory.builder()
        .setProperties(testConfig)
        .getProperties()
        .getProperty(InternalUtils.JDBC_SSL)
        .equals("on");
    testConfig.put(
        KafkaConnectorConfigParams.SNOWFLAKE_URL_NAME, "sfctest0.snowflakecomputing.com");
    assert SnowflakeConnectionServiceFactory.builder()
        .setProperties(testConfig)
        .getProperties()
        .getProperty(InternalUtils.JDBC_SSL)
        .equals("on");
    testConfig.put(
        KafkaConnectorConfigParams.SNOWFLAKE_URL_NAME,
        "http://sfctest0.snowflakecomputing.com:400");
    assert SnowflakeConnectionServiceFactory.builder()
        .setProperties(testConfig)
        .getProperties()
        .getProperty(InternalUtils.JDBC_SSL)
        .equals("off");
  }

  @Test
  void createConnectionService_SnowpipeStreaming() {

    Map<String, String> config = TestUtils.getConnectorConfigurationForStreaming(false);
    ConnectorConfigTools.setDefaultValues(config);
    SnowflakeConnectionService service =
        SnowflakeConnectionServiceFactory.builder().setProperties(config).build();

    assert service.getConnectorName().equals(TEST_CONNECTOR_NAME);

    assertThat(service.getTelemetryClient()).isInstanceOf(SnowflakeTelemetryService.class);
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
    TestUtils.createTableWithMetadataColumn(tableName);
    // table exists
    assert conn.tableExist(tableName);
    // insert some value
    TestUtils.executeQuery("insert into \"" + tableName + "\" values(123)");
    ResultSet resultSet = TestUtils.showTable(tableName);
    // value inserted
    assert InternalUtils.resultSize(resultSet) == 1;
    // create table if not exists
    TestUtils.createTableWithMetadataColumn(tableName);
    resultSet = TestUtils.showTable(tableName);
    // table hasn't been overwritten
    assert InternalUtils.resultSize(resultSet) == 1;
    // overwrite table
    TestUtils.createTableWithMetadataColumn(tableName, true);
    resultSet = TestUtils.showTable(tableName);
    // new table
    assert InternalUtils.resultSize(resultSet) == 0;
    // table is compatible
    assert conn.isTableCompatible(tableName);
    TestUtils.dropTable(tableName);
    // dropped table
    assert !conn.tableExist(tableName);
    // create incompatible table
    TestUtils.executeQuery("create table \"" + tableName + "\" (num int)");
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

  /**
   * Integration test for SNOW-3029864: Verifies that the configured snowflake.role.name is actually
   * used when establishing JDBC connections for DDL operations (table creation, schema checks,
   * etc.).
   */
  @Test
  void testRoleIsUsedInJdbcConnection() throws SQLException {
    // given - connection service with role from config
    Map<String, String> config = TestUtils.transformProfileFileToConnectorConfiguration(true);
    String expectedRole = config.get(KafkaConnectorConfigParams.SNOWFLAKE_ROLE_NAME);
    SnowflakeConnectionService service =
        SnowflakeConnectionServiceFactory.builder().setProperties(config).build();

    String actualRole;
    // when - get JDBC connection and query current role
    try (Statement stmt = service.getConnection().createStatement();
        ResultSet resultSet = stmt.executeQuery("SELECT CURRENT_ROLE()")) {
      resultSet.next();
      actualRole = resultSet.getString(1);
    }

    // then - the active role should match the configured role (case-insensitive, Snowflake
    // uppercases)
    assertThat(actualRole)
        .as("JDBC connection should use the configured snowflake.role.name")
        .isEqualToIgnoringCase(expectedRole);

    // and - DDL operations (table creation) should work with this role
    String testTable = TestUtils.randomTableName();
    TestUtils.createTableWithMetadataColumn(testTable);
    assertThat(service.tableExist(testTable))
        .as("Table creation should succeed with the configured role")
        .isTrue();

    // cleanup
    TestUtils.dropTable(testTable);
    service.close();
  }

  /**
   * Verifies getStructuredObjectFieldNames against a real backend: a table with two distinct
   * structured OBJECT columns. INFORMATION_SCHEMA.FIELDS has no column-name column, so the query
   * must join FIELDS.ROW_IDENTIFIER = COLUMNS.DTD_IDENTIFIER to return only the requested column's
   * sub-fields -- not the sibling OBJECT column's fields. A plain OBJECT_NAME (table) filter would
   * incorrectly return the union of both columns' fields.
   */
  @Test
  void testGetStructuredObjectFieldNames_scopesToRequestedColumn() {
    TestUtils.executeQuery(
        "create or replace table \""
            + tableName
            + "\" ("
            + "OBJ_A OBJECT(field_a1 NUMBER, field_a2 STRING), "
            + "OBJ_B OBJECT(field_b1 NUMBER, field_b2 STRING, field_b3 BOOLEAN))");

    Assertions.assertThat(conn.getStructuredObjectFieldNames(tableName, "OBJ_A"))
        .containsExactly("field_a1", "field_a2");
    Assertions.assertThat(conn.getStructuredObjectFieldNames(tableName, "OBJ_B"))
        .containsExactly("field_b1", "field_b2", "field_b3");
  }
}
