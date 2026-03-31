package com.snowflake.kafka.connector.internal;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.*;

import com.snowflake.kafka.connector.internal.schemaevolution.ColumnInfos;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/**
 * Tests for DDL methods in StandardSnowflakeConnectionService: appendColumnsToTable and
 * alterNonNullableColumns.
 */
public class StandardSnowflakeConnectionServiceDdlTest {

  private Connection mockJdbcConn;
  // Separate stubs for the isIcebergTable SHOW query vs the ALTER DDL query.
  private PreparedStatement mockShowStmt;
  private PreparedStatement mockAlterStmt;
  private ResultSet mockEmptyRs;
  private StandardSnowflakeConnectionService service;

  @BeforeEach
  public void setUp() throws Exception {
    mockJdbcConn = mock(Connection.class);
    when(mockJdbcConn.isClosed()).thenReturn(false);

    // isIcebergTable uses SHOW ICEBERG TABLES LIKE → returns empty ResultSet (non-iceberg)
    mockShowStmt = mock(PreparedStatement.class);
    mockEmptyRs = mock(ResultSet.class);
    when(mockEmptyRs.next()).thenReturn(false);
    when(mockShowStmt.executeQuery()).thenReturn(mockEmptyRs);

    // ALTER DDL statement
    mockAlterStmt = mock(PreparedStatement.class);

    when(mockJdbcConn.prepareStatement(argThat(s -> s != null && s.startsWith("show"))))
        .thenReturn(mockShowStmt);
    when(mockJdbcConn.prepareStatement(argThat(s -> s != null && !s.startsWith("show"))))
        .thenReturn(mockAlterStmt);

    service = createServiceWithMockConnection(mockJdbcConn);
  }

  private static StandardSnowflakeConnectionService createServiceWithMockConnection(
      Connection mockConn) throws Exception {
    org.objenesis.Objenesis objenesis = new org.objenesis.ObjenesisStd();
    StandardSnowflakeConnectionService svc =
        objenesis.newInstance(StandardSnowflakeConnectionService.class);

    Field connField = StandardSnowflakeConnectionService.class.getDeclaredField("conn");
    connField.setAccessible(true);
    connField.set(svc, mockConn);

    Field loggerField = StandardSnowflakeConnectionService.class.getDeclaredField("LOGGER");
    loggerField.setAccessible(true);
    loggerField.set(svc, new KCLogger(StandardSnowflakeConnectionService.class.getName()));

    return svc;
  }

  /** Captures the ALTER SQL (second prepareStatement call; first is the SHOW ICEBERG check). */
  private String captureAlterSql() throws SQLException {
    ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
    verify(mockJdbcConn, times(2)).prepareStatement(sqlCaptor.capture());
    return sqlCaptor.getAllValues().get(1);
  }

  @Test
  public void testAppendColumnsToTable_singleColumn_generatesCorrectSql() throws SQLException {
    Map<String, ColumnInfos> columns = new LinkedHashMap<>();
    columns.put("new_col", new ColumnInfos("VARCHAR", null));

    service.appendColumnsToTable("test_table", columns);

    String sql = captureAlterSql();

    // Table name uses identifier(?), column name is quoted inline
    assertTrue(sql.startsWith("alter table identifier(?) add column if not exists "));
    assertTrue(sql.contains("\"new_col\" VARCHAR"));
    assertTrue(sql.contains("comment 'column created by schema evolution"));

    // Only the table name is a binding
    verify(mockAlterStmt).setString(1, "\"test_table\"");
    verify(mockAlterStmt).execute();
  }

  @Test
  public void testAppendColumnsToTable_multipleColumns_repeatsIfNotExists() throws SQLException {
    Map<String, ColumnInfos> columns = new LinkedHashMap<>();
    columns.put("col_a", new ColumnInfos("VARCHAR", null));
    columns.put("col_b", new ColumnInfos("NUMBER", null));

    service.appendColumnsToTable("test_table", columns);

    String sql = captureAlterSql();

    assertTrue(sql.contains("\"col_a\" VARCHAR"));
    assertTrue(sql.contains(", if not exists \"col_b\" NUMBER"));

    verify(mockAlterStmt).setString(1, "\"test_table\"");
    verify(mockAlterStmt).execute();
  }

  @Test
  public void testAppendColumnsToTable_withComment_includesDdlComment() throws SQLException {
    Map<String, ColumnInfos> columns = new LinkedHashMap<>();
    columns.put("col1", new ColumnInfos("INT", "source field doc"));

    service.appendColumnsToTable("test_table", columns);

    String sql = captureAlterSql();

    assertTrue(sql.contains("INT comment 'source field doc'"));
  }

  @Test
  public void testAppendColumnsToTable_nullMap_doesNothing() throws SQLException {
    service.appendColumnsToTable("test_table", null);
    // No SQL calls at all — not even the isIcebergTable check
    verify(mockJdbcConn, never()).prepareStatement(anyString());
  }

  @Test
  public void testAppendColumnsToTable_emptyMap_doesNothing() throws SQLException {
    service.appendColumnsToTable("test_table", Collections.emptyMap());
    verify(mockJdbcConn, never()).prepareStatement(anyString());
  }

  @Test
  public void testAppendColumnsToTable_sqlException_throwsError2015() throws SQLException {
    // isIcebergTable SHOW succeeds (returns empty); only the ALTER fails
    when(mockJdbcConn.prepareStatement(argThat(s -> s != null && !s.startsWith("show"))))
        .thenThrow(new SQLException("test error"));

    Map<String, ColumnInfos> columns = new LinkedHashMap<>();
    columns.put("col1", new ColumnInfos("VARCHAR", null));

    SnowflakeKafkaConnectorException ex =
        assertThrows(
            SnowflakeKafkaConnectorException.class,
            () -> service.appendColumnsToTable("test_table", columns));
    assertTrue(ex.getMessage().contains("2015"));
  }

  @Test
  public void testAlterNonNullableColumns_singleColumn_generatesCorrectSql() throws SQLException {
    service.alterNonNullableColumns("test_table", Arrays.asList("COL1"));

    String sql = captureAlterSql();

    // Table name uses identifier(?), column names are quoted inline
    assertTrue(sql.startsWith("alter table identifier(?) alter "));
    assertTrue(sql.contains("\"COL1\" drop not null"));
    assertTrue(
        sql.contains(
            "\"COL1\" comment 'column altered to be nullable by schema evolution"
                + " from Snowflake Kafka Connector'"));

    verify(mockAlterStmt).setString(1, "\"test_table\"");
    verify(mockAlterStmt).execute();
  }

  @Test
  public void testAlterNonNullableColumns_multipleColumns_generatesCorrectSql()
      throws SQLException {
    service.alterNonNullableColumns("test_table", Arrays.asList("COL_A", "COL_B"));

    String sql = captureAlterSql();

    assertTrue(sql.contains("\"COL_A\" drop not null"));
    assertTrue(sql.contains("\"COL_B\" drop not null"));

    verify(mockAlterStmt).setString(1, "\"test_table\"");
    verify(mockAlterStmt).execute();
  }

  @Test
  public void testAppendColumnsToTable_caseSensitiveColumnsQuotedInline() throws SQLException {
    Map<String, ColumnInfos> columns = new LinkedHashMap<>();
    columns.put("city", new ColumnInfos("VARCHAR", null));

    service.appendColumnsToTable("test_table", columns);

    String sql = captureAlterSql();

    // Lowercase "city" is quoted inline to preserve case
    assertTrue(sql.contains("\"city\" VARCHAR"));
  }

  @Test
  public void testAlterNonNullableColumns_caseSensitiveColumnsQuotedInline() throws SQLException {
    service.alterNonNullableColumns("test_table", Arrays.asList("city"));

    String sql = captureAlterSql();

    assertTrue(sql.contains("\"city\" drop not null"));
    assertTrue(sql.contains("\"city\" comment"));
  }

  @Test
  public void testAppendColumnsToTable_embeddedQuotesEscaped() throws SQLException {
    Map<String, ColumnInfos> columns = new LinkedHashMap<>();
    columns.put("col\"name", new ColumnInfos("VARCHAR", null));

    service.appendColumnsToTable("test_table", columns);

    String sql = captureAlterSql();

    // Embedded double quotes are escaped per SQL standard
    assertTrue(sql.contains("\"col\"\"name\" VARCHAR"));
  }

  @Test
  public void testAlterNonNullableColumns_nullList_doesNothing() throws SQLException {
    service.alterNonNullableColumns("test_table", null);
    verify(mockJdbcConn, never()).prepareStatement(anyString());
  }

  @Test
  public void testAlterNonNullableColumns_emptyList_doesNothing() throws SQLException {
    service.alterNonNullableColumns("test_table", Collections.emptyList());
    verify(mockJdbcConn, never()).prepareStatement(anyString());
  }

  @Test
  public void testAlterNonNullableColumns_sqlException_throwsError2016() throws SQLException {
    // isIcebergTable SHOW succeeds (returns empty); only the ALTER fails
    when(mockJdbcConn.prepareStatement(argThat(s -> s != null && !s.startsWith("show"))))
        .thenThrow(new SQLException("test error"));

    SnowflakeKafkaConnectorException ex =
        assertThrows(
            SnowflakeKafkaConnectorException.class,
            () -> service.alterNonNullableColumns("test_table", Arrays.asList("COL1")));
    assertTrue(ex.getMessage().contains("2016"));
  }

  @Test
  public void testAppendColumnsToTable_icebergTable_usesAlterIcebergTable() throws SQLException {
    // Simulate isIcebergTable returning true
    when(mockEmptyRs.next()).thenReturn(true);

    Map<String, ColumnInfos> columns = new LinkedHashMap<>();
    columns.put("new_col", new ColumnInfos("VARCHAR", null));

    service.appendColumnsToTable("iceberg_table", columns);

    String sql = captureAlterSql();
    assertTrue(sql.startsWith("alter iceberg table identifier(?) add column if not exists "));
  }

  // ---------------------------------------------------------------------------
  // shouldEvolveSchema tests
  // ---------------------------------------------------------------------------

  @Test
  public void testShouldEvolveSchema_icebergTable_seEnabled_returnsTrue() throws Exception {
    // Grant row: grantee_name = role, privilege = OWNERSHIP
    ResultSet grantRs = mock(ResultSet.class);
    when(grantRs.next()).thenReturn(true, false);
    when(grantRs.getString("grantee_name")).thenReturn("TEST_ROLE");
    when(grantRs.getString("privilege")).thenReturn("OWNERSHIP");

    // SHOW TABLES returns nothing (iceberg table)
    ResultSet emptyRs = mock(ResultSet.class);
    when(emptyRs.next()).thenReturn(false);

    // SHOW ICEBERG TABLES returns a row with enable_schema_evolution = Y
    ResultSet icebergRs = mock(ResultSet.class);
    when(icebergRs.next()).thenReturn(true, false);
    when(icebergRs.getString("enable_schema_evolution")).thenReturn("Y");

    Connection conn = mock(Connection.class);
    when(conn.isClosed()).thenReturn(false);

    PreparedStatement grantStmt = mock(PreparedStatement.class);
    when(grantStmt.executeQuery()).thenReturn(grantRs);
    PreparedStatement showTablesStmt = mock(PreparedStatement.class);
    when(showTablesStmt.executeQuery()).thenReturn(emptyRs);
    PreparedStatement showIcebergStmt = mock(PreparedStatement.class);
    when(showIcebergStmt.executeQuery()).thenReturn(icebergRs);

    when(conn.prepareStatement(argThat(s -> s != null && s.startsWith("show grants"))))
        .thenReturn(grantStmt);
    when(conn.prepareStatement(argThat(s -> s != null && s.equals("show tables like ? limit 1"))))
        .thenReturn(showTablesStmt);
    when(conn.prepareStatement(
            argThat(s -> s != null && s.equals("show iceberg tables like ? limit 1"))))
        .thenReturn(showIcebergStmt);

    StandardSnowflakeConnectionService svc = createServiceWithMockConnection(conn);
    assertTrue(svc.shouldEvolveSchema("iceberg_table", "TEST_ROLE"));
  }

  @Test
  public void testShouldEvolveSchema_regularTable_seEnabled_returnsTrue() throws Exception {
    ResultSet grantRs = mock(ResultSet.class);
    when(grantRs.next()).thenReturn(true, false);
    when(grantRs.getString("grantee_name")).thenReturn("TEST_ROLE");
    when(grantRs.getString("privilege")).thenReturn("OWNERSHIP");

    ResultSet tableRs = mock(ResultSet.class);
    when(tableRs.next()).thenReturn(true, false);
    when(tableRs.getString("enable_schema_evolution")).thenReturn("Y");

    Connection conn = mock(Connection.class);
    when(conn.isClosed()).thenReturn(false);

    PreparedStatement grantStmt = mock(PreparedStatement.class);
    when(grantStmt.executeQuery()).thenReturn(grantRs);
    PreparedStatement showTablesStmt = mock(PreparedStatement.class);
    when(showTablesStmt.executeQuery()).thenReturn(tableRs);

    when(conn.prepareStatement(argThat(s -> s != null && s.startsWith("show grants"))))
        .thenReturn(grantStmt);
    when(conn.prepareStatement(argThat(s -> s != null && s.equals("show tables like ? limit 1"))))
        .thenReturn(showTablesStmt);

    StandardSnowflakeConnectionService svc = createServiceWithMockConnection(conn);
    assertTrue(svc.shouldEvolveSchema("regular_table", "TEST_ROLE"));
  }

  @Test
  public void testShouldEvolveSchema_tableNotFound_returnsFalse() throws Exception {
    ResultSet grantRs = mock(ResultSet.class);
    when(grantRs.next()).thenReturn(true, false);
    when(grantRs.getString("grantee_name")).thenReturn("TEST_ROLE");
    when(grantRs.getString("privilege")).thenReturn("OWNERSHIP");

    ResultSet emptyRs = mock(ResultSet.class);
    when(emptyRs.next()).thenReturn(false);

    Connection conn = mock(Connection.class);
    when(conn.isClosed()).thenReturn(false);

    PreparedStatement grantStmt = mock(PreparedStatement.class);
    when(grantStmt.executeQuery()).thenReturn(grantRs);
    PreparedStatement showTablesStmt = mock(PreparedStatement.class);
    when(showTablesStmt.executeQuery()).thenReturn(emptyRs);
    PreparedStatement showIcebergStmt = mock(PreparedStatement.class);
    when(showIcebergStmt.executeQuery()).thenReturn(emptyRs);

    when(conn.prepareStatement(argThat(s -> s != null && s.startsWith("show grants"))))
        .thenReturn(grantStmt);
    when(conn.prepareStatement(argThat(s -> s != null && s.equals("show tables like ? limit 1"))))
        .thenReturn(showTablesStmt);
    when(conn.prepareStatement(
            argThat(s -> s != null && s.equals("show iceberg tables like ? limit 1"))))
        .thenReturn(showIcebergStmt);

    StandardSnowflakeConnectionService svc = createServiceWithMockConnection(conn);
    assertFalse(svc.shouldEvolveSchema("missing_table", "TEST_ROLE"));
  }

  @Test
  public void testAlterNonNullableColumns_icebergTable_usesAlterIcebergTable() throws SQLException {
    // Simulate isIcebergTable returning true
    when(mockEmptyRs.next()).thenReturn(true);

    service.alterNonNullableColumns("iceberg_table", Arrays.asList("COL1"));

    String sql = captureAlterSql();
    assertTrue(sql.startsWith("alter iceberg table identifier(?) alter "));
  }
}
