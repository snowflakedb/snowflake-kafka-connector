package com.snowflake.kafka.connector.internal;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

import com.snowflake.kafka.connector.internal.schemaevolution.ColumnInfos;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.PreparedStatement;
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
  private PreparedStatement mockStmt;
  private StandardSnowflakeConnectionService service;

  @BeforeEach
  public void setUp() throws Exception {
    mockJdbcConn = mock(Connection.class);
    mockStmt = mock(PreparedStatement.class);
    when(mockJdbcConn.isClosed()).thenReturn(false);
    when(mockJdbcConn.prepareStatement(anyString())).thenReturn(mockStmt);

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

  @Test
  public void testAppendColumnsToTable_singleColumn_generatesCorrectSql() throws SQLException {
    Map<String, ColumnInfos> columns = new LinkedHashMap<>();
    columns.put("new_col", new ColumnInfos("VARCHAR", null));

    service.appendColumnsToTable("test_table", columns);

    ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
    verify(mockJdbcConn).prepareStatement(sqlCaptor.capture());
    String sql = sqlCaptor.getValue();

    // Table name uses identifier(?), column name is quoted inline
    assertTrue(sql.startsWith("alter table identifier(?) add column if not exists "));
    assertTrue(sql.contains("\"new_col\" VARCHAR"));
    assertTrue(sql.contains("comment 'column created by schema evolution"));

    // Only the table name is a binding
    verify(mockStmt).setString(1, "test_table");
    verify(mockStmt).execute();
  }

  @Test
  public void testAppendColumnsToTable_multipleColumns_repeatsIfNotExists() throws SQLException {
    Map<String, ColumnInfos> columns = new LinkedHashMap<>();
    columns.put("col_a", new ColumnInfos("VARCHAR", null));
    columns.put("col_b", new ColumnInfos("NUMBER", null));

    service.appendColumnsToTable("test_table", columns);

    ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
    verify(mockJdbcConn).prepareStatement(sqlCaptor.capture());
    String sql = sqlCaptor.getValue();

    assertTrue(sql.contains("\"col_a\" VARCHAR"));
    assertTrue(sql.contains(", if not exists \"col_b\" NUMBER"));

    verify(mockStmt).setString(1, "test_table");
    verify(mockStmt).execute();
  }

  @Test
  public void testAppendColumnsToTable_withComment_includesDdlComment() throws SQLException {
    Map<String, ColumnInfos> columns = new LinkedHashMap<>();
    columns.put("col1", new ColumnInfos("INT", "source field doc"));

    service.appendColumnsToTable("test_table", columns);

    ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
    verify(mockJdbcConn).prepareStatement(sqlCaptor.capture());
    String sql = sqlCaptor.getValue();

    assertTrue(sql.contains("INT comment 'source field doc'"));
  }

  @Test
  public void testAppendColumnsToTable_nullMap_doesNothing() throws SQLException {
    service.appendColumnsToTable("test_table", null);
    verify(mockJdbcConn, never()).prepareStatement(anyString());
  }

  @Test
  public void testAppendColumnsToTable_emptyMap_doesNothing() throws SQLException {
    service.appendColumnsToTable("test_table", Collections.emptyMap());
    verify(mockJdbcConn, never()).prepareStatement(anyString());
  }

  @Test
  public void testAppendColumnsToTable_sqlException_throwsError2015() throws SQLException {
    when(mockJdbcConn.prepareStatement(anyString())).thenThrow(new SQLException("test error"));

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

    ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
    verify(mockJdbcConn).prepareStatement(sqlCaptor.capture());
    String sql = sqlCaptor.getValue();

    // Table name uses identifier(?), column names are quoted inline
    assertTrue(sql.startsWith("alter table identifier(?) alter "));
    assertTrue(sql.contains("\"COL1\" drop not null"));
    assertTrue(
        sql.contains(
            "\"COL1\" comment 'column altered to be nullable by schema evolution"
                + " from Snowflake Kafka Connector'"));

    verify(mockStmt).setString(1, "test_table");
    verify(mockStmt).execute();
  }

  @Test
  public void testAlterNonNullableColumns_multipleColumns_generatesCorrectSql()
      throws SQLException {
    service.alterNonNullableColumns("test_table", Arrays.asList("COL_A", "COL_B"));

    ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
    verify(mockJdbcConn).prepareStatement(sqlCaptor.capture());
    String sql = sqlCaptor.getValue();

    assertTrue(sql.contains("\"COL_A\" drop not null"));
    assertTrue(sql.contains("\"COL_B\" drop not null"));

    verify(mockStmt).setString(1, "test_table");
    verify(mockStmt).execute();
  }

  @Test
  public void testAppendColumnsToTable_caseSensitiveColumnsQuotedInline() throws SQLException {
    Map<String, ColumnInfos> columns = new LinkedHashMap<>();
    columns.put("city", new ColumnInfos("VARCHAR", null));

    service.appendColumnsToTable("test_table", columns);

    ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
    verify(mockJdbcConn).prepareStatement(sqlCaptor.capture());
    String sql = sqlCaptor.getValue();

    // Lowercase "city" is quoted inline to preserve case
    assertTrue(sql.contains("\"city\" VARCHAR"));
  }

  @Test
  public void testAlterNonNullableColumns_caseSensitiveColumnsQuotedInline() throws SQLException {
    service.alterNonNullableColumns("test_table", Arrays.asList("city"));

    ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
    verify(mockJdbcConn).prepareStatement(sqlCaptor.capture());
    String sql = sqlCaptor.getValue();

    assertTrue(sql.contains("\"city\" drop not null"));
    assertTrue(sql.contains("\"city\" comment"));
  }

  @Test
  public void testAppendColumnsToTable_embeddedQuotesEscaped() throws SQLException {
    Map<String, ColumnInfos> columns = new LinkedHashMap<>();
    columns.put("col\"name", new ColumnInfos("VARCHAR", null));

    service.appendColumnsToTable("test_table", columns);

    ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
    verify(mockJdbcConn).prepareStatement(sqlCaptor.capture());
    String sql = sqlCaptor.getValue();

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
    when(mockJdbcConn.prepareStatement(anyString())).thenThrow(new SQLException("test error"));

    SnowflakeKafkaConnectorException ex =
        assertThrows(
            SnowflakeKafkaConnectorException.class,
            () -> service.alterNonNullableColumns("test_table", Arrays.asList("COL1")));
    assertTrue(ex.getMessage().contains("2016"));
  }
}
