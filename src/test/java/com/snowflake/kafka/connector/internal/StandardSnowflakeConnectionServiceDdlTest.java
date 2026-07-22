package com.snowflake.kafka.connector.internal;

import static org.assertj.core.api.Assertions.assertThat;
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
  // getStructuredObjectFieldNames tests
  // ---------------------------------------------------------------------------

  @Test
  public void testGetStructuredObjectFieldNames_returnsFieldNames() throws SQLException {
    PreparedStatement infoSchemaStmt = mock(PreparedStatement.class);
    ResultSet infoSchemaRs = mock(ResultSet.class);
    when(infoSchemaRs.next()).thenReturn(true, true, false);
    when(infoSchemaRs.getString("FIELD_NAME")).thenReturn("OFFSET", "TOPIC");
    when(infoSchemaStmt.executeQuery()).thenReturn(infoSchemaRs);

    // Override so the SELECT query also routes to infoSchemaStmt.
    when(mockJdbcConn.prepareStatement(
            argThat(s -> s != null && s.startsWith("SELECT FIELD_NAME"))))
        .thenReturn(infoSchemaStmt);

    List<String> fields = service.getStructuredObjectFieldNames("MY_TABLE", "RECORD_METADATA");

    assertThat(fields).containsExactly("OFFSET", "TOPIC");
    verify(infoSchemaStmt).setString(1, "MY_TABLE");
    verify(infoSchemaStmt).setString(2, "RECORD_METADATA");
  }

  @Test
  public void testGetStructuredObjectFieldNames_sqlException_returnsEmpty() throws SQLException {
    when(mockJdbcConn.prepareStatement(
            argThat(s -> s != null && s.startsWith("SELECT FIELD_NAME"))))
        .thenThrow(new SQLException("connection refused"));

    List<String> fields = service.getStructuredObjectFieldNames("MY_TABLE", "RECORD_METADATA");

    assertThat(fields).isEmpty();
  }

  @Test
  public void testGetStructuredObjectFieldNames_matchesIdentifierCaseVerbatim()
      throws SQLException {
    PreparedStatement infoSchemaStmt = mock(PreparedStatement.class);
    ResultSet infoSchemaRs = mock(ResultSet.class);
    when(infoSchemaRs.next()).thenReturn(true, false);
    when(infoSchemaRs.getString("FIELD_NAME")).thenReturn("OFFSET");
    when(infoSchemaStmt.executeQuery()).thenReturn(infoSchemaRs);

    ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
    when(mockJdbcConn.prepareStatement(
            argThat(s -> s != null && s.startsWith("SELECT FIELD_NAME"))))
        .thenReturn(infoSchemaStmt);

    // A non-uppercase table name -- the common case for pass-through topics and topic2table.map
    // values. The pre-fix query wrapped the bind params in UPPER(?), which never matched the
    // case-preserving (quoted) identifier stored in INFORMATION_SCHEMA and silently returned no
    // fields.
    List<String> fields =
        service.getStructuredObjectFieldNames("my_lower_table", "RECORD_METADATA");

    assertThat(fields).containsExactly("OFFSET");

    verify(mockJdbcConn, atLeastOnce()).prepareStatement(sqlCaptor.capture());
    String infoSchemaSql =
        sqlCaptor.getAllValues().stream()
            .filter(s -> s != null && s.startsWith("SELECT FIELD_NAME"))
            .findFirst()
            .orElseThrow(() -> new AssertionError("SELECT FIELD_NAME query was not executed"));
    assertThat(infoSchemaSql).doesNotContain("UPPER(");
    assertThat(infoSchemaSql).contains("TABLE_NAME = ?");
    // The identifier is passed through verbatim (not uppercased).
    verify(infoSchemaStmt).setString(1, "my_lower_table");
    verify(infoSchemaStmt).setString(2, "RECORD_METADATA");
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

  @Test
  public void testCreateTableWithOnlyMetadataColumn_otherSqlError_throws() throws SQLException {
    SQLException otherError = new SQLException("Some other SQL error");
    when(mockAlterStmt.execute()).thenThrow(otherError);

    assertThrows(
        SnowflakeKafkaConnectorException.class,
        () -> service.createTableWithOnlyMetadataColumn("MY_TABLE"));
  }

  @Test
  public void createIcebergTable_noOptions_generatesMandatorySkeleton() throws SQLException {
    // createIcebergTable issues exactly one prepareStatement (no SHOW probe).
    PreparedStatement createStmt = mock(PreparedStatement.class);
    when(mockJdbcConn.prepareStatement(anyString())).thenReturn(createStmt);

    service.createIcebergTableWithOnlyMetadataColumn("test_table", "");

    ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
    verify(mockJdbcConn).prepareStatement(sqlCaptor.capture());
    String sql = sqlCaptor.getValue().toLowerCase();

    assertThat(sql).startsWith("create iceberg table if not exists identifier(?)");
    assertThat(sql).contains("record_metadata");
    // connector-mandatory clauses are always present
    assertThat(sql).contains("catalog = 'snowflake'");
    assertThat(sql).contains("enable_schema_evolution = true");
    assertThat(sql).contains("error_logging = true");
    // no operator options -> nothing extra spliced in
    assertThat(sql).doesNotContain("iceberg_version");
    assertThat(sql).doesNotContain("external_volume");
    assertThat(sql).doesNotContain("cluster by");
    verify(createStmt).setString(1, "\"test_table\"");
    verify(createStmt).execute();
  }

  @Test
  public void createIcebergTable_splicesOptionsAfterColumnList() throws SQLException {
    PreparedStatement createStmt = mock(PreparedStatement.class);
    when(mockJdbcConn.prepareStatement(anyString())).thenReturn(createStmt);

    service.createIcebergTableWithOnlyMetadataColumn(
        "test_table", "EXTERNAL_VOLUME='my_vol' ICEBERG_VERSION=3 CLUSTER BY (id)");

    ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
    verify(mockJdbcConn).prepareStatement(sqlCaptor.capture());
    String sql = sqlCaptor.getValue();

    // options land right after the column list, before the connector-mandatory clauses so
    // positional clauses like CLUSTER BY are valid.
    assertThat(sql)
        .contains(
            ") EXTERNAL_VOLUME='my_vol' ICEBERG_VERSION=3 CLUSTER BY (id) catalog = 'SNOWFLAKE'");
    assertThat(sql.toLowerCase()).contains("enable_schema_evolution = true");
    assertThat(sql.toLowerCase()).contains("error_logging = true");
  }

  @Test
  public void createIcebergTable_blankOptions_treatedAsNone() throws SQLException {
    PreparedStatement createStmt = mock(PreparedStatement.class);
    when(mockJdbcConn.prepareStatement(anyString())).thenReturn(createStmt);

    service.createIcebergTableWithOnlyMetadataColumn("test_table", "   ");

    ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
    verify(mockJdbcConn).prepareStatement(sqlCaptor.capture());
    // collapses the column-list paren straight into the catalog clause (single space)
    assertThat(sqlCaptor.getValue()).contains(") catalog = 'SNOWFLAKE'");
  }

  @Test
  public void testHasErrorLoggingEnabled_onMeansEnabled() throws Exception {
    // SHOW TABLES renders error_logging as ON/OFF, not Y/N.
    Connection conn = mock(Connection.class);
    when(conn.isClosed()).thenReturn(false);
    PreparedStatement showStmt = mock(PreparedStatement.class);
    ResultSet rs = mock(ResultSet.class);
    when(rs.next()).thenReturn(true);
    when(rs.getString("error_logging")).thenReturn("ON");
    when(showStmt.executeQuery()).thenReturn(rs);
    when(conn.prepareStatement("show tables like ? limit 1")).thenReturn(showStmt);

    StandardSnowflakeConnectionService svc = createServiceWithMockConnection(conn);
    assertTrue(svc.hasErrorLoggingEnabled("my_table"));
  }

  @Test
  public void testHasErrorLoggingEnabled_offMeansDisabled() throws Exception {
    Connection conn = mock(Connection.class);
    when(conn.isClosed()).thenReturn(false);
    PreparedStatement showStmt = mock(PreparedStatement.class);
    ResultSet rs = mock(ResultSet.class);
    when(rs.next()).thenReturn(true);
    when(rs.getString("error_logging")).thenReturn("OFF");
    when(showStmt.executeQuery()).thenReturn(rs);
    when(conn.prepareStatement("show tables like ? limit 1")).thenReturn(showStmt);

    StandardSnowflakeConnectionService svc = createServiceWithMockConnection(conn);
    assertFalse(svc.hasErrorLoggingEnabled("my_table"));
  }

  // ---- createIcebergTableWithOnlyMetadataColumn: VARIANT-first with OBJECT fallback ----

  @Test
  public void createIcebergTable_prefersVariantMetadata() throws Exception {
    Connection conn = mock(Connection.class);
    when(conn.isClosed()).thenReturn(false);
    PreparedStatement stmt = mock(PreparedStatement.class);
    when(stmt.execute()).thenReturn(false);
    when(conn.prepareStatement(anyString())).thenReturn(stmt);

    StandardSnowflakeConnectionService svc = createServiceWithMockConnection(conn);
    svc.createIcebergTableWithOnlyMetadataColumn("t", "EXTERNAL_VOLUME='v'");

    ArgumentCaptor<String> sql = ArgumentCaptor.forClass(String.class);
    verify(conn, times(1)).prepareStatement(sql.capture()); // only the VARIANT attempt
    assertThat(sql.getValue().toLowerCase()).contains("record_metadata variant");
    assertThat(sql.getValue()).doesNotContain("OBJECT(");
  }

  @Test
  public void createIcebergTable_fallsBackToObjectOnVariantUnsupported() throws Exception {
    Connection conn = mock(Connection.class);
    when(conn.isClosed()).thenReturn(false);
    PreparedStatement stmt = mock(PreparedStatement.class);
    // First attempt (VARIANT) rejected with errno 91386; second attempt (OBJECT) succeeds.
    when(stmt.execute())
        .thenThrow(
            new SQLException("Unsupported data type 'VARIANT' for iceberg tables.", "42601", 91386))
        .thenReturn(false);
    when(conn.prepareStatement(anyString())).thenReturn(stmt);

    StandardSnowflakeConnectionService svc = createServiceWithMockConnection(conn);
    svc.createIcebergTableWithOnlyMetadataColumn("t", "");

    ArgumentCaptor<String> sql = ArgumentCaptor.forClass(String.class);
    verify(conn, times(2)).prepareStatement(sql.capture());
    assertThat(sql.getAllValues().get(0).toLowerCase()).contains("record_metadata variant");
    assertThat(sql.getAllValues().get(1)).contains("OBJECT(offset"); // structured OBJECT schema
  }

  @Test
  public void createIcebergTable_rethrowsNon91386WithoutFallback() throws Exception {
    Connection conn = mock(Connection.class);
    when(conn.isClosed()).thenReturn(false);
    PreparedStatement stmt = mock(PreparedStatement.class);
    // e.g. missing external volume (not the VARIANT-unsupported errno) -> must NOT fall back.
    when(stmt.execute())
        .thenThrow(new SQLException("External volume 'v' does not exist", "42601", 91361));
    when(conn.prepareStatement(anyString())).thenReturn(stmt);

    StandardSnowflakeConnectionService svc = createServiceWithMockConnection(conn);
    assertThrows(
        RuntimeException.class, () -> svc.createIcebergTableWithOnlyMetadataColumn("t", ""));
    verify(conn, times(1)).prepareStatement(anyString()); // no OBJECT fallback attempt
  }

  // ---- isRecordMetadataStructuredObject ----

  @Test
  public void isRecordMetadataStructuredObject_objectColumn_true() throws Exception {
    assertTrue(
        serviceForDescribe(
                "RECORD_METADATA", "OBJECT(offset NUMBER(19,0), topic VARCHAR(16777216))")
            .isRecordMetadataStructuredObject("t"));
  }

  @Test
  public void isRecordMetadataStructuredObject_variantColumn_false() throws Exception {
    assertFalse(
        serviceForDescribe("RECORD_METADATA", "VARIANT").isRecordMetadataStructuredObject("t"));
  }

  @Test
  public void isRecordMetadataStructuredObject_noMetadataColumn_false() throws Exception {
    assertFalse(
        serviceForDescribe("SOME_COL", "NUMBER(38,0)").isRecordMetadataStructuredObject("t"));
  }

  /** Builds a service whose DESC TABLE returns a single column with the given name/type. */
  private static StandardSnowflakeConnectionService serviceForDescribe(
      String columnName, String columnType) throws Exception {
    Connection conn = mock(Connection.class);
    when(conn.isClosed()).thenReturn(false);
    PreparedStatement stmt = mock(PreparedStatement.class);
    ResultSet rs = mock(ResultSet.class);
    when(rs.next()).thenReturn(true).thenReturn(false);
    when(rs.getString("name")).thenReturn(columnName);
    when(rs.getString("type")).thenReturn(columnType);
    when(rs.getString("comment")).thenReturn(null);
    when(rs.getString("null?")).thenReturn("Y");
    when(rs.getString("default")).thenReturn(null);
    when(rs.getString("autoincrement")).thenReturn(null);
    when(stmt.executeQuery()).thenReturn(rs);
    when(conn.prepareStatement(anyString())).thenReturn(stmt);
    return createServiceWithMockConnection(conn);
  }
}
