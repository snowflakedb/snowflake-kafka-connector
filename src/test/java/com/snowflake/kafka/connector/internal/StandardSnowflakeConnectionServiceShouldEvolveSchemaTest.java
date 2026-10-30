package com.snowflake.kafka.connector.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.startsWith;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * SNOW-4392681: under SPCS no role is configured, so the schema evolution grant check must use the
 * session's current role.
 */
public class StandardSnowflakeConnectionServiceShouldEvolveSchemaTest {

  // Mixed case: matches only if the current role is quoted rather than upper-cased.
  private static final String SESSION_ROLE = "Service_Owner";

  private Connection mockJdbcConn;
  private PreparedStatement currentRoleStmt;
  private StandardSnowflakeConnectionService service;

  @BeforeEach
  public void setUp() throws Exception {
    mockJdbcConn = mock(Connection.class);
    when(mockJdbcConn.isClosed()).thenReturn(false);
    PreparedStatement emptyStmt = mock(PreparedStatement.class);
    ResultSet empty = mock(ResultSet.class);
    when(emptyStmt.executeQuery()).thenReturn(empty);
    when(mockJdbcConn.prepareStatement(anyString())).thenReturn(emptyStmt);

    currentRoleStmt = mock(PreparedStatement.class);
    ResultSet roleRs = mock(ResultSet.class);
    when(roleRs.next()).thenReturn(true, false);
    when(roleRs.getString(1)).thenReturn(SESSION_ROLE);
    when(currentRoleStmt.executeQuery()).thenReturn(roleRs);
    when(mockJdbcConn.prepareStatement("select current_role()")).thenReturn(currentRoleStmt);

    PreparedStatement grantsStmt = mock(PreparedStatement.class);
    ResultSet grantsRs = mock(ResultSet.class);
    when(grantsRs.next()).thenReturn(true, false);
    when(grantsRs.getString("grantee_name")).thenReturn(SESSION_ROLE);
    when(grantsRs.getString("privilege")).thenReturn("OWNERSHIP");
    when(grantsStmt.executeQuery()).thenReturn(grantsRs);
    when(mockJdbcConn.prepareStatement(startsWith("show grants"))).thenReturn(grantsStmt);

    PreparedStatement tablesStmt = mock(PreparedStatement.class);
    ResultSet tablesRs = mock(ResultSet.class);
    when(tablesRs.next()).thenReturn(true, false);
    when(tablesRs.getString("enable_schema_evolution")).thenReturn("Y");
    when(tablesStmt.executeQuery()).thenReturn(tablesRs);
    when(mockJdbcConn.prepareStatement("show tables like ? limit 1")).thenReturn(tablesStmt);

    service = createServiceWithMockConnection(mockJdbcConn);
  }

  @ParameterizedTest
  @NullSource
  @ValueSource(strings = {"", " "})
  public void shouldCheckGrantsOfCurrentRoleWhenNoRoleGiven(String role) throws SQLException {
    assertThat(service.shouldEvolveSchema("MY_TABLE", role)).isTrue();

    verify(mockJdbcConn).prepareStatement("select current_role()");
  }

  @ParameterizedTest
  @ValueSource(strings = {"\"Service_Owner\""})
  public void shouldNotQueryCurrentRoleWhenRoleGiven(String role) throws SQLException {
    assertThat(service.shouldEvolveSchema("MY_TABLE", role)).isTrue();

    verify(mockJdbcConn, never()).prepareStatement("select current_role()");
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
}
