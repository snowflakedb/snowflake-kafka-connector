package com.snowflake.kafka.connector.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.ArgumentCaptor;

public class StandardSnowflakeConnectionServicePipeExistTest {

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

  @ParameterizedTest
  @CsvSource(
      delimiter = '|',
      quoteCharacter = '\'',
      value = {
        "MY_PIPE|\"MY_PIPE\"",
        "my-pipe|\"my-pipe\"",
        "my.pipe.name|\"my.pipe.name\"",
        "topic.data-pipe|\"topic.data-pipe\""
      })
  public void testPipeExist_bindsQuotedIdentifier(String pipeName, String expectedBound)
      throws SQLException {
    assertThat(service.pipeExist(pipeName)).isTrue();

    ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
    verify(mockJdbcConn).prepareStatement(sqlCaptor.capture());
    assertThat(sqlCaptor.getValue()).isEqualToIgnoringCase("desc pipe identifier(?)");
    verify(mockStmt).setString(1, expectedBound);
  }

  @Test
  public void testPipeExist_escapesEmbeddedQuotes() throws SQLException {
    assertThat(service.pipeExist("a\"b")).isTrue();
    verify(mockStmt).setString(1, "\"a\"\"b\"");
  }

  @Test
  public void testPipeExist_sqlExceptionReturnsFalse() throws SQLException {
    when(mockStmt.execute()).thenThrow(new SQLException("syntax error unexpected '-'"));

    assertThat(service.pipeExist("my-pipe.name")).isFalse();
    verify(mockStmt).setString(1, "\"my-pipe.name\"");
  }

  @Test
  public void testMigrateSsv1ChannelOffset_bindsQuotedPipeName() throws SQLException {
    ResultSet mockRs = mock(ResultSet.class);
    when(mockRs.next()).thenReturn(true);
    when(mockRs.getString(1)).thenReturn("{\"ssv1_channel_found\":false}");
    when(mockStmt.executeQuery()).thenReturn(mockRs);

    service.migrateSsv1ChannelOffset("my.table", "ssv1", "ssv2", "my-pipe.name");

    verify(mockStmt).setString(1, "\"my.table\"");
    verify(mockStmt).setString(4, "\"my-pipe.name\"");
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
