package com.snowflake.kafka.connector.internal;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.snowflake.kafka.connector.internal.advisory.AdvisoryMessage;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import org.junit.jupiter.api.Test;

public class StandardSnowflakeConnectionServiceAdvisoryTest {

  private static StandardSnowflakeConnectionService serviceWith(Connection conn) throws Exception {
    org.objenesis.Objenesis objenesis = new org.objenesis.ObjenesisStd();
    StandardSnowflakeConnectionService svc =
        objenesis.newInstance(StandardSnowflakeConnectionService.class);
    Field connField = StandardSnowflakeConnectionService.class.getDeclaredField("conn");
    connField.setAccessible(true);
    connField.set(svc, conn);
    Field loggerField = StandardSnowflakeConnectionService.class.getDeclaredField("LOGGER");
    loggerField.setAccessible(true);
    loggerField.set(svc, new KCLogger(StandardSnowflakeConnectionService.class.getName()));
    return svc;
  }

  private static Connection mockConnReturning(String json) throws Exception {
    Connection conn = mock(Connection.class);
    when(conn.isClosed()).thenReturn(false);
    PreparedStatement stmt = mock(PreparedStatement.class);
    ResultSet rs = mock(ResultSet.class);
    when(conn.prepareStatement(anyString())).thenReturn(stmt);
    when(stmt.executeQuery()).thenReturn(rs);
    when(rs.next()).thenReturn(true);
    when(rs.getString(1)).thenReturn(json);
    return conn;
  }

  @Test
  public void parsesMessages() throws Exception {
    String json =
        "{\"messages\":[{\"level\":\"WARN\",\"code\":\"DEPRECATED_VERSION\",\"text\":\"upgrade\"}]}";
    List<AdvisoryMessage> msgs =
        serviceWith(mockConnReturning(json))
            .getKcAdvisoryMessages("{\"connectorVersion\":\"4.1.0\"}");
    assertEquals(1, msgs.size());
    assertEquals("WARN", msgs.get(0).getLevel());
    assertEquals("DEPRECATED_VERSION", msgs.get(0).getCode());
    assertEquals("upgrade", msgs.get(0).getText());
  }

  @Test
  public void emptyMessages_returnsEmptyList() throws Exception {
    List<AdvisoryMessage> msgs =
        serviceWith(mockConnReturning("{\"messages\":[]}")).getKcAdvisoryMessages("{}");
    assertTrue(msgs.isEmpty());
  }

  @Test
  public void sqlException_failsSafeToEmptyList() throws Exception {
    Connection conn = mock(Connection.class);
    when(conn.isClosed()).thenReturn(false);
    when(conn.prepareStatement(anyString())).thenThrow(new SQLException("function does not exist"));
    List<AdvisoryMessage> msgs = serviceWith(conn).getKcAdvisoryMessages("{}");
    assertTrue(msgs.isEmpty()); // must not throw
  }

  @Test
  public void malformedJson_failsSafeToEmptyList() throws Exception {
    List<AdvisoryMessage> msgs =
        serviceWith(mockConnReturning("{not json")).getKcAdvisoryMessages("{}");
    assertTrue(msgs.isEmpty()); // must not throw
  }
}
