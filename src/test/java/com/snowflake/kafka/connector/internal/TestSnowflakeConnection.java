package com.snowflake.kafka.connector.internal;

import static com.snowflake.kafka.connector.internal.TestUtils.getConfFromFileName;

import com.snowflake.client.jdbc.SnowflakeDriver;
import com.snowflake.kafka.connector.Utils;
import java.sql.Connection;
import java.util.Properties;

/** Connection to test environment generated from a profile file stored locally. */
public class TestSnowflakeConnection {

  /** Given a profile file path name, generate a connection by constructing a snowflake driver. */
  public static Connection getConnection() throws Exception {
    SnowflakeURL url =
        new SnowflakeURL(getConfFromFileName(TestUtils.PROFILE_PATH).get(Utils.SF_URL));

    Properties properties =
        InternalUtils.createProperties(getConfFromFileName(TestUtils.PROFILE_PATH), url);

    return new SnowflakeDriver().connect(url.getJdbcUrl(), properties);
  }
}
