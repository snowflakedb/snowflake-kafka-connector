package com.snowflake.kafka.connector.internal;

import static com.snowflake.kafka.connector.internal.TestUtils.transformProfileFileToConnectorConfiguration;

import com.snowflake.client.jdbc.SnowflakeDriver;
import com.snowflake.kafka.connector.Utils;
import java.sql.Connection;
import java.util.Map;
import java.util.Properties;

/** Connection to test environment generated from a profile file stored locally. */
public class NonEncryptedKeyTestSnowflakeConnection {

  /** Given a profile file path name, generate a connection by constructing a snowflake driver. */
  public static Connection getConnection() throws Exception {

      Map<String, String> connectorConfiguration = transformProfileFileToConnectorConfiguration(false);
    SnowflakeURL url =
        new SnowflakeURL(connectorConfiguration.get(Utils.SF_URL));

    Properties properties =
        InternalUtils.makeJdbcDriverPropertiesFromConnectorConfiguration(connectorConfiguration, url);

    return new SnowflakeDriver().connect(url.getJdbcUrl(), properties);
  }
}
