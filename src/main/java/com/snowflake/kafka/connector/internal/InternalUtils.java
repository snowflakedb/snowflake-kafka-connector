package com.snowflake.kafka.connector.internal;

import static org.apache.commons.lang3.StringUtils.isBlank;

import com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams;
import com.snowflake.kafka.connector.Utils;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.Properties;
import net.snowflake.client.core.SFSessionProperty;

public class InternalUtils {
  // authenticator type
  public static final String SNOWFLAKE_JWT = "snowflake_jwt";
  // JDBC parameter list
  static final String JDBC_DATABASE = "db";
  static final String JDBC_SCHEMA = "schema";
  static final String JDBC_USER = "user";
  static final String JDBC_PRIVATE_KEY = "privateKey";
  static final String JDBC_SSL = "ssl";
  static final String JDBC_SESSION_KEEP_ALIVE = "client_session_keep_alive";
  static final String JDBC_WAREHOUSE = "warehouse"; // for test only
  static final String JDBC_TOKEN = SFSessionProperty.TOKEN.getPropertyKey();
  static final String JDBC_QUERY_RESULT_FORMAT = "JDBC_QUERY_RESULT_FORMAT";
  // internal parameters

  private static final KCLogger LOGGER = new KCLogger(InternalUtils.class.getName());

  private static final DateTimeFormatter ISO_DATE_TIME_FORMAT =
      DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'").withZone(ZoneOffset.UTC);

  /**
   * count the size of result set
   *
   * @param resultSet sql result set
   * @return size
   * @throws SQLException when failed to read result set
   */
  static int resultSize(ResultSet resultSet) throws SQLException {
    int size = 0;
    while (resultSet.next()) {
      size++;
    }
    return size;
  }

  static void assertNotEmpty(String name, Object value) {
    if (value == null || (value instanceof String && value.toString().isEmpty())) {
      switch (name.toLowerCase()) {
        case "tablename":
          throw SnowflakeErrors.ERROR_0005.getException();
        case "pipename":
          throw SnowflakeErrors.ERROR_0006.getException();
        case "conf":
          throw SnowflakeErrors.ERROR_0001.getException();
        default:
          throw SnowflakeErrors.ERROR_0003.getException("parameter name: " + name);
      }
    }
  }

  /**
   * convert a timestamp to Date String
   *
   * @param time a long integer representing timestamp
   * @return date string
   */
  static String timestampToDate(long time) {
    String date = ISO_DATE_TIME_FORMAT.format(Instant.ofEpochMilli(time));
    LOGGER.debug("converted date: {}", date);
    return date;
  }

  /**
   * create a properties for snowflake connection
   *
   * @param connectorConfiguration a map contains all parameters
   * @param url target server url
   * @return a Properties instance
   */
  static Properties makeJdbcDriverPropertiesFromConnectorConfiguration(
      Map<String, String> connectorConfiguration, SnowflakeURL url) {
    Properties properties = new Properties();

    // decrypt rsa key
    String privateKey = "";
    String privateKeyPassphrase = "";
    String role = "";

    for (Map.Entry<String, String> entry : connectorConfiguration.entrySet()) {
      // case insensitive
      switch (entry.getKey().toLowerCase()) {
        case KafkaConnectorConfigParams.SNOWFLAKE_DATABASE_NAME:
          properties.put(JDBC_DATABASE, entry.getValue());
          break;
        case KafkaConnectorConfigParams.SNOWFLAKE_PRIVATE_KEY:
          privateKey = entry.getValue();
          break;
        case KafkaConnectorConfigParams.SNOWFLAKE_SCHEMA_NAME:
          properties.put(JDBC_SCHEMA, entry.getValue());
          break;
        case KafkaConnectorConfigParams.SNOWFLAKE_USER_NAME:
          properties.put(JDBC_USER, entry.getValue());
          break;
        case KafkaConnectorConfigParams.SNOWFLAKE_PRIVATE_KEY_PASSPHRASE:
          privateKeyPassphrase = entry.getValue();
          break;
        case KafkaConnectorConfigParams.SNOWFLAKE_ROLE_NAME:
          role = entry.getValue();
          break;
        default:
          // ignore unrecognized keys
      }
    }

    properties.put(SFSessionProperty.AUTHENTICATOR.getPropertyKey(), SNOWFLAKE_JWT);
    if (isBlank(privateKey)) {
      throw SnowflakeErrors.ERROR_0013.getException();
    }
    properties.put(
        JDBC_PRIVATE_KEY, PrivateKeyTool.parsePrivateKey(privateKey, privateKeyPassphrase));

    // set role for JDBC connection (SNOW-3029864)
    if (!isBlank(role)) {
      properties.put(SFSessionProperty.ROLE.getPropertyKey(), role);
    }

    // set ssl
    if (url.sslEnabled()) {
      properties.put(JDBC_SSL, "on");
    } else {
      properties.put(JDBC_SSL, "off");
    }
    // put values for optional parameters
    properties.put(JDBC_SESSION_KEEP_ALIVE, "true");
    // SNOW-989387 - Set query resultset format to JSON as a workaround
    properties.put(JDBC_QUERY_RESULT_FORMAT, "json");
    properties.put(SFSessionProperty.ALLOW_UNDERSCORES_IN_HOST.getPropertyKey(), "true");

    if (!properties.containsKey(JDBC_SCHEMA)) {
      throw SnowflakeErrors.ERROR_0014.getException();
    }

    if (!properties.containsKey(JDBC_DATABASE)) {
      throw SnowflakeErrors.ERROR_0015.getException();
    }

    if (!properties.containsKey(JDBC_USER)) {
      throw SnowflakeErrors.ERROR_0016.getException();
    }

    return properties;
  }

  /**
   * Helper method to decide whether to add any properties related to proxy server. These property
   * is passed on to snowflake JDBC while calling put API, which requires proxyProperties
   *
   * @param conf
   * @return proxy parameters if needed
   */
  protected static Properties generateProxyParametersIfRequired(Map<String, String> conf) {
    Properties proxyProperties = new Properties();
    // Set proxyHost and proxyPort only if both of them are present and are non null
    if (conf.get(KafkaConnectorConfigParams.JVM_PROXY_HOST) != null
        && conf.get(KafkaConnectorConfigParams.JVM_PROXY_PORT) != null) {
      proxyProperties.put(SFSessionProperty.USE_PROXY.getPropertyKey(), "true");
      proxyProperties.put(
          SFSessionProperty.PROXY_HOST.getPropertyKey(),
          conf.get(KafkaConnectorConfigParams.JVM_PROXY_HOST));
      proxyProperties.put(
          SFSessionProperty.PROXY_PORT.getPropertyKey(),
          conf.get(KafkaConnectorConfigParams.JVM_PROXY_PORT));

      // nonProxyHosts parameter is not required. Check if it was set or not.
      if (conf.get(KafkaConnectorConfigParams.JVM_NON_PROXY_HOSTS) != null) {
        proxyProperties.put(
            SFSessionProperty.NON_PROXY_HOSTS.getPropertyKey(),
            conf.get(KafkaConnectorConfigParams.JVM_NON_PROXY_HOSTS));
      }

      // For username and password, check if host and port are given.
      // If they are given, check if username and password are non null
      String username = conf.get(KafkaConnectorConfigParams.JVM_PROXY_USERNAME);
      String password = conf.get(KafkaConnectorConfigParams.JVM_PROXY_PASSWORD);

      if (username != null && password != null) {
        proxyProperties.put(SFSessionProperty.PROXY_USER.getPropertyKey(), username);
        proxyProperties.put(SFSessionProperty.PROXY_PASSWORD.getPropertyKey(), password);
      }
    }
    return proxyProperties;
  }

  protected static Properties parseJdbcPropertiesMap(Map<String, String> conf) {
    String jdbcConfigMapInput = conf.get(KafkaConnectorConfigParams.SNOWFLAKE_JDBC_MAP);
    if (jdbcConfigMapInput == null) {
      return new Properties();
    }
    Map<String, String> jdbcMap = Utils.parseCommaSeparatedKeyValuePairs(jdbcConfigMapInput);
    Properties properties = new Properties();
    properties.putAll(jdbcMap);
    return properties;
  }

  /** Interfaces to define the lambda function to be used by backoffAndRetry */
  public interface backoffFunction {
    Object apply() throws Exception;
  }
}
