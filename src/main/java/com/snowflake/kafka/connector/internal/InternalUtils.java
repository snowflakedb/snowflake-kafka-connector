package com.snowflake.kafka.connector.internal;

import static org.apache.commons.lang3.StringUtils.isBlank;

import com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams;
import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.config.SinkTaskConfig;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.Properties;

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
  static final String JDBC_TOKEN = JdbcPropertyKeys.TOKEN;
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
   * Build JDBC driver properties from a parsed {@link SinkTaskConfig}.
   *
   * @param config parsed sink task configuration
   * @param url target server url
   * @return a Properties instance ready for JDBC
   */
  static Properties makeJdbcDriverProperties(SinkTaskConfig config, SnowflakeURL url) {
    Properties properties = new Properties();

    putIfNotBlank(properties, JDBC_DATABASE, config.getSnowflakeDatabase());
    putIfNotBlank(properties, JDBC_SCHEMA, config.getSnowflakeSchema());
    putIfNotBlank(properties, JDBC_USER, config.getSnowflakeUser());
    putIfNotBlank(properties, JdbcPropertyKeys.ROLE, config.getSnowflakeRole());

    properties.put(JdbcPropertyKeys.AUTHENTICATOR, SNOWFLAKE_JWT);

    String privateKey = config.getSnowflakePrivateKey();
    if (isBlank(privateKey)) {
      throw SnowflakeErrors.ERROR_0013.getException();
    }
    properties.put(
        JDBC_PRIVATE_KEY,
        PrivateKeyTool.parsePrivateKey(privateKey, config.getSnowflakePrivateKeyPassphrase()));

    properties.put(JDBC_SSL, url.sslEnabled() ? "on" : "off");
    // put values for optional parameters
    properties.put(JDBC_SESSION_KEEP_ALIVE, "true");
    // SNOW-989387 - Set query resultset format to JSON as a workaround
    properties.put(JDBC_QUERY_RESULT_FORMAT, "json");
    properties.put(JdbcPropertyKeys.ALLOW_UNDERSCORES_IN_HOST, "true");

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

  private static void putIfNotBlank(Properties properties, String key, String value) {
    if (!isBlank(value)) {
      properties.put(key, value);
    }
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
      proxyProperties.put(JdbcPropertyKeys.USE_PROXY, "true");
      proxyProperties.put(
          JdbcPropertyKeys.PROXY_HOST, conf.get(KafkaConnectorConfigParams.JVM_PROXY_HOST));
      proxyProperties.put(
          JdbcPropertyKeys.PROXY_PORT, conf.get(KafkaConnectorConfigParams.JVM_PROXY_PORT));

      // nonProxyHosts parameter is not required. Check if it was set or not.
      if (conf.get(KafkaConnectorConfigParams.JVM_NON_PROXY_HOSTS) != null) {
        proxyProperties.put(
            JdbcPropertyKeys.NON_PROXY_HOSTS,
            conf.get(KafkaConnectorConfigParams.JVM_NON_PROXY_HOSTS));
      }

      // For username and password, check if host and port are given.
      // If they are given, check if username and password are non null
      String username = conf.get(KafkaConnectorConfigParams.JVM_PROXY_USERNAME);
      String password = conf.get(KafkaConnectorConfigParams.JVM_PROXY_PASSWORD);

      if (username != null && password != null) {
        proxyProperties.put(JdbcPropertyKeys.PROXY_USER, username);
        proxyProperties.put(JdbcPropertyKeys.PROXY_PASSWORD, password);
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
