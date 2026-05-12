package com.snowflake.kafka.connector.internal;

import static org.apache.commons.lang3.StringUtils.isBlank;

import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.config.AuthenticatorType;
import com.snowflake.kafka.connector.config.SinkTaskConfig;
import com.snowflake.kafka.connector.internal.oauth.OAuthAccessTokenFetcher;
import com.snowflake.kafka.connector.internal.oauth.OAuthURL;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
import org.apache.kafka.common.config.types.Password;

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

    switch (config.getAuthenticator()) {
      case OAUTH:
        properties.put(JdbcPropertyKeys.AUTHENTICATOR, AuthenticatorType.OAUTH.toConfigValue());
        String oauthClientId =
            config.getOauthClientId().orElseThrow(SnowflakeErrors.ERROR_0026::getException);
        Password oauthClientSecret =
            config.getOauthClientSecret().orElseThrow(SnowflakeErrors.ERROR_0027::getException);
        URL oauthUrl =
            config.getOauthTokenEndpoint().isPresent()
                ? OAuthURL.from(config.getOauthTokenEndpoint().get())
                : url;
        properties.put(
            JDBC_TOKEN,
            OAuthAccessTokenFetcher.fetchAccessToken(
                oauthUrl, oauthClientId, oauthClientSecret, config.getOauthRefreshToken()));
        break;
      case SNOWFLAKE_JWT:
        properties.put(JdbcPropertyKeys.AUTHENTICATOR, SNOWFLAKE_JWT);
        properties.put(
            JDBC_PRIVATE_KEY,
            PrivateKeyTool.parsePrivateKey(
                config
                    .getSnowflakePrivateKey()
                    .orElseThrow(SnowflakeErrors.ERROR_0013::getException),
                config.getSnowflakePrivateKeyPassphrase()));
        break;
      default:
        throw new IllegalStateException("unhandled authenticator: " + config.getAuthenticator());
    }

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
   * @param config parsed connector configuration
   * @return proxy parameters if needed
   */
  protected static Properties generateProxyParametersIfRequired(SinkTaskConfig config) {
    Properties properties = new Properties();
    // Set proxyHost and proxyPort only if both of them are present and are non null
    if (config.getProxyHost() != null && config.getProxyPort() != null) {
      properties.put(JdbcPropertyKeys.USE_PROXY, "true");
      properties.put(JdbcPropertyKeys.PROXY_HOST, config.getProxyHost());
      properties.put(JdbcPropertyKeys.PROXY_PORT, config.getProxyPort());

      // nonProxyHosts parameter is not required. Check if it was set or not.
      if (config.getNonProxyHosts() != null) {
        properties.put(JdbcPropertyKeys.NON_PROXY_HOSTS, config.getNonProxyHosts());
      }

      // For username and password, check if host and port are given.
      // If they are given, check if username and password are non null
      if (config.getProxyUsername() != null && config.getProxyPassword() != null) {
        properties.put(JdbcPropertyKeys.PROXY_USER, config.getProxyUsername());
        properties.put(JdbcPropertyKeys.PROXY_PASSWORD, config.getProxyPassword());
      }
    }
    return properties;
  }

  protected static Properties parseJdbcPropertiesMap(SinkTaskConfig config) {
    if (config.getJdbcMap() == null) {
      return new Properties();
    }
    Properties properties = new Properties();
    properties.putAll(Utils.parseCommaSeparatedKeyValuePairs(config.getJdbcMap()));
    return properties;
  }

  /** Interfaces to define the lambda function to be used by backoffAndRetry */
  public interface backoffFunction {
    Object apply() throws Exception;
  }
}
