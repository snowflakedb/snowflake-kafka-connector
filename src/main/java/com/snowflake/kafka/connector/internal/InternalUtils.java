package com.snowflake.kafka.connector.internal;

import com.google.common.base.Strings;
import com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig;
import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.internal.streaming.IngestionMethodConfig;
import com.snowflake.kafka.connector.internal.telemetry.SnowflakeTelemetryService;
import java.security.KeyFactory;
import java.security.PrivateKey;
import java.security.spec.PKCS8EncodedKeySpec;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;
import net.snowflake.client.core.SFSessionProperty;
import net.snowflake.client.jdbc.internal.apache.commons.codec.binary.Base64;
import net.snowflake.client.jdbc.internal.org.bouncycastle.jce.provider.BouncyCastleProvider;
import net.snowflake.ingest.connection.IngestStatus;

public class InternalUtils {
  // JDBC parameter list
  static final String JDBC_DATABASE = "db";
  static final String JDBC_SCHEMA = "schema";
  static final String JDBC_USER = "user";
  static final String JDBC_PRIVATE_KEY = "privateKey";
  static final String JDBC_SSL = "ssl";
  static final String JDBC_SESSION_KEEP_ALIVE = "client_session_keep_alive";
  static final String JDBC_WAREHOUSE = "warehouse"; // for test only
  static final String JDBC_AUTHENTICATOR = SFSessionProperty.AUTHENTICATOR.getPropertyKey();
  static final String JDBC_TOKEN = SFSessionProperty.TOKEN.getPropertyKey();
  static final String JDBC_QUERY_RESULT_FORMAT = "JDBC_QUERY_RESULT_FORMAT";
  // internal parameters
  static final long MAX_RECOVERY_TIME = 10 * 24 * 3600 * 1000; // 10 days

  private static final KCLogger LOGGER = new KCLogger(InternalUtils.class.getName());

  // backoff with 1, 2, 4, 8 seconds
  public static final int backoffSec[] = {0, 1, 2, 4, 8};

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
        case "stagename":
          throw SnowflakeErrors.ERROR_0004.getException();
        case "pipename":
          throw SnowflakeErrors.ERROR_0006.getException();
        case "conf":
          throw SnowflakeErrors.ERROR_0001.getException();
        default:
          throw SnowflakeErrors.ERROR_0003.getException("parameter name: " + name);
      }
    }
  }

  static PrivateKey parsePrivateKey(String key) {
    // remove header, footer, and line breaks
    key = key.replaceAll("-+[A-Za-z ]+-+", "");
    key = key.replaceAll("\\s", "");

    java.security.Security.addProvider(new BouncyCastleProvider());
    byte[] encoded = Base64.decodeBase64(key);
    try {
      KeyFactory kf = KeyFactory.getInstance("RSA");
      PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(encoded);
      return kf.generatePrivate(keySpec);
    } catch (Exception e) {
      throw SnowflakeErrors.ERROR_0002.getException(e);
    }
  }

  /**
   * convert a timestamp to Date String
   *
   * @param time a long integer representing timestamp
   * @return date string
   */
  static String timestampToDate(long time) {
    TimeZone tz = TimeZone.getTimeZone("UTC");

    DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");

    df.setTimeZone(tz);

    String date = df.format(new Date(time));

    LOGGER.debug("converted date: {}", date);

    return date;
  }

  /**
   * Use this function if you want to create properties from user provided Snowflake kafka connector
   * config. It assumes the caller wants to use this Property for Snowpipe based Kafka Connector.
   *
   * @param conf User provided kafka connector config
   * @param url target server url
   * @return Properties object which will be passed down to JDBC connection
   */
  static Properties createProperties(Map<String, String> conf, SnowflakeURL url) {
    return createProperties(conf, url, IngestionMethodConfig.SNOWPIPE);
  }

  /**
   * create a properties for snowflake connection
   *
   * @param conf a map contains all parameters
   * @param url target server url
   * @param ingestionMethodConfig which ingestion method is provided.
   * @return a Properties instance
   */
  static Properties createProperties(
      Map<String, String> conf, SnowflakeURL url, IngestionMethodConfig ingestionMethodConfig) {
    Properties properties = new Properties();

    // decrypt rsa key
    String privateKey = "";
    String privateKeyPassphrase = "";

    // OAuth params
    String oAuthClientId = "";
    String oAuthClientSecret = "";
    String oAuthRefreshToken = "";
    String oAuthTokenEndpoint = "";

    // OAuth access token
    String token = "";

    for (Map.Entry<String, String> entry : conf.entrySet()) {
      // case insensitive
      switch (entry.getKey().toLowerCase()) {
        case Utils.SF_DATABASE:
          properties.put(JDBC_DATABASE, entry.getValue());
          break;
        case Utils.SF_PRIVATE_KEY:
          privateKey = entry.getValue();
          break;
        case Utils.SF_SCHEMA:
          properties.put(JDBC_SCHEMA, entry.getValue());
          break;
        case Utils.SF_USER:
          properties.put(JDBC_USER, entry.getValue());
          break;
        case Utils.SF_WAREHOUSE:
          properties.put(JDBC_WAREHOUSE, entry.getValue());
          break;
        case Utils.PRIVATE_KEY_PASSPHRASE:
          privateKeyPassphrase = entry.getValue();
          break;
        case Utils.SF_AUTHENTICATOR:
          properties.put(JDBC_AUTHENTICATOR, entry.getValue());
          break;
        case Utils.SF_OAUTH_CLIENT_ID:
          oAuthClientId = entry.getValue();
          break;
        case Utils.SF_OAUTH_CLIENT_SECRET:
          oAuthClientSecret = entry.getValue();
          break;
        case Utils.SF_OAUTH_REFRESH_TOKEN:
          oAuthRefreshToken = entry.getValue();
          break;
        case Utils.SF_OAUTH_TOKEN_ENDPOINT:
          oAuthTokenEndpoint = entry.getValue();
        default:
          // ignore unrecognized keys
      }
    }

    // Set credential
    if (!properties.containsKey(JDBC_AUTHENTICATOR)) {
      properties.put(JDBC_AUTHENTICATOR, Utils.SNOWFLAKE_JWT);
    }
    if (properties.getProperty(JDBC_AUTHENTICATOR).equals(Utils.SNOWFLAKE_JWT)) {
      // JWT key pair auth
      if (!privateKeyPassphrase.isEmpty()) {
        properties.put(
            JDBC_PRIVATE_KEY,
            EncryptionUtils.parseEncryptedPrivateKey(privateKey, privateKeyPassphrase));
      } else if (!privateKey.isEmpty()) {
        properties.put(JDBC_PRIVATE_KEY, parsePrivateKey(privateKey));
      }
    } else if (properties.getProperty(JDBC_AUTHENTICATOR).equals(Utils.OAUTH)) {
      // OAuth auth
      if (oAuthClientId.isEmpty()) {
        throw SnowflakeErrors.ERROR_0026.getException();
      }
      if (oAuthClientSecret.isEmpty()) {
        throw SnowflakeErrors.ERROR_0027.getException();
      }
      if (oAuthRefreshToken.isEmpty()) {
        throw SnowflakeErrors.ERROR_0028.getException();
      }
      URL oauthUrl =
          oAuthTokenEndpoint.isEmpty()
              ? new SnowflakeURL(conf.get(Utils.SF_URL))
              : OAuthURL.from(oAuthTokenEndpoint);

      String accessToken =
          Utils.getSnowflakeOAuthAccessToken(
              oauthUrl, oAuthClientId, oAuthClientSecret, oAuthRefreshToken);
      properties.put(JDBC_TOKEN, accessToken);
    } else {
      throw SnowflakeErrors.ERROR_0029.getException();
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

    /**
     * Behavior change in JDBC release 3.13.25
     *
     * @see <a href="https://community.snowflake.com/s/article/JDBC-Driver-Release-Notes">Snowflake
     *     Documentation Release Notes </a>
     */
    properties.put(SFSessionProperty.ALLOW_UNDERSCORES_IN_HOST.getPropertyKey(), "true");

    if (ingestionMethodConfig == IngestionMethodConfig.SNOWPIPE_STREAMING) {
      final String providedSFRoleInConfig = conf.get(Utils.SF_ROLE);
      if (!Strings.isNullOrEmpty(providedSFRoleInConfig)) {
        LOGGER.info("Using provided role {} for JDBC connection.", providedSFRoleInConfig);
        properties.put(SFSessionProperty.ROLE, providedSFRoleInConfig);
      } else {
        LOGGER.info(
            "Snowflake role is not provided, user's default role will be used for JDBC connection");
      }
    }

    // required parameter check, the OAuth parameter is already checked when fetching access token
    if (properties.getProperty(JDBC_AUTHENTICATOR).equals(Utils.SNOWFLAKE_JWT)
        && !properties.containsKey(JDBC_PRIVATE_KEY)) {
      throw SnowflakeErrors.ERROR_0013.getException();
    }

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
    if (conf.get(SnowflakeSinkConnectorConfig.JVM_PROXY_HOST) != null
        && conf.get(SnowflakeSinkConnectorConfig.JVM_PROXY_PORT) != null) {
      proxyProperties.put(SFSessionProperty.USE_PROXY.getPropertyKey(), "true");
      proxyProperties.put(
          SFSessionProperty.PROXY_HOST.getPropertyKey(),
          conf.get(SnowflakeSinkConnectorConfig.JVM_PROXY_HOST));
      proxyProperties.put(
          SFSessionProperty.PROXY_PORT.getPropertyKey(),
          conf.get(SnowflakeSinkConnectorConfig.JVM_PROXY_PORT));

      // nonProxyHosts parameter is not required. Check if it was set or not.
      if (conf.get(SnowflakeSinkConnectorConfig.JVM_NON_PROXY_HOSTS) != null) {
        proxyProperties.put(
            SFSessionProperty.NON_PROXY_HOSTS.getPropertyKey(),
            conf.get(SnowflakeSinkConnectorConfig.JVM_NON_PROXY_HOSTS));
      }

      // For username and password, check if host and port are given.
      // If they are given, check if username and password are non null
      String username = conf.get(SnowflakeSinkConnectorConfig.JVM_PROXY_USERNAME);
      String password = conf.get(SnowflakeSinkConnectorConfig.JVM_PROXY_PASSWORD);

      if (username != null && password != null) {
        proxyProperties.put(SFSessionProperty.PROXY_USER.getPropertyKey(), username);
        proxyProperties.put(SFSessionProperty.PROXY_PASSWORD.getPropertyKey(), password);
      }
    }
    return proxyProperties;
  }

  protected static Properties parseJdbcPropertiesMap(Map<String, String> conf) {
    String jdbcConfigMapInput = conf.get(SnowflakeSinkConnectorConfig.SNOWFLAKE_JDBC_MAP);
    if (jdbcConfigMapInput == null) {
      return new Properties();
    }
    Map<String, String> jdbcMap = Utils.parseCommaSeparatedKeyValuePairs(jdbcConfigMapInput);
    Properties properties = new Properties();
    properties.putAll(jdbcMap);
    return properties;
  }

  /**
   * convert ingest status to ingested file status
   *
   * @param status an ingest status
   * @return an ingest file status
   */
  static IngestedFileStatus convertIngestStatus(IngestStatus status) {
    switch (status) {
      case LOADED:
        return IngestedFileStatus.LOADED;

      case LOAD_IN_PROGRESS:
        return IngestedFileStatus.LOAD_IN_PROGRESS;

      case PARTIALLY_LOADED:
        return IngestedFileStatus.PARTIALLY_LOADED;

      case LOAD_FAILED:

      default:
        return IngestedFileStatus.FAILED;
    }
  }

  /**
   * ingested file status some status are grouped as 'finalized' status (LOADED, PARTIALLY_LOADED,
   * FAILED) -- we can purge these files others are grouped as 'not_finalized'
   */
  enum IngestedFileStatus // for ingest sdk
  {
    LOADED,
    PARTIALLY_LOADED,
    FAILED,
    // partially_loaded, or failed
    LOAD_IN_PROGRESS,
    NOT_FOUND,
  }

  /** Interfaces to define the lambda function to be used by backoffAndRetry */
  public interface backoffFunction {
    Object apply() throws Exception;
  }

  /**
   * Backoff logic
   *
   * @param telemetry telemetry service
   * @param operation Internal Operation Type which corresponds to the lambda function runnable
   * @param runnable the lambda function itself
   * @return the object that the function returns
   * @throws Exception if the runnable function throws exception
   */
  public static Object backoffAndRetry(
      final SnowflakeTelemetryService telemetry,
      final SnowflakeInternalOperations operation,
      final backoffFunction runnable)
      throws Exception {
    Exception finalException = null;
    for (final int iteration : backoffSec) {
      if (iteration != 0) {
        Thread.sleep(iteration * 1000L);
        LOGGER.debug("Retry Count:{} for operation:{}", iteration, operation);
      }
      try {
        return runnable.apply();
      } catch (Exception e) {
        finalException = e;
        LOGGER.error(
            "Retry count:{} caught an exception for operation:{} with message:{}",
            iteration,
            operation,
            e.getMessage());
      }
    }
    throw SnowflakeErrors.ERROR_2010.getException(finalException, telemetry);
  }
}
