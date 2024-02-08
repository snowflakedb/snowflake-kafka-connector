/*
 * Copyright (c) 2019 Snowflake Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.snowflake.kafka.connector;

import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.BEHAVIOR_ON_NULL_VALUES_CONFIG;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.BehaviorOnNullValues.VALIDATOR;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.INGESTION_METHOD_OPT;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.JMX_OPT;

import com.google.common.collect.ImmutableMap;
import com.snowflake.kafka.connector.internal.BufferThreshold;
import com.snowflake.kafka.connector.internal.InternalUtils;
import com.snowflake.kafka.connector.internal.KCLogger;
import com.snowflake.kafka.connector.internal.OAuthConstants;
import com.snowflake.kafka.connector.internal.SnowflakeErrors;
import com.snowflake.kafka.connector.internal.SnowflakeInternalOperations;
import com.snowflake.kafka.connector.internal.SnowflakeURL;
import com.snowflake.kafka.connector.internal.streaming.IngestionMethodConfig;
import com.snowflake.kafka.connector.internal.streaming.StreamingUtils;
import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.Authenticator;
import java.net.PasswordAuthentication;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLEncoder;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import net.snowflake.client.jdbc.internal.apache.http.HttpHeaders;
import net.snowflake.client.jdbc.internal.apache.http.client.methods.CloseableHttpResponse;
import net.snowflake.client.jdbc.internal.apache.http.client.methods.HttpPost;
import net.snowflake.client.jdbc.internal.apache.http.client.utils.URIBuilder;
import net.snowflake.client.jdbc.internal.apache.http.entity.ContentType;
import net.snowflake.client.jdbc.internal.apache.http.entity.StringEntity;
import net.snowflake.client.jdbc.internal.apache.http.impl.client.CloseableHttpClient;
import net.snowflake.client.jdbc.internal.apache.http.impl.client.HttpClientBuilder;
import net.snowflake.client.jdbc.internal.apache.http.util.EntityUtils;
import net.snowflake.client.jdbc.internal.google.gson.JsonObject;
import net.snowflake.client.jdbc.internal.google.gson.JsonParser;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigValue;

/** Various arbitrary helper functions */
public class Utils {

  // Connector version, change every release
  public static final String VERSION = "2.1.0";

  // connector parameter list
  public static final String NAME = "name";
  public static final String SF_DATABASE = "snowflake.database.name";
  public static final String SF_SCHEMA = "snowflake.schema.name";
  public static final String SF_USER = "snowflake.user.name";
  public static final String SF_PRIVATE_KEY = "snowflake.private.key";
  public static final String SF_URL = "snowflake.url.name";
  public static final String SF_SSL = "sfssl"; // for test only
  public static final String SF_WAREHOUSE = "sfwarehouse"; // for test only
  public static final String PRIVATE_KEY_PASSPHRASE = "snowflake.private.key" + ".passphrase";
  public static final String SF_AUTHENTICATOR =
      "snowflake.authenticator"; // TODO: SNOW-889748 change to enum
  public static final String SF_OAUTH_CLIENT_ID = "snowflake.oauth.client.id";
  public static final String SF_OAUTH_CLIENT_SECRET = "snowflake.oauth.client.secret";
  public static final String SF_OAUTH_REFRESH_TOKEN = "snowflake.oauth.refresh.token";

  // authenticator type
  public static final String SNOWFLAKE_JWT = "snowflake_jwt";
  public static final String OAUTH = "oauth";

  /**
   * This value should be present if ingestion method is {@link
   * IngestionMethodConfig#SNOWPIPE_STREAMING}
   */
  public static final String SF_ROLE = "snowflake.role.name";

  // constants strings
  private static final String KAFKA_OBJECT_PREFIX = "SNOWFLAKE_KAFKA_CONNECTOR";

  // task id
  public static final String TASK_ID = "task_id";

  // jvm proxy
  public static final String HTTP_USE_PROXY = "http.useProxy";
  public static final String HTTPS_PROXY_HOST = "https.proxyHost";
  public static final String HTTPS_PROXY_PORT = "https.proxyPort";
  public static final String HTTP_PROXY_HOST = "http.proxyHost";
  public static final String HTTP_PROXY_PORT = "http.proxyPort";
  public static final String HTTP_NON_PROXY_HOSTS = "http.nonProxyHosts";

  public static final String JDK_HTTP_AUTH_TUNNELING = "jdk.http.auth.tunneling.disabledSchemes";
  public static final String HTTPS_PROXY_USER = "https.proxyUser";
  public static final String HTTPS_PROXY_PASSWORD = "https.proxyPassword";
  public static final String HTTP_PROXY_USER = "http.proxyUser";
  public static final String HTTP_PROXY_PASSWORD = "http.proxyPassword";

  // jdbc log dir
  public static final String JAVA_IO_TMPDIR = "java.io.tmpdir";

  private static final Random random = new Random();

  // mvn repo
  private static final String MVN_REPO =
      "https://repo1.maven.org/maven2/com/snowflake/snowflake-kafka-connector/";

  public static final String TABLE_COLUMN_CONTENT = "RECORD_CONTENT";
  public static final String TABLE_COLUMN_METADATA = "RECORD_METADATA";

  public static final String GET_EXCEPTION_FORMAT = "{}, Exception message: {}, cause: {}";
  public static final String GET_EXCEPTION_MISSING_MESSAGE = "missing exception message";
  public static final String GET_EXCEPTION_MISSING_CAUSE = "missing exception cause";

  private static final KCLogger LOGGER = new KCLogger(Utils.class.getName());

  /**
   * Quote the column name if needed: When there is quote already, we do nothing; otherwise we
   * convert the name to upper case and add quote
   */
  public static String quoteNameIfNeeded(String name) {
    int length = name.length();
    if (name.charAt(0) == '"' && (length >= 2 && name.charAt(length - 1) == '"')) {
      return name;
    }
    return '"' + name.toUpperCase() + '"';
  }

  /**
   * check the connector version from Maven repo, report if any update version is available.
   *
   * <p>A URl connection timeout is added in case Maven repo is not reachable in a proxy'd
   * environment. Returning false from this method doesnt have any side effects to start the
   * connector.
   */
  static boolean checkConnectorVersion() {
    LOGGER.info("Current Snowflake Kafka Connector Version: {}", VERSION);
    try {
      String latestVersion = null;
      int largestNumber = 0;
      URLConnection urlConnection = new URL(MVN_REPO).openConnection();
      urlConnection.setConnectTimeout(5000);
      urlConnection.setReadTimeout(5000);
      InputStream input = urlConnection.getInputStream();
      BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(input));
      String line;
      Pattern pattern = Pattern.compile("(\\d+\\.\\d+\\.\\d+?)");
      while ((line = bufferedReader.readLine()) != null) {
        Matcher matcher = pattern.matcher(line);
        if (matcher.find()) {
          String version = matcher.group(1);
          String[] numbers = version.split("\\.");
          int num =
              Integer.parseInt(numbers[0]) * 10000
                  + Integer.parseInt(numbers[1]) * 100
                  + Integer.parseInt(numbers[2]);
          if (num > largestNumber) {
            largestNumber = num;
            latestVersion = version;
          }
        }
      }

      if (latestVersion == null) {
        throw new Exception("can't retrieve version number from Maven repo");
      } else if (!latestVersion.equals(VERSION)) {
        LOGGER.warn(
            "Connector update is available, please upgrade Snowflake Kafka Connector ({} -> {}) ",
            VERSION,
            latestVersion);
      }
    } catch (Exception e) {
      LOGGER.warn("can't verify latest connector version " + "from Maven Repo\n{}", e.getMessage());
      return false;
    }

    return true;
  }

  /**
   * @param appName connector name
   * @return connector object prefix
   */
  private static String getObjectPrefix(String appName) {
    return KAFKA_OBJECT_PREFIX + "_" + appName;
  }

  /**
   * generate stage name by given table
   *
   * @param appName connector name
   * @param table table name
   * @return stage name
   */
  public static String stageName(String appName, String table) {
    String stageName = getObjectPrefix(appName) + "_STAGE_" + table;

    LOGGER.debug("generated stage name: {}", stageName);

    return stageName;
  }

  /**
   * generate pipe name by given table and partition
   *
   * @param appName connector name
   * @param table table name
   * @param partition partition name
   * @return pipe name
   */
  public static String pipeName(String appName, String table, int partition) {
    String pipeName = getObjectPrefix(appName) + "_PIPE_" + table + "_" + partition;

    LOGGER.debug("generated pipe name: {}", pipeName);

    return pipeName;
  }

  /**
   * Read JDBC logging directory from environment variable JDBC_LOG_DIR and set that in System
   * property
   */
  public static void setJDBCLoggingDirectory() {
    String jdbcTmpDir = System.getenv(SnowflakeSinkConnectorConfig.SNOWFLAKE_JDBC_LOG_DIR);
    if (jdbcTmpDir != null) {
      File jdbcTmpDirObj = new File(jdbcTmpDir);
      if (jdbcTmpDirObj.isDirectory()) {
        LOGGER.info("jdbc tracing directory = {}", jdbcTmpDir);
        System.setProperty(JAVA_IO_TMPDIR, jdbcTmpDir);
      } else {
        LOGGER.info(
            "invalid JDBC_LOG_DIR {} defaulting to {}",
            jdbcTmpDir,
            System.getProperty(JAVA_IO_TMPDIR));
      }
    }
  }

  /**
   * validate whether proxy settings in the config is valid
   *
   * @param config connector configuration
   */
  static ImmutableMap<String, String> validateProxySettings(Map<String, String> config) {
    Map<String, String> invalidConfigParams = new HashMap<String, String>();

    String host =
        SnowflakeSinkConnectorConfig.getProperty(
            config, SnowflakeSinkConnectorConfig.JVM_PROXY_HOST);
    String port =
        SnowflakeSinkConnectorConfig.getProperty(
            config, SnowflakeSinkConnectorConfig.JVM_PROXY_PORT);

    // either both host and port are provided or none of them are provided
    if (host != null ^ port != null) {
      invalidConfigParams.put(
          SnowflakeSinkConnectorConfig.JVM_PROXY_HOST,
          "proxy host and port must be provided together");
      invalidConfigParams.put(
          SnowflakeSinkConnectorConfig.JVM_PROXY_PORT,
          "proxy host and port must be provided together");
    } else if (host != null) {
      String username =
          SnowflakeSinkConnectorConfig.getProperty(
              config, SnowflakeSinkConnectorConfig.JVM_PROXY_USERNAME);
      String password =
          SnowflakeSinkConnectorConfig.getProperty(
              config, SnowflakeSinkConnectorConfig.JVM_PROXY_PASSWORD);
      // either both username and password are provided or none of them are provided
      if (username != null ^ password != null) {
        invalidConfigParams.put(
            SnowflakeSinkConnectorConfig.JVM_PROXY_USERNAME,
            "proxy username and password must be provided together");
        invalidConfigParams.put(
            SnowflakeSinkConnectorConfig.JVM_PROXY_PASSWORD,
            "proxy username and password must be provided together");
      }
    }

    return ImmutableMap.copyOf(invalidConfigParams);
  }

  /**
   * Enable JVM proxy
   *
   * @param config connector configuration
   * @return false if wrong config
   */
  static boolean enableJVMProxy(Map<String, String> config) {
    String host =
        SnowflakeSinkConnectorConfig.getProperty(
            config, SnowflakeSinkConnectorConfig.JVM_PROXY_HOST);
    String port =
        SnowflakeSinkConnectorConfig.getProperty(
            config, SnowflakeSinkConnectorConfig.JVM_PROXY_PORT);
    String nonProxyHosts =
        SnowflakeSinkConnectorConfig.getProperty(
            config, SnowflakeSinkConnectorConfig.JVM_NON_PROXY_HOSTS);
    if (host != null && port != null) {
      LOGGER.info(
          "enable jvm proxy: {}:{} and bypass proxy for hosts: {}", host, port, nonProxyHosts);

      // enable https proxy
      System.setProperty(HTTP_USE_PROXY, "true");
      System.setProperty(HTTP_PROXY_HOST, host);
      System.setProperty(HTTP_PROXY_PORT, port);
      System.setProperty(HTTPS_PROXY_HOST, host);
      System.setProperty(HTTPS_PROXY_PORT, port);

      // If the user provided the jvm.nonProxy.hosts configuration then we
      // will append that to the list provided by the JVM argument
      // -Dhttp.nonProxyHosts and not override it altogether, if it exists.
      if (nonProxyHosts != null) {
        nonProxyHosts =
            (System.getProperty(HTTP_NON_PROXY_HOSTS) != null)
                ? System.getProperty(HTTP_NON_PROXY_HOSTS) + "|" + nonProxyHosts
                : nonProxyHosts;
        System.setProperty(HTTP_NON_PROXY_HOSTS, nonProxyHosts);
      }

      // set username and password
      String username =
          SnowflakeSinkConnectorConfig.getProperty(
              config, SnowflakeSinkConnectorConfig.JVM_PROXY_USERNAME);
      String password =
          SnowflakeSinkConnectorConfig.getProperty(
              config, SnowflakeSinkConnectorConfig.JVM_PROXY_PASSWORD);
      if (username != null && password != null) {
        Authenticator.setDefault(
            new Authenticator() {
              @Override
              public PasswordAuthentication getPasswordAuthentication() {
                return new PasswordAuthentication(username, password.toCharArray());
              }
            });
        System.setProperty(JDK_HTTP_AUTH_TUNNELING, "");
        System.setProperty(HTTP_PROXY_USER, username);
        System.setProperty(HTTP_PROXY_PASSWORD, password);
        System.setProperty(HTTPS_PROXY_USER, username);
        System.setProperty(HTTPS_PROXY_PASSWORD, password);
      }
    }

    return true;
  }

  /**
   * validates that given name is a valid snowflake object identifier
   *
   * @param objName snowflake object name
   * @return true if given object name is valid
   */
  static boolean isValidSnowflakeObjectIdentifier(String objName) {
    return objName.matches("^[_a-zA-Z]{1}[_$a-zA-Z0-9]+$");
  }

  /**
   * validates that given name is a valid snowflake application name, support '-'
   *
   * @param appName snowflake application name
   * @return true if given application name is valid
   */
  static boolean isValidSnowflakeApplicationName(String appName) {
    return appName.matches("^[-_a-zA-Z]{1}[-_$a-zA-Z0-9]+$");
  }

  static boolean isValidSnowflakeTableName(String tableName) {
    return tableName.matches("^([_a-zA-Z]{1}[_$a-zA-Z0-9]+\\.){0,2}[_a-zA-Z]{1}[_$a-zA-Z0-9]+$");
  }

  /**
   * Validate input configuration
   *
   * @param config configuration Map
   * @return connector name
   */
  static String validateConfig(Map<String, String> config) {
    Map<String, String> invalidConfigParams = new HashMap<String, String>(); // verify all config

    // define the input parameters / keys in one place as static constants,
    // instead of using them directly
    // define the thresholds statically in one place as static constants,
    // instead of using the values directly

    // unique name of this connector instance
    String connectorName = config.getOrDefault(SnowflakeSinkConnectorConfig.NAME, "");
    if (connectorName.isEmpty() || !isValidSnowflakeApplicationName(connectorName)) {
      invalidConfigParams.put(
          SnowflakeSinkConnectorConfig.NAME,
          Utils.formatString(
              "{} is empty or invalid. It should match Snowflake object identifier syntax. Please"
                  + " see the documentation.",
              SnowflakeSinkConnectorConfig.NAME));
    }

    // If config doesnt have ingestion method defined, default is snowpipe or if snowpipe is
    // explicitly passed in as ingestion method
    // Below checks are just for snowpipe.
    if (!config.containsKey(INGESTION_METHOD_OPT)
        || config
            .get(INGESTION_METHOD_OPT)
            .equalsIgnoreCase(IngestionMethodConfig.SNOWPIPE.toString())) {
      invalidConfigParams.putAll(
          BufferThreshold.validateBufferThreshold(config, IngestionMethodConfig.SNOWPIPE));

      if (config.containsKey(SnowflakeSinkConnectorConfig.ENABLE_SCHEMATIZATION_CONFIG)
          && Boolean.parseBoolean(
              config.get(SnowflakeSinkConnectorConfig.ENABLE_SCHEMATIZATION_CONFIG))) {
        invalidConfigParams.put(
            SnowflakeSinkConnectorConfig.ENABLE_SCHEMATIZATION_CONFIG,
            Utils.formatString(
                "Schematization is only available with {}.",
                IngestionMethodConfig.SNOWPIPE_STREAMING.toString()));
      }
      if (config.containsKey(SnowflakeSinkConnectorConfig.SNOWPIPE_STREAMING_FILE_VERSION)) {
        invalidConfigParams.put(
            SnowflakeSinkConnectorConfig.SNOWPIPE_STREAMING_FILE_VERSION,
            Utils.formatString(
                "{} is only available with ingestion type: {}.",
                SnowflakeSinkConnectorConfig.SNOWPIPE_STREAMING_FILE_VERSION,
                IngestionMethodConfig.SNOWPIPE_STREAMING.toString()));
      }
      if (config.containsKey(
              SnowflakeSinkConnectorConfig.ENABLE_STREAMING_CLIENT_OPTIMIZATION_CONFIG)
          && Boolean.parseBoolean(
              config.get(
                  SnowflakeSinkConnectorConfig.ENABLE_STREAMING_CLIENT_OPTIMIZATION_CONFIG))) {
        invalidConfigParams.put(
            SnowflakeSinkConnectorConfig.ENABLE_STREAMING_CLIENT_OPTIMIZATION_CONFIG,
            Utils.formatString(
                "Streaming client optimization is only available with {}.",
                IngestionMethodConfig.SNOWPIPE_STREAMING.toString()));
      }
    }

    if (config.containsKey(SnowflakeSinkConnectorConfig.TOPICS_TABLES_MAP)
        && parseTopicToTableMap(config.get(SnowflakeSinkConnectorConfig.TOPICS_TABLES_MAP))
            == null) {
      invalidConfigParams.put(
          SnowflakeSinkConnectorConfig.TOPICS_TABLES_MAP,
          Utils.formatString(
              "Invalid {} config format: {}",
              SnowflakeSinkConnectorConfig.TOPICS_TABLES_MAP,
              config.get(SnowflakeSinkConnectorConfig.TOPICS_TABLES_MAP)));
    }

    // sanity check
    if (!config.containsKey(SnowflakeSinkConnectorConfig.SNOWFLAKE_DATABASE)) {
      invalidConfigParams.put(
          SnowflakeSinkConnectorConfig.SNOWFLAKE_DATABASE,
          Utils.formatString(
              "{} cannot be empty.", SnowflakeSinkConnectorConfig.SNOWFLAKE_DATABASE));
    }

    // sanity check
    if (!config.containsKey(SnowflakeSinkConnectorConfig.SNOWFLAKE_SCHEMA)) {
      invalidConfigParams.put(
          SnowflakeSinkConnectorConfig.SNOWFLAKE_SCHEMA,
          Utils.formatString("{} cannot be empty.", SnowflakeSinkConnectorConfig.SNOWFLAKE_SCHEMA));
    }

    switch (config
        .getOrDefault(SnowflakeSinkConnectorConfig.AUTHENTICATOR_TYPE, Utils.SNOWFLAKE_JWT)
        .toLowerCase()) {
        // TODO: SNOW-889748 change to enum
      case Utils.SNOWFLAKE_JWT:
        if (!config.containsKey(SnowflakeSinkConnectorConfig.SNOWFLAKE_PRIVATE_KEY)) {
          invalidConfigParams.put(
              SnowflakeSinkConnectorConfig.SNOWFLAKE_PRIVATE_KEY,
              Utils.formatString(
                  "{} cannot be empty when using {} authenticator.",
                  SnowflakeSinkConnectorConfig.SNOWFLAKE_PRIVATE_KEY,
                  Utils.SNOWFLAKE_JWT));
        }
        break;
      case Utils.OAUTH:
        if (!config.containsKey(SnowflakeSinkConnectorConfig.OAUTH_CLIENT_ID)) {
          invalidConfigParams.put(
              SnowflakeSinkConnectorConfig.OAUTH_CLIENT_ID,
              Utils.formatString(
                  "{} cannot be empty when using {} authenticator.",
                  SnowflakeSinkConnectorConfig.OAUTH_CLIENT_ID,
                  Utils.OAUTH));
        }
        if (!config.containsKey(SnowflakeSinkConnectorConfig.OAUTH_CLIENT_SECRET)) {
          invalidConfigParams.put(
              SnowflakeSinkConnectorConfig.OAUTH_CLIENT_SECRET,
              Utils.formatString(
                  "{} cannot be empty when using {} authenticator.",
                  SnowflakeSinkConnectorConfig.OAUTH_CLIENT_SECRET,
                  Utils.OAUTH));
        }
        if (!config.containsKey(SnowflakeSinkConnectorConfig.OAUTH_REFRESH_TOKEN)) {
          invalidConfigParams.put(
              SnowflakeSinkConnectorConfig.OAUTH_REFRESH_TOKEN,
              Utils.formatString(
                  "{} cannot be empty when using {} authenticator.",
                  SnowflakeSinkConnectorConfig.OAUTH_REFRESH_TOKEN,
                  Utils.OAUTH));
        }
        break;
      default:
        invalidConfigParams.put(
            SnowflakeSinkConnectorConfig.AUTHENTICATOR_TYPE,
            Utils.formatString(
                "{} should be one of {} or {}.",
                SnowflakeSinkConnectorConfig.AUTHENTICATOR_TYPE,
                Utils.SNOWFLAKE_JWT,
                Utils.OAUTH));
    }

    if (!config.containsKey(SnowflakeSinkConnectorConfig.SNOWFLAKE_USER)) {
      invalidConfigParams.put(
          SnowflakeSinkConnectorConfig.SNOWFLAKE_USER,
          Utils.formatString("{} cannot be empty.", SnowflakeSinkConnectorConfig.SNOWFLAKE_USER));
    }

    if (!config.containsKey(SnowflakeSinkConnectorConfig.SNOWFLAKE_URL)) {
      invalidConfigParams.put(
          SnowflakeSinkConnectorConfig.SNOWFLAKE_URL,
          Utils.formatString("{} cannot be empty.", SnowflakeSinkConnectorConfig.SNOWFLAKE_URL));
    }
    // jvm proxy settings
    invalidConfigParams.putAll(validateProxySettings(config));

    // set jdbc logging directory
    Utils.setJDBCLoggingDirectory();

    // validate whether kafka provider config is a valid value
    if (config.containsKey(SnowflakeSinkConnectorConfig.PROVIDER_CONFIG)) {
      try {
        SnowflakeSinkConnectorConfig.KafkaProvider.of(
            config.get(SnowflakeSinkConnectorConfig.PROVIDER_CONFIG));
      } catch (IllegalArgumentException exception) {
        invalidConfigParams.put(
            SnowflakeSinkConnectorConfig.PROVIDER_CONFIG,
            Utils.formatString("Kafka provider config error:{}", exception.getMessage()));
      }
    }

    if (config.containsKey(BEHAVIOR_ON_NULL_VALUES_CONFIG)) {
      try {
        // This throws an exception if config value is invalid.
        VALIDATOR.ensureValid(
            BEHAVIOR_ON_NULL_VALUES_CONFIG, config.get(BEHAVIOR_ON_NULL_VALUES_CONFIG));
      } catch (ConfigException exception) {
        invalidConfigParams.put(
            BEHAVIOR_ON_NULL_VALUES_CONFIG,
            Utils.formatString(
                "Kafka config:{} error:{}",
                BEHAVIOR_ON_NULL_VALUES_CONFIG,
                exception.getMessage()));
      }
    }

    if (config.containsKey(JMX_OPT)) {
      if (!(config.get(JMX_OPT).equalsIgnoreCase("true")
          || config.get(JMX_OPT).equalsIgnoreCase("false"))) {
        invalidConfigParams.put(
            JMX_OPT, Utils.formatString("Kafka config:{} should either be true or false", JMX_OPT));
      }
    }

    // Check all config values for ingestion method == IngestionMethodConfig.SNOWPIPE_STREAMING
    invalidConfigParams.putAll(StreamingUtils.validateStreamingSnowpipeConfig(config));

    // logs and throws exception if there are invalid params
    handleInvalidParameters(ImmutableMap.copyOf(invalidConfigParams));

    return connectorName;
  }

  /**
   * modify invalid application name in config and return the generated application name
   *
   * @param config input config object
   */
  public static void convertAppName(Map<String, String> config) {
    String appName = config.getOrDefault(SnowflakeSinkConnectorConfig.NAME, "");
    // If appName is empty the following call will throw error
    String validAppName = generateValidName(appName, new HashMap<String, String>());

    config.put(SnowflakeSinkConnectorConfig.NAME, validAppName);
  }

  /**
   * verify topic name, and generate valid table name
   *
   * @param topic input topic name
   * @param topic2table topic to table map
   * @return valid table name
   */
  public static String tableName(String topic, Map<String, String> topic2table) {
    return generateValidName(topic, topic2table);
  }

  /**
   * verify topic name, and generate valid table/application name
   *
   * @param topic input topic name
   * @param topic2table topic to table map
   * @return valid table/application name
   */
  public static String generateValidName(String topic, Map<String, String> topic2table) {
    final String PLACE_HOLDER = "_";
    if (topic == null || topic.isEmpty()) {
      throw SnowflakeErrors.ERROR_0020.getException("topic name: " + topic);
    }
    if (topic2table.containsKey(topic)) {
      return topic2table.get(topic);
    }

    // try matching regex tables
    for (String regexTopic : topic2table.keySet()) {
      if (topic.matches(regexTopic)) {
        return topic2table.get(regexTopic);
      }
    }

    if (Utils.isValidSnowflakeObjectIdentifier(topic)) {
      return topic;
    }

    for (Map.Entry<String, String> entry : topic2table.entrySet()) {
      if (entry.getKey().startsWith(TOPIC_MATCHER_PREFIX)) {
        String regex = entry.getKey().replaceFirst(TOPIC_MATCHER_PREFIX, "");
        String finalTableName = topic.replaceAll(regex, entry.getValue());
        if (Utils.isValidSnowflakeObjectIdentifier(finalTableName)) {
          topic2table.put(topic, finalTableName);
          return finalTableName;
        }
      }
    }

    int hash = Math.abs(topic.hashCode());

    StringBuilder result = new StringBuilder();

    // remove wildcard regex from topic name to generate table name
    topic = topic.replaceAll("\\.\\*", "");

    int index = 0;
    // first char
    if (topic.substring(index, index + 1).matches("[_a-zA-Z]")) {
      result.append(topic.charAt(0));
      index++;
    } else {
      result.append(PLACE_HOLDER);
    }
    while (index < topic.length()) {
      if (topic.substring(index, index + 1).matches("[_$a-zA-Z0-9]")) {
        result.append(topic.charAt(index));
      } else {
        result.append(PLACE_HOLDER);
      }
      index++;
    }

    result.append(PLACE_HOLDER);
    result.append(hash);

    return result.toString();
  }

  public static String TOPIC_MATCHER_PREFIX = "REGEX_MATCHER>";
  public static Map<String, String> parseTopicToTableMap(String input) {
    Map<String, String> topic2Table = new HashMap<>();
    boolean isInvalid = false;
    for (String str : input.split(",")) {
      String[] tt = str.split(":");

      if (tt.length != 2 || tt[0].trim().isEmpty() || tt[1].trim().isEmpty()) {
        LOGGER.error(
            "Invalid {} config format: {}", SnowflakeSinkConnectorConfig.TOPICS_TABLES_MAP, input);
        return null;
      }

      String topic = tt[0].trim();
      String table = tt[1].trim();

      if (!topic.startsWith(TOPIC_MATCHER_PREFIX)) {
        if (!isValidSnowflakeTableName(table)) {
          LOGGER.error(
              "table name {} should have at least 2 "
                  + "characters, start with _a-zA-Z, and only contains "
                  + "_$a-zA-z0-9",
              table);
          isInvalid = true;
        }
      }

      if (topic2Table.containsKey(topic)) {
        LOGGER.error("topic name {} is duplicated", topic);
        isInvalid = true;
      }

      // check that regexes don't overlap
      for (String parsedTopic : topic2Table.keySet()) {
        if (parsedTopic.matches(topic) || topic.matches(parsedTopic)) {
          LOGGER.error(
              "topic regexes cannot overlap. overlapping regexes: {}, {}", parsedTopic, topic);
          isInvalid = true;
        }
      }

      topic2Table.put(tt[0].trim(), tt[1].trim());
    }
    if (isInvalid) {
      throw SnowflakeErrors.ERROR_0021.getException();
    }
    return topic2Table;
  }

  static final String loginPropList[] = {SF_URL, SF_USER, SF_SCHEMA, SF_DATABASE};

  public static boolean isSingleFieldValid(Config result) {
    // if any single field validation failed
    for (ConfigValue v : result.configValues()) {
      if (!v.errorMessages().isEmpty()) {
        return false;
      }
    }
    // if any of url, user, schema, database or password is empty
    // update error message and return false
    boolean isValidate = true;
    final String errorMsg = " must be provided";
    Map<String, ConfigValue> validateMap = validateConfigToMap(result);
    //
    for (String prop : loginPropList) {
      if (validateMap.get(prop).value() == null) {
        updateConfigErrorMessage(result, prop, errorMsg);
        isValidate = false;
      }
    }

    return isValidate;
  }

  public static Map<String, ConfigValue> validateConfigToMap(final Config result) {
    Map<String, ConfigValue> validateMap = new HashMap<>();
    for (ConfigValue v : result.configValues()) {
      validateMap.put(v.name(), v);
    }
    return validateMap;
  }

  public static void updateConfigErrorMessage(Config result, String key, String msg) {
    for (ConfigValue v : result.configValues()) {
      if (v.name().equals(key)) {
        v.addErrorMessage(key + msg);
      }
    }
  }

  // static elements
  // log message tag
  static final String SF_LOG_TAG = "[SF_KAFKA_CONNECTOR]";

  /**
   * the following method wraps log messages with Snowflake tag. For example,
   *
   * <p>[SF_KAFKA_CONNECTOR] this is a log message
   *
   * <p>[SF_KAFKA_CONNECTOR] this is the second line
   *
   * <p>All log messages should be wrapped by Snowflake tag. Then user can filter out log messages
   * output from Snowflake Kafka connector by these tags.
   *
   * @param format log message format string
   * @param vars variable list
   * @return log message wrapped by snowflake tag
   */
  public static String formatLogMessage(String format, Object... vars) {
    return SF_LOG_TAG + " " + formatString(format, vars);
  }

  public static String formatString(String format, Object... vars) {
    for (int i = 0; i < vars.length; i++) {
      format = format.replaceFirst("\\{}", Objects.toString(vars[i]).replaceAll("\\$", "\\\\\\$"));
    }
    return format;
  }

  /**
   * Get OAuth access token given refresh token
   *
   * @param url OAuth server url
   * @param clientId OAuth clientId
   * @param clientSecret OAuth clientSecret
   * @param refreshToken OAuth refresh token
   * @return OAuth access token
   */
  public static String getSnowflakeOAuthAccessToken(
      SnowflakeURL url, String clientId, String clientSecret, String refreshToken) {
    return getSnowflakeOAuthToken(
        url,
        clientId,
        clientSecret,
        refreshToken,
        OAuthConstants.REFRESH_TOKEN,
        OAuthConstants.REFRESH_TOKEN,
        OAuthConstants.ACCESS_TOKEN);
  }

  /**
   * Get OAuth token given integration info <a
   * href="https://docs.snowflake.com/en/user-guide/oauth-snowflake-overview">Snowflake OAuth
   * Overview</a>
   *
   * @param url OAuth server url
   * @param clientId OAuth clientId
   * @param clientSecret OAuth clientSecret
   * @param credential OAuth credential, either az code or refresh token
   * @param grantType OAuth grant type, either authorization_code or refresh_token
   * @param credentialType OAuth credential key, either code or refresh_token
   * @param tokenType type of OAuth token to get, either access_token or refresh_token
   * @return OAuth token
   */
  // TODO: SNOW-895296 Integrate OAuth utils with streaming ingest SDK
  public static String getSnowflakeOAuthToken(
      SnowflakeURL url,
      String clientId,
      String clientSecret,
      String credential,
      String grantType,
      String credentialType,
      String tokenType) {
    Map<String, String> headers = new HashMap<>();
    headers.put(HttpHeaders.CONTENT_TYPE, OAuthConstants.OAUTH_CONTENT_TYPE_HEADER);
    headers.put(
        HttpHeaders.AUTHORIZATION,
        OAuthConstants.BASIC_AUTH_HEADER_PREFIX
            + Base64.getEncoder().encodeToString((clientId + ":" + clientSecret).getBytes()));

    Map<String, String> payload = new HashMap<>();
    payload.put(OAuthConstants.GRANT_TYPE_PARAM, grantType);
    payload.put(credentialType, credential);
    payload.put(OAuthConstants.REDIRECT_URI, OAuthConstants.DEFAULT_REDIRECT_URI);

    // Encode and convert payload into string entity
    String payloadString =
        payload.entrySet().stream()
            .map(
                e -> {
                  try {
                    return e.getKey() + "=" + URLEncoder.encode(e.getValue(), "UTF-8");
                  } catch (UnsupportedEncodingException ex) {
                    throw SnowflakeErrors.ERROR_1004.getException(ex);
                  }
                })
            .collect(Collectors.joining("&"));
    final StringEntity entity =
        new StringEntity(payloadString, ContentType.APPLICATION_FORM_URLENCODED);

    HttpPost post =
        buildOAuthHttpPostRequest(url, OAuthConstants.TOKEN_REQUEST_ENDPOINT, headers, entity);

    // Request access token
    CloseableHttpClient client = HttpClientBuilder.create().build();
    try {
      return InternalUtils.backoffAndRetry(
              null,
              SnowflakeInternalOperations.FETCH_OAUTH_TOKEN,
              () -> {
                try (CloseableHttpResponse httpResponse = client.execute(post)) {
                  String respBodyString = EntityUtils.toString(httpResponse.getEntity());
                  JsonObject respBody = JsonParser.parseString(respBodyString).getAsJsonObject();
                  // Trim surrounding quotation marks
                  return respBody.get(tokenType).toString().replaceAll("^\"|\"$", "");
                } catch (Exception e) {
                  throw SnowflakeErrors.ERROR_1004.getException(
                      "Failed to get Oauth access token after retries");
                }
              })
          .toString();
    } catch (Exception e) {
      throw SnowflakeErrors.ERROR_1004.getException(e);
    }
  }

  /**
   * Build OAuth http post request base on headers and payload
   *
   * @param url target url
   * @param headers headers key value pairs
   * @param entity payload entity
   * @return HttpPost request for OAuth
   */
  public static HttpPost buildOAuthHttpPostRequest(
      SnowflakeURL url, String path, Map<String, String> headers, StringEntity entity) {
    // Build post request
    URI uri;
    try {
      uri =
          new URIBuilder().setHost(url.toString()).setScheme(url.getScheme()).setPath(path).build();
    } catch (URISyntaxException e) {
      throw SnowflakeErrors.ERROR_1004.getException(e);
    }

    // Add headers
    HttpPost post = new HttpPost(uri);
    for (Map.Entry<String, String> e : headers.entrySet()) {
      post.addHeader(e.getKey(), e.getValue());
    }

    post.setEntity(entity);

    return post;
  }

  /**
   * Get the message and cause of a missing exception, handling the null or empty cases of each
   *
   * @param customMessage A custom message to prepend to the exception
   * @param ex The message to parse through
   * @return A string with the custom message and the exceptions message or cause, if exists
   */
  public static String getExceptionMessage(String customMessage, Exception ex) {
    String message =
        ex.getMessage() == null || ex.getMessage().isEmpty()
            ? GET_EXCEPTION_MISSING_MESSAGE
            : ex.getMessage();
    String cause =
        ex.getCause() == null || ex.getCause().getStackTrace() == null
            ? GET_EXCEPTION_MISSING_CAUSE
            : Arrays.toString(ex.getCause().getStackTrace());

    return formatString(GET_EXCEPTION_FORMAT, customMessage, message, cause);
  }

  private static void handleInvalidParameters(ImmutableMap<String, String> invalidConfigParams) {
    // log all invalid params and throw exception
    if (!invalidConfigParams.isEmpty()) {
      String invalidParamsMessage = "";

      for (String invalidKey : invalidConfigParams.keySet()) {
        String invalidValue = invalidConfigParams.get(invalidKey);
        String errorMessage =
            Utils.formatString(
                "Config value '{}' is invalid. Error message: '{}'", invalidKey, invalidValue);
        invalidParamsMessage += errorMessage + "\n";
      }

      LOGGER.error("Invalid config: " + invalidParamsMessage);
      throw SnowflakeErrors.ERROR_0001.getException(invalidParamsMessage);
    }
  }
}
