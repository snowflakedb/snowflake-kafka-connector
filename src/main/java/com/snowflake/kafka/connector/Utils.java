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
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.DELIVERY_GUARANTEE;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.INGESTION_METHOD_OPT;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.JMX_OPT;

import com.snowflake.kafka.connector.internal.BufferThreshold;
import com.snowflake.kafka.connector.internal.LoggerHandler;
import com.snowflake.kafka.connector.internal.SnowflakeErrors;
import com.snowflake.kafka.connector.internal.SnowflakeKafkaConnectorException;
import com.snowflake.kafka.connector.internal.streaming.IngestionMethodConfig;
import com.snowflake.kafka.connector.internal.streaming.StreamingUtils;
import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.Authenticator;
import java.net.PasswordAuthentication;
import java.net.URL;
import java.net.URLConnection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigValue;

/** Various arbitrary helper functions */
public class Utils {

  // Connector version, change every release
  public static final String VERSION = "1.9.1";

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

  private static final LoggerHandler LOGGER = new LoggerHandler(Utils.class.getName());

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
  static void validateProxySetting(Map<String, String> config) {
    String host =
        SnowflakeSinkConnectorConfig.getProperty(
            config, SnowflakeSinkConnectorConfig.JVM_PROXY_HOST);
    String port =
        SnowflakeSinkConnectorConfig.getProperty(
            config, SnowflakeSinkConnectorConfig.JVM_PROXY_PORT);
    // either both host and port are provided or none of them are provided
    if (host != null ^ port != null) {
      throw SnowflakeErrors.ERROR_0022.getException(
          SnowflakeSinkConnectorConfig.JVM_PROXY_HOST
              + " and "
              + SnowflakeSinkConnectorConfig.JVM_PROXY_PORT
              + " must be provided together");
    } else if (host != null) {
      String username =
          SnowflakeSinkConnectorConfig.getProperty(
              config, SnowflakeSinkConnectorConfig.JVM_PROXY_USERNAME);
      String password =
          SnowflakeSinkConnectorConfig.getProperty(
              config, SnowflakeSinkConnectorConfig.JVM_PROXY_PASSWORD);
      // either both username and password are provided or none of them are provided
      if (username != null ^ password != null) {
        throw SnowflakeErrors.ERROR_0023.getException(
            SnowflakeSinkConnectorConfig.JVM_PROXY_USERNAME
                + " and "
                + SnowflakeSinkConnectorConfig.JVM_PROXY_PASSWORD
                + " must be provided together");
      }
    }
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
    if (host != null && port != null) {
      LOGGER.info("enable jvm proxy: {}:{}", host, port);

      // enable https proxy
      System.setProperty(HTTP_USE_PROXY, "true");
      System.setProperty(HTTP_PROXY_HOST, host);
      System.setProperty(HTTP_PROXY_PORT, port);
      System.setProperty(HTTPS_PROXY_HOST, host);
      System.setProperty(HTTPS_PROXY_PORT, port);

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
    boolean configIsValid = true; // verify all config

    // define the input parameters / keys in one place as static constants,
    // instead of using them directly
    // define the thresholds statically in one place as static constants,
    // instead of using the values directly

    // unique name of this connector instance
    String connectorName = config.getOrDefault(SnowflakeSinkConnectorConfig.NAME, "");
    if (connectorName.isEmpty() || !isValidSnowflakeApplicationName(connectorName)) {
      LOGGER.error(
          "{} is empty or invalid. It should match Snowflake object identifier syntax. Please see "
              + "the documentation.",
          SnowflakeSinkConnectorConfig.NAME);
      configIsValid = false;
    }

    // If config doesnt have ingestion method defined, default is snowpipe or if snowpipe is
    // explicitly passed in as ingestion method
    // Below checks are just for snowpipe.
    if (!config.containsKey(INGESTION_METHOD_OPT)
        || config
            .get(INGESTION_METHOD_OPT)
            .equalsIgnoreCase(IngestionMethodConfig.SNOWPIPE.toString())) {
      if (!BufferThreshold.validateBufferThreshold(config, IngestionMethodConfig.SNOWPIPE)) {
        configIsValid = false;
      }

      if (config.containsKey(SnowflakeSinkConnectorConfig.ENABLE_SCHEMATIZATION_CONFIG)
          && Boolean.parseBoolean(
              config.get(SnowflakeSinkConnectorConfig.ENABLE_SCHEMATIZATION_CONFIG))) {
        configIsValid = false;
        LOGGER.error(
            "Schematization is only available with {}.",
            IngestionMethodConfig.SNOWPIPE_STREAMING.toString());
      }
      if (config.containsKey(SnowflakeSinkConnectorConfig.SNOWPIPE_STREAMING_FILE_VERSION)) {
        configIsValid = false;
        LOGGER.error(
            "{} is only available with ingestion type: {}.",
            SnowflakeSinkConnectorConfig.SNOWPIPE_STREAMING_FILE_VERSION,
            IngestionMethodConfig.SNOWPIPE_STREAMING.toString());
      }
    }

    if (config.containsKey(SnowflakeSinkConnectorConfig.TOPICS_TABLES_MAP)
        && parseTopicToTableMap(config.get(SnowflakeSinkConnectorConfig.TOPICS_TABLES_MAP))
            == null) {
      configIsValid = false;
    }

    // sanity check
    if (!config.containsKey(SnowflakeSinkConnectorConfig.SNOWFLAKE_DATABASE)) {
      LOGGER.error("{} cannot be empty.", SnowflakeSinkConnectorConfig.SNOWFLAKE_DATABASE);
      configIsValid = false;
    }

    // sanity check
    if (!config.containsKey(SnowflakeSinkConnectorConfig.SNOWFLAKE_SCHEMA)) {
      LOGGER.error("{} cannot be empty.", SnowflakeSinkConnectorConfig.SNOWFLAKE_SCHEMA);
      configIsValid = false;
    }

    if (!config.containsKey(SnowflakeSinkConnectorConfig.SNOWFLAKE_PRIVATE_KEY)) {
      LOGGER.error("{} cannot be empty.", SnowflakeSinkConnectorConfig.SNOWFLAKE_PRIVATE_KEY);
      configIsValid = false;
    }

    if (!config.containsKey(SnowflakeSinkConnectorConfig.SNOWFLAKE_USER)) {
      LOGGER.error("{} cannot be empty.", SnowflakeSinkConnectorConfig.SNOWFLAKE_USER);
      configIsValid = false;
    }

    if (!config.containsKey(SnowflakeSinkConnectorConfig.SNOWFLAKE_URL)) {
      LOGGER.error("{} cannot be empty.", SnowflakeSinkConnectorConfig.SNOWFLAKE_URL);
      configIsValid = false;
    }
    // jvm proxy settings
    try {
      validateProxySetting(config);
    } catch (SnowflakeKafkaConnectorException e) {
      LOGGER.error("Proxy settings error: ", e.getMessage());
      configIsValid = false;
    }

    // set jdbc logging directory
    Utils.setJDBCLoggingDirectory();

    // validate whether kafka provider config is a valid value
    if (config.containsKey(SnowflakeSinkConnectorConfig.PROVIDER_CONFIG)) {
      try {
        SnowflakeSinkConnectorConfig.KafkaProvider.of(
            config.get(SnowflakeSinkConnectorConfig.PROVIDER_CONFIG));
      } catch (IllegalArgumentException exception) {
        LOGGER.error("Kafka provider config error:{}", exception.getMessage());
        configIsValid = false;
      }
    }

    if (config.containsKey(BEHAVIOR_ON_NULL_VALUES_CONFIG)) {
      try {
        // This throws an exception if config value is invalid.
        VALIDATOR.ensureValid(
            BEHAVIOR_ON_NULL_VALUES_CONFIG, config.get(BEHAVIOR_ON_NULL_VALUES_CONFIG));
      } catch (ConfigException exception) {
        LOGGER.error(
            "Kafka config:{} error:{}", BEHAVIOR_ON_NULL_VALUES_CONFIG, exception.getMessage());
        configIsValid = false;
      }
    }

    if (config.containsKey(JMX_OPT)) {
      if (!(config.get(JMX_OPT).equalsIgnoreCase("true")
          || config.get(JMX_OPT).equalsIgnoreCase("false"))) {
        LOGGER.error("Kafka config:{} should either be true or false", JMX_OPT);
        configIsValid = false;
      }
    }

    try {
      SnowflakeSinkConnectorConfig.IngestionDeliveryGuarantee.of(
          config.getOrDefault(
              DELIVERY_GUARANTEE,
              SnowflakeSinkConnectorConfig.IngestionDeliveryGuarantee.AT_LEAST_ONCE.name()));
    } catch (IllegalArgumentException exception) {
      LOGGER.error(
          "Delivery Guarantee config:{} error:{}", DELIVERY_GUARANTEE, exception.getMessage());
      configIsValid = false;
    }

    // Check all config values for ingestion method == IngestionMethodConfig.SNOWPIPE_STREAMING
    final boolean isStreamingConfigValid = StreamingUtils.isStreamingSnowpipeConfigValid(config);

    if (!configIsValid || !isStreamingConfigValid) {
      throw SnowflakeErrors.ERROR_0001.getException();
    }

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
    if (Utils.isValidSnowflakeObjectIdentifier(topic)) {
      return topic;
    }
    int hash = Math.abs(topic.hashCode());

    StringBuilder result = new StringBuilder();

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

      if (!isValidSnowflakeTableName(table)) {
        LOGGER.error(
            "table name {} should have at least 2 "
                + "characters, start with _a-zA-Z, and only contains "
                + "_$a-zA-z0-9",
            table);
        isInvalid = true;
      }

      if (topic2Table.containsKey(topic)) {
        LOGGER.error("topic name {} is duplicated", topic);
        isInvalid = true;
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
}
