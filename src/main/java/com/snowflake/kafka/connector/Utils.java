/*
 * Copyright (c) 2024 Snowflake Inc. All rights reserved.
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

import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.SNOWFLAKE_STREAMING_ICEBERG_ENABLED;

import com.google.common.collect.ImmutableMap;
import com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams;
import com.snowflake.kafka.connector.internal.KCLogger;
import com.snowflake.kafka.connector.internal.SnowflakeErrors;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.Authenticator;
import java.net.PasswordAuthentication;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigValue;

/** Various arbitrary helper functions */
public class Utils {

  // Connector version, change every release
  public static final String VERSION = "4.0.0-rc4";

  // constants strings
  private static final String KAFKA_OBJECT_PREFIX = "SNOWFLAKE_KAFKA_CONNECTOR";

  // task id
  public static final String TASK_ID = "task_id";

  public static final String JDK_HTTP_AUTH_TUNNELING = "jdk.http.auth.tunneling.disabledSchemes";

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
   * Check the connector version from Maven repo, report if any update version is available.
   *
   * <p>A URl connection timeout is added in case Maven repo is not reachable in a proxy'd
   * environment. Returning false from this method doesn't have any side effects to start the
   * connector.
   *
   * <p>Version upgrade logic:
   *
   * <ul>
   *   <li>Suggest only version that is newer than current version. If many new versions available
   *       suggest the most recent one.
   *   <li>Never suggest RC (release candidate) versions
   * </ul>
   */
  public static boolean checkConnectorVersion() {
    return checkConnectorVersion(VERSION, fetchAvailableVersionsFromMaven());
  }

  /**
   * Check connector version with provided current version and available versions.
   *
   * @param currentVersionString current version string
   * @param availableVersions list of available version strings from Maven
   */
  static boolean checkConnectorVersion(
      String currentVersionString, List<String> availableVersions) {
    LOGGER.info("Current Snowflake Kafka Connector Version: {}", currentVersionString);
    try {
      SemanticVersion currentVersion = new SemanticVersion(currentVersionString);
      String recommendedVersion = findRecommendedVersion(currentVersion, availableVersions);

      if (recommendedVersion != null) {
        LOGGER.warn(
            "Connector update is available, please upgrade Snowflake Kafka Connector ({} -> {})."
                + " Please check release notes for breaking changes and upgrade procedures before"
                + " installing.",
            currentVersionString,
            recommendedVersion);
      }
      return true;
    } catch (Exception e) {
      LOGGER.warn("can't verify latest connector version\n{}", e.getMessage());
    }
    return false;
  }

  /**
   * Fetch available versions from Maven repository.
   *
   * @return list of available version strings
   */
  static List<String> fetchAvailableVersionsFromMaven() {
    List<String> versions = new ArrayList<>();
    try {
      URLConnection urlConnection = new URL(MVN_REPO).openConnection();
      urlConnection.setConnectTimeout(5000);
      urlConnection.setReadTimeout(5000);
      InputStream input = urlConnection.getInputStream();
      BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(input));

      String line;
      Pattern pattern = Pattern.compile("(\\d+\\.\\d+\\.\\d+(?:-[rR][cC]\\d*)?)");

      while ((line = bufferedReader.readLine()) != null) {
        Matcher matcher = pattern.matcher(line);
        if (matcher.find()) {
          versions.add(matcher.group(1));
        }
      }
    } catch (Exception e) {
      LOGGER.warn("Failed to fetch versions from Maven: {}", e.getMessage());
    }
    return versions;
  }

  /**
   * Find the recommended version to upgrade to based on current version and available versions.
   * Package-private for testing.
   *
   * @param currentVersion the current connector version
   * @param availableVersions list of available version strings
   * @return recommended version string, or null if no upgrade is recommended
   */
  static String findRecommendedVersion(
      SemanticVersion currentVersion, List<String> availableVersions) {
    SemanticVersion highestCompatibleVersion = null;

    for (String versionString : availableVersions) {
      try {
        SemanticVersion version = new SemanticVersion(versionString);

        // Skip RC versions
        if (version.isReleaseCandidate()) {
          continue;
        }

        // Skip versions that are not greater than current
        if (version.compareTo(currentVersion) <= 0) {
          continue;
        }

        // Track the highest compatible version
        if (highestCompatibleVersion == null || version.compareTo(highestCompatibleVersion) > 0) {
          highestCompatibleVersion = version;
        }
      } catch (IllegalArgumentException e) {
        LOGGER.warn("Could not parse version string {}", versionString, e);
      }
    }

    return highestCompatibleVersion != null ? highestCompatibleVersion.toString() : null;
  }

  /**
   * validate whether proxy settings in the config is valid
   *
   * @param config connector configuration
   */
  public static ImmutableMap<String, String> validateProxySettings(Map<String, String> config) {
    Map<String, String> invalidConfigParams = new HashMap<String, String>();

    String host =
        ConnectorConfigTools.getProperty(config, KafkaConnectorConfigParams.JVM_PROXY_HOST);
    String port =
        ConnectorConfigTools.getProperty(config, KafkaConnectorConfigParams.JVM_PROXY_PORT);

    // either both host and port are provided or none of them are provided
    if (host != null ^ port != null) {
      invalidConfigParams.put(
          KafkaConnectorConfigParams.JVM_PROXY_HOST,
          "proxy host and port must be provided together");
      invalidConfigParams.put(
          KafkaConnectorConfigParams.JVM_PROXY_PORT,
          "proxy host and port must be provided together");
    } else if (host != null) {
      String username =
          ConnectorConfigTools.getProperty(config, KafkaConnectorConfigParams.JVM_PROXY_USERNAME);
      String password =
          ConnectorConfigTools.getProperty(config, KafkaConnectorConfigParams.JVM_PROXY_PASSWORD);
      // either both username and password are provided or none of them are provided
      if (username != null ^ password != null) {
        invalidConfigParams.put(
            KafkaConnectorConfigParams.JVM_PROXY_USERNAME,
            "proxy username and password must be provided together");
        invalidConfigParams.put(
            KafkaConnectorConfigParams.JVM_PROXY_PASSWORD,
            "proxy username and password must be provided together");
      }
    }

    return ImmutableMap.copyOf(invalidConfigParams);
  }

  /**
   * Enable JVM proxy
   *
   * @param config connector configuration
   */
  public static void enableJVMProxy(Map<String, String> config) {
    String host =
        ConnectorConfigTools.getProperty(config, KafkaConnectorConfigParams.JVM_PROXY_HOST);
    String port =
        ConnectorConfigTools.getProperty(config, KafkaConnectorConfigParams.JVM_PROXY_PORT);
    String nonProxyHosts =
        ConnectorConfigTools.getProperty(config, KafkaConnectorConfigParams.JVM_NON_PROXY_HOSTS);
    if (host != null && port != null) {
      LOGGER.info(
          "enable jvm proxy: {}:{} and bypass proxy for hosts: {}", host, port, nonProxyHosts);

      // enable https proxy
      System.setProperty(KafkaConnectorConfigParams.HTTP_USE_PROXY, "true");
      System.setProperty(KafkaConnectorConfigParams.HTTP_PROXY_HOST, host);
      System.setProperty(KafkaConnectorConfigParams.HTTP_PROXY_PORT, port);
      System.setProperty(KafkaConnectorConfigParams.HTTPS_PROXY_HOST, host);
      System.setProperty(KafkaConnectorConfigParams.HTTPS_PROXY_PORT, port);

      // If the user provided the jvm.nonProxy.hosts configuration then we
      // will append that to the list provided by the JVM argument
      // -Dhttp.nonProxyHosts and not override it altogether, if it exists.
      if (nonProxyHosts != null) {
        nonProxyHosts =
            (System.getProperty(KafkaConnectorConfigParams.HTTP_NON_PROXY_HOSTS) != null)
                ? System.getProperty(KafkaConnectorConfigParams.HTTP_NON_PROXY_HOSTS)
                    + "|"
                    + nonProxyHosts
                : nonProxyHosts;
        System.setProperty(KafkaConnectorConfigParams.HTTP_NON_PROXY_HOSTS, nonProxyHosts);
      }

      // set username and password
      String username =
          ConnectorConfigTools.getProperty(config, KafkaConnectorConfigParams.JVM_PROXY_USERNAME);
      String password =
          ConnectorConfigTools.getProperty(config, KafkaConnectorConfigParams.JVM_PROXY_PASSWORD);
      if (username != null && password != null) {
        Authenticator.setDefault(
            new Authenticator() {
              @Override
              public PasswordAuthentication getPasswordAuthentication() {
                return new PasswordAuthentication(username, password.toCharArray());
              }
            });
        System.setProperty(JDK_HTTP_AUTH_TUNNELING, "");
        System.setProperty(KafkaConnectorConfigParams.HTTP_PROXY_USER, username);
        System.setProperty(KafkaConnectorConfigParams.HTTP_PROXY_PASSWORD, password);
        System.setProperty(KafkaConnectorConfigParams.HTTPS_PROXY_USER, username);
        System.setProperty(KafkaConnectorConfigParams.HTTPS_PROXY_PASSWORD, password);
      }
    }
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
  public static boolean isValidSnowflakeApplicationName(String appName) {
    return appName.matches("^[-_a-zA-Z]{1}[-_$a-zA-Z0-9]+$");
  }

  static boolean isValidSnowflakeTableName(String tableName) {
    return tableName.matches("^([_a-zA-Z]{1}[_$a-zA-Z0-9]+\\.){0,2}[_a-zA-Z]{1}[_$a-zA-Z0-9]+$");
  }

  /**
   * @param config config with applied default values
   * @return true when Iceberg mode is enabled.
   */
  public static boolean isIcebergEnabled(Map<String, String> config) {
    return Boolean.parseBoolean(config.get(SNOWFLAKE_STREAMING_ICEBERG_ENABLED));
  }

  /**
   * @param config config with applied default values
   * @return role specified in rhe config
   */
  public static String getRole(Map<String, String> config) {
    return config.get(KafkaConnectorConfigParams.SNOWFLAKE_ROLE_NAME);
  }

  public static String getDatabase(Map<String, String> config) {
    return config.get(KafkaConnectorConfigParams.SNOWFLAKE_DATABASE_NAME);
  }

  public static String getSchema(Map<String, String> config) {
    return config.get(KafkaConnectorConfigParams.SNOWFLAKE_SCHEMA_NAME);
  }

  public static String getUser(Map<String, String> config) {
    return config.get(KafkaConnectorConfigParams.SNOWFLAKE_USER_NAME);
  }

  /** Asserts that the provided value is not {@code null}. */
  public static void assertNotNull(String name, Object value) {
    if (value == null) {
      throw new IllegalArgumentException(name + " cannot be null");
    }
  }

  /**
   * Concatenates the provided path segments into a dot-delimited string while escaping any literal
   * dots.
   */
  public static String concatDotPath(String... path) {
    StringBuilder sb = new StringBuilder();
    for (String segment : path) {
      if (segment == null) {
        throw new IllegalArgumentException("Path cannot be null");
      }
      if (sb.length() > 0) {
        sb.append('.');
      }
      sb.append(segment.replace("\\", "\\\\").replace(".", "\\."));
    }
    return sb.toString();
  }

  /**
   * Class for returned GeneratedName. isNameFromMap equal to True indicates that the name was
   * resolved by using the map passed to appropriate function. {@link
   * Utils#generateTableName(String, Map)}
   */
  public static class GeneratedName {
    private final String name;
    private final boolean isNameFromMap;

    private GeneratedName(String name, boolean isNameFromMap) {
      this.name = name;
      this.isNameFromMap = isNameFromMap;
    }

    private static GeneratedName fromMap(String name) {
      return new GeneratedName(name, true);
    }

    public static GeneratedName generated(String name) {
      return new GeneratedName(name, false);
    }

    public String getName() {
      return name;
    }

    public boolean isNameFromMap() {
      return isNameFromMap;
    }
  }

  /**
   * modify invalid application name in config and return the generated application name
   *
   * @param config input config object
   */
  public static void convertAppName(Map<String, String> config) {
    String appName = config.getOrDefault(KafkaConnectorConfigParams.NAME, "");
    // If appName is empty the following call will throw error
    String validAppName = generateValidName(appName, new HashMap<>());

    config.put(KafkaConnectorConfigParams.NAME, validAppName);
  }

  /**
   * verify topic name, and generate valid table name
   *
   * @param topic input topic name
   * @param topic2table topic to table map
   * @return valid table name
   */
  public static String getTableName(String topic, Map<String, String> topic2table) {
    return generateValidName(topic, topic2table);
  }

  /**
   * Verify topic name and generate a valid table name. The returned GeneratedName has a flag
   * isNameFromMap that indicates if the name was retrieved from the passed topic2table map which
   * has particular outcomes for the SnowflakeSinkServiceV1
   *
   * @param topic input topic name
   * @param topic2table topic to table map
   * @return return GeneratedName with valid table name and a flag whether the name was taken from
   *     the topic2table
   */
  public static GeneratedName generateTableName(String topic, Map<String, String> topic2table) {
    return generateValidNameFromMap(topic, topic2table);
  }

  /**
   * verify topic name, and generate valid table/application name
   *
   * @param topic input topic name
   * @param topic2table topic to table map
   * @return valid table/application name
   */
  public static String generateValidName(String topic, Map<String, String> topic2table) {
    return generateValidNameFromMap(topic, topic2table).name;
  }

  /**
   * verify topic name, and generate valid table/application name
   *
   * @param topic input topic name
   * @param topic2table topic to table map
   * @return valid generated table/application name
   */
  private static GeneratedName generateValidNameFromMap(
      String topic, Map<String, String> topic2table) {
    final String PLACE_HOLDER = "_";
    if (topic == null || topic.isEmpty()) {
      throw SnowflakeErrors.ERROR_0020.getException("topic name: " + topic);
    }
    if (topic2table.containsKey(topic)) {
      return GeneratedName.fromMap(topic2table.get(topic));
    }

    // try matching regex tables
    for (String regexTopic : topic2table.keySet()) {
      if (topic.matches(regexTopic)) {
        return GeneratedName.fromMap(topic2table.get(regexTopic));
      }
    }

    if (Utils.isValidSnowflakeObjectIdentifier(topic)) {
      return GeneratedName.generated(topic);
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

    return GeneratedName.generated(result.toString());
  }

  public static Map<String, String> parseTopicToTableMap(String input) {
    Map<String, String> topic2Table = new HashMap<>();
    boolean isInvalid = false;
    for (String str : input.split(",")) {
      String[] tt = str.split(":");

      if (tt.length != 2 || tt[0].trim().isEmpty() || tt[1].trim().isEmpty()) {
        LOGGER.error(
            "Invalid {} config format: {}",
            KafkaConnectorConfigParams.SNOWFLAKE_TOPICS2TABLE_MAP,
            input);
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

  /**
   * Convert a Comma separated key value pairs into a Map
   *
   * @param input Provided in KC config
   * @return Map
   */
  public static Map<String, String> parseCommaSeparatedKeyValuePairs(String input) {
    Map<String, String> pairs = new HashMap<>();
    for (String str : input.split(",")) {
      String[] tt = str.split(":");

      if (tt.length != 2 || tt[0].trim().isEmpty() || tt[1].trim().isEmpty()) {
        LOGGER.error(
            "Invalid {} config format: {}",
            KafkaConnectorConfigParams.SNOWFLAKE_STREAMING_CLIENT_PROVIDER_OVERRIDE_MAP,
            input);
        throw SnowflakeErrors.ERROR_0030.getException();
      }
      pairs.put(tt[0].trim(), tt[1].trim());
    }
    return pairs;
  }

  static final String[] loginPropList = {
    KafkaConnectorConfigParams.SNOWFLAKE_URL_NAME,
    KafkaConnectorConfigParams.SNOWFLAKE_USER_NAME,
    KafkaConnectorConfigParams.SNOWFLAKE_SCHEMA_NAME,
    KafkaConnectorConfigParams.SNOWFLAKE_DATABASE_NAME
  };

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
}
