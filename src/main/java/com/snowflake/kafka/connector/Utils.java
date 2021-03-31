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

import com.snowflake.kafka.connector.internal.Logging;
import com.snowflake.kafka.connector.internal.SnowflakeErrors;
import com.snowflake.kafka.connector.internal.SnowflakeKafkaConnectorException;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Various arbitrary helper functions
 */
public class Utils
{

  //Connector version, change every release
  public static final String VERSION = "1.5.2";

  //connector parameter list
  public static final String NAME = "name";
  public static final String SF_DATABASE = "snowflake.database.name";
  public static final String SF_SCHEMA = "snowflake.schema.name";
  public static final String SF_USER = "snowflake.user.name";
  public static final String SF_PRIVATE_KEY = "snowflake.private.key";
  public static final String SF_URL = "snowflake.url.name";
  public static final String SF_SSL = "sfssl";                 // for test only
  public static final String SF_WAREHOUSE = "sfwarehouse";     //for test only
  public static final String PRIVATE_KEY_PASSPHRASE = "snowflake.private.key" +
    ".passphrase";

  //constants strings
  private static final String KAFKA_OBJECT_PREFIX = "SNOWFLAKE_KAFKA_CONNECTOR";

  //task id
  public static final String TASK_ID = "task_id";

  //jvm proxy
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


  //jdbc log dir
  public static final String JAVA_IO_TMPDIR = "java.io.tmpdir";

  private static final Random random = new Random();

  //mvn repo
  private static final String MVN_REPO =
    "https://repo1.maven.org/maven2/com/snowflake/snowflake-kafka-connector/";

  private static final Logger LOGGER =
    LoggerFactory.getLogger(Utils.class.getName());

  /**
   * check the connector version from Maven repo, report if any update
   * version is available.
   *
   * A URl connection timeout is added in case Maven repo is not reachable in a proxy'd environment.
   * Returning false from this method doesnt have any side effects to start the connector.
   */
  static boolean checkConnectorVersion()
  {
    LOGGER.info(Logging.logMessage("Current Snowflake Kafka Connector Version: {}",
      VERSION));
    try
    {
      String latestVersion = null;
      int largestNumber = 0;
      URLConnection urlConnection =  new URL(MVN_REPO).openConnection();
      urlConnection.setConnectTimeout(5000);
      urlConnection.setReadTimeout(5000);
      InputStream input = urlConnection.getInputStream();
      BufferedReader bufferedReader =
        new BufferedReader(new InputStreamReader(input));
      String line;
      Pattern pattern = Pattern.compile("(\\d+\\.\\d+\\.\\d+?)");
      while ((line = bufferedReader.readLine()) != null)
      {
        Matcher matcher = pattern.matcher(line);
        if (matcher.find())
        {
          String version = matcher.group(1);
          String[] numbers = version.split("\\.");
          int num =
            Integer.parseInt(numbers[0]) * 10000 +
              Integer.parseInt(numbers[1]) * 100 +
              Integer.parseInt(numbers[2]);
          if (num > largestNumber)
          {
            largestNumber = num;
            latestVersion = version;
          }
        }
      }

      if (latestVersion == null)
      {
        throw new Exception("can't retrieve version number from Maven repo");
      }
      else if (!latestVersion.equals(VERSION))
      {
        LOGGER.warn(Logging.logMessage("Connector update is available, please" +
          " upgrade Snowflake Kafka Connector ({} -> {}) ", VERSION,
          latestVersion));
      }
    } catch (Exception e)
    {
      LOGGER.warn(Logging.logMessage("can't verify latest connector version " +
        "from Maven Repo\n{}", e.getMessage()));
      return false;
    }

    return true;
  }

  /**
   * @param appName connector name
   * @return connector object prefix
   */
  private static String getObjectPrefix(String appName)
  {
    return KAFKA_OBJECT_PREFIX + "_" + appName;
  }

  /**
   * generate stage name by given table
   *
   * @param appName connector name
   * @param table   table name
   * @return stage name
   */
  public static String stageName(String appName, String table)
  {
    String stageName = getObjectPrefix(appName) + "_STAGE_" + table;

    LOGGER.debug(Logging.logMessage("generated stage name: {}", stageName));

    return stageName;
  }

  /**
   * generate pipe name by given table and partition
   *
   * @param appName   connector name
   * @param table     table name
   * @param partition partition name
   * @return pipe name
   */
  public static String pipeName(String appName, String table, int partition)
  {
    String pipeName = getObjectPrefix(appName) + "_PIPE_" + table + "_" +
      partition;

    LOGGER.debug(Logging.logMessage("generated pipe name: {}", pipeName));

    return pipeName;
  }

  /**
   * Read JDBC logging directory from environment variable JDBC_LOG_DIR and set that in System property
   */
  public static void setJDBCLoggingDirectory()
  {
    String jdbcTmpDir = System.getenv(SnowflakeSinkConnectorConfig.SNOWFLAKE_JDBC_LOG_DIR);
    if (jdbcTmpDir != null)
    {
      File jdbcTmpDirObj = new File(jdbcTmpDir);
      if (jdbcTmpDirObj.isDirectory())
      {
        LOGGER.info(Logging.logMessage("jdbc tracing directory = {}", jdbcTmpDir));
        System.setProperty(JAVA_IO_TMPDIR, jdbcTmpDir);
      } else {
        LOGGER.info(Logging.logMessage("invalid JDBC_LOG_DIR {} defaulting to {}", jdbcTmpDir,
          System.getProperty(JAVA_IO_TMPDIR)));
      }
    }
  }

  /**
   * validate whether proxy settings in the config is valid
   * @param config connector configuration
   */
  static void validateProxySetting(Map<String, String> config)
  {
    String host = SnowflakeSinkConnectorConfig.getProperty(config, SnowflakeSinkConnectorConfig.JVM_PROXY_HOST);
    String port = SnowflakeSinkConnectorConfig.getProperty(config, SnowflakeSinkConnectorConfig.JVM_PROXY_PORT);
    // either both host and port are provided or none of them are provided
    if (host != null ^ port != null)
    {
      throw SnowflakeErrors.ERROR_0022.getException(SnowflakeSinkConnectorConfig.JVM_PROXY_HOST +
        " and " + SnowflakeSinkConnectorConfig.JVM_PROXY_PORT + " must be provided together");
    }
    else if (host != null)
    {
      String username = SnowflakeSinkConnectorConfig.getProperty(config, SnowflakeSinkConnectorConfig.JVM_PROXY_USERNAME);
      String password = SnowflakeSinkConnectorConfig.getProperty(config, SnowflakeSinkConnectorConfig.JVM_PROXY_PASSWORD);
      // either both username and password are provided or none of them are provided
      if (username != null ^ password != null)
      {
        throw SnowflakeErrors.ERROR_0023.getException(SnowflakeSinkConnectorConfig.JVM_PROXY_USERNAME +
          " and " + SnowflakeSinkConnectorConfig.JVM_PROXY_PASSWORD + " must be provided together");
      }
    }
  }

  /**
   * Enable JVM proxy
   *
   * @param config connector configuration
   * @return false if wrong config
   */
  static boolean enableJVMProxy(Map<String, String> config)
  {
    String host = SnowflakeSinkConnectorConfig.getProperty(config, SnowflakeSinkConnectorConfig.JVM_PROXY_HOST);
    String port = SnowflakeSinkConnectorConfig.getProperty(config, SnowflakeSinkConnectorConfig.JVM_PROXY_PORT);
    if (host != null && port != null)
    {
      LOGGER.info(Logging.logMessage("enable jvm proxy: {}:{}", host, port));

      // enable https proxy
      System.setProperty(HTTP_USE_PROXY, "true");
      System.setProperty(HTTP_PROXY_HOST, host);
      System.setProperty(HTTP_PROXY_PORT, port);
      System.setProperty(HTTPS_PROXY_HOST, host);
      System.setProperty(HTTPS_PROXY_PORT, port);

      // set username and password
      String username = SnowflakeSinkConnectorConfig.getProperty(config, SnowflakeSinkConnectorConfig.JVM_PROXY_USERNAME);
      String password = SnowflakeSinkConnectorConfig.getProperty(config, SnowflakeSinkConnectorConfig.JVM_PROXY_PASSWORD);
      if (username != null && password != null) {
        Authenticator.setDefault(
          new Authenticator() {
            @Override
            public PasswordAuthentication getPasswordAuthentication() {
              return new PasswordAuthentication(username, password.toCharArray());
            }
          }
        );
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
  static boolean isValidSnowflakeObjectIdentifier(String objName)
  {
    return objName.matches("^[_a-zA-Z]{1}[_$a-zA-Z0-9]+$");
  }

  /**
   * validates that given name is a valid snowflake application name, support '-'
   *
   * @param appName snowflake application name
   * @return true if given application name is valid
   */
  static boolean isValidSnowflakeApplicationName(String appName)
  {
    return appName.matches("^[-_a-zA-Z]{1}[-_$a-zA-Z0-9]+$");
  }

  static boolean isValidSnowflakeTableName(String tableName)
  {
    return tableName.matches("^([_a-zA-Z]{1}[_$a-zA-Z0-9]+\\.){0,2}[_a-zA-Z]{1}[_$a-zA-Z0-9]+$");
  }

  /**
   * Validate input configuration
   *
   * @param config configuration Map
   * @return connector name
   */
  static String validateConfig(Map<String, String> config)
  {
    boolean configIsValid = true; // verify all config

    // define the input parameters / keys in one place as static constants,
    // instead of using them directly
    // define the thresholds statically in one place as static constants,
    // instead of using the values directly

    // unique name of this connector instance
    String connectorName =
      config.getOrDefault(SnowflakeSinkConnectorConfig.NAME, "");
    if (connectorName.isEmpty() || !isValidSnowflakeApplicationName(connectorName))
    {
      LOGGER.error(Logging.logMessage("{} is empty or invalid. It " +
        "should match Snowflake object identifier syntax. Please see the " +
        "documentation.", SnowflakeSinkConnectorConfig.NAME));
      configIsValid = false;
    }

    //verify buffer.flush.time
    if (!config.containsKey(SnowflakeSinkConnectorConfig.BUFFER_FLUSH_TIME_SEC))
    {
      LOGGER.error(Logging.logMessage("{} is empty",
        SnowflakeSinkConnectorConfig.BUFFER_FLUSH_TIME_SEC));
      configIsValid = false;
    }
    else
    {
      try
      {
        long time = Long.parseLong(
          config.get(SnowflakeSinkConnectorConfig.BUFFER_FLUSH_TIME_SEC));
        if (time < SnowflakeSinkConnectorConfig.BUFFER_FLUSH_TIME_SEC_MIN)
        {
          LOGGER.error((Logging.logMessage("{} is {}, it should be greater " +
              "than {}",
            SnowflakeSinkConnectorConfig.BUFFER_FLUSH_TIME_SEC, time,
            SnowflakeSinkConnectorConfig.BUFFER_FLUSH_TIME_SEC_MIN)));
          configIsValid = false;
        }
      } catch (Exception e)
      {
        LOGGER.error(Logging.logMessage("{} should be an integer",
          SnowflakeSinkConnectorConfig.BUFFER_FLUSH_TIME_SEC));
        configIsValid = false;
      }
    }

    //verify buffer.count.records
    if (!config.containsKey(SnowflakeSinkConnectorConfig.BUFFER_COUNT_RECORDS))
    {
      LOGGER.error(Logging.logMessage("{} is empty",
        SnowflakeSinkConnectorConfig.BUFFER_COUNT_RECORDS));
      configIsValid = false;
    }
    else
    {
      try
      {
        long num = Long.parseLong(
          config.get(SnowflakeSinkConnectorConfig.BUFFER_COUNT_RECORDS));
        if (num < 0)
        {
          LOGGER.error(Logging.logMessage("{} is {}, it should not be negative",
            SnowflakeSinkConnectorConfig.BUFFER_COUNT_RECORDS, num));
          configIsValid = false;
        }
      } catch (Exception e)
      {
        LOGGER.error(Logging.logMessage("{} should be an integer",
          SnowflakeSinkConnectorConfig.BUFFER_COUNT_RECORDS));
        configIsValid = false;
      }
    }

    //verify buffer.size.bytes
    if (config.containsKey(SnowflakeSinkConnectorConfig.BUFFER_SIZE_BYTES))
    {
      try
      {
        long bsb = Long.parseLong(config.get(SnowflakeSinkConnectorConfig
          .BUFFER_SIZE_BYTES));
        if (bsb < SnowflakeSinkConnectorConfig.BUFFER_SIZE_BYTES_MIN)   // 1 byte
        {
          LOGGER.error(Logging.logMessage("{} is too low at {}. It must be " +
              "{} or greater.",
            SnowflakeSinkConnectorConfig.BUFFER_SIZE_BYTES, bsb,
            SnowflakeSinkConnectorConfig.BUFFER_SIZE_BYTES_MIN));
          configIsValid = false;
        }
      } catch (Exception e)
      {
        LOGGER.error(Logging.logMessage("{} should be an integer",
          SnowflakeSinkConnectorConfig.BUFFER_SIZE_BYTES));
        configIsValid = false;
      }
    }
    else
    {
      LOGGER.error(Logging.logMessage("{} is empty",
        SnowflakeSinkConnectorConfig.BUFFER_SIZE_BYTES));
      configIsValid = false;
    }

    if(config.containsKey(SnowflakeSinkConnectorConfig.TOPICS_TABLES_MAP) &&
    parseTopicToTableMap(config.get(SnowflakeSinkConnectorConfig.TOPICS_TABLES_MAP))==null)
    {
      configIsValid = false;
    }

    // sanity check
    if (!config.containsKey(SnowflakeSinkConnectorConfig.SNOWFLAKE_DATABASE))
    {
      LOGGER.error(Logging.logMessage("{} cannot be empty.",
        SnowflakeSinkConnectorConfig.SNOWFLAKE_DATABASE));
      configIsValid = false;
    }

    // sanity check
    if (!config.containsKey(SnowflakeSinkConnectorConfig.SNOWFLAKE_SCHEMA))
    {
      LOGGER.error(Logging.logMessage("{} cannot be empty.",
        SnowflakeSinkConnectorConfig.SNOWFLAKE_SCHEMA));
      configIsValid = false;
    }

    if (!config.containsKey(SnowflakeSinkConnectorConfig.SNOWFLAKE_PRIVATE_KEY))
    {
      LOGGER.error(Logging.logMessage("{} cannot be empty.",
        SnowflakeSinkConnectorConfig.SNOWFLAKE_PRIVATE_KEY));
      configIsValid = false;
    }

    if (!config.containsKey(SnowflakeSinkConnectorConfig.SNOWFLAKE_USER))
    {
      LOGGER.error(Logging.logMessage("{} cannot be empty.",
        SnowflakeSinkConnectorConfig.SNOWFLAKE_USER));
      configIsValid = false;
    }

    if (!config.containsKey(SnowflakeSinkConnectorConfig.SNOWFLAKE_URL))
    {
      LOGGER.error(Logging.logMessage("{} cannot be empty.",
        SnowflakeSinkConnectorConfig.SNOWFLAKE_URL));
      configIsValid = false;
    }
    // jvm proxy settings
    try
    {
      validateProxySetting(config);
    } catch (SnowflakeKafkaConnectorException e)
    {
      LOGGER.error(Logging.logMessage("Proxy settings error: ", e.getMessage()));
      configIsValid = false;
    }

    // set jdbc logging directory
    Utils.setJDBCLoggingDirectory();

    if (!configIsValid)
    {
      throw SnowflakeErrors.ERROR_0001.getException();
    }

    return connectorName;
  }

  /**
   * modify invalid application name in config and return the generated application name
   * @param config input config object
   */
  public static void convertAppName(Map<String, String> config)
  {
    String appName =
      config.getOrDefault(SnowflakeSinkConnectorConfig.NAME, "");
    // If appName is empty the following call will throw error
    String validAppName = generateValidName(appName, new HashMap<String, String>());

    config.put(SnowflakeSinkConnectorConfig.NAME, validAppName);
  }

  /**
   * verify topic name, and generate valid table name
   * @param topic input topic name
   * @param topic2table topic to table map
   * @return valid table name
   */
  public static String tableName(String topic, Map<String, String> topic2table)
  {
    return generateValidName(topic, topic2table);
  }

  /**
   * verify topic name, and generate valid table/application name
   * @param topic input topic name
   * @param topic2table topic to table map
   * @return valid table/application name
   */
  public static String generateValidName(String topic, Map<String, String> topic2table)
  {
    final String PLACE_HOLDER = "_";
    if(topic == null || topic.isEmpty())
    {
      throw SnowflakeErrors.ERROR_0020.getException("topic name: " + topic);
    }
    if(topic2table.containsKey(topic))
    {
      return topic2table.get(topic);
    }
    if(Utils.isValidSnowflakeObjectIdentifier(topic))
    {
      return topic;
    }
    int hash = Math.abs(topic.hashCode());

    StringBuilder result = new StringBuilder();

    int index = 0;
    //first char
    if(topic.substring(index,index + 1).matches("[_a-zA-Z]"))
    {
      result.append(topic.charAt(0));
      index ++;
    }
    else
    {
      result.append(PLACE_HOLDER);
    }
    while(index < topic.length())
    {
      if (topic.substring(index, index + 1).matches("[_$a-zA-Z0-9]"))
      {
        result.append(topic.charAt(index));
      }
      else
      {
        result.append(PLACE_HOLDER);
      }
      index ++;
    }

    result.append(PLACE_HOLDER);
    result.append(hash);

    return result.toString();
  }

  public static Map<String, String> parseTopicToTableMap(String input)
  {
    Map<String, String> topic2Table = new HashMap<>();
    boolean isInvalid = false;
    for (String str : input.split(","))
    {
      String[] tt = str.split(":");


      if (tt.length != 2 || tt[0].trim().isEmpty() || tt[1].trim().isEmpty())
      {
        LOGGER.error(Logging.logMessage("Invalid {} config format: {}",
          SnowflakeSinkConnectorConfig.TOPICS_TABLES_MAP, input));
        return null;
      }

      String topic = tt[0].trim();
      String table = tt[1].trim();

      if (!isValidSnowflakeTableName(table))
      {
        LOGGER.error(
          Logging.logMessage("table name {} should have at least 2 " +
            "characters, start with _a-zA-Z, and only contains " +
            "_$a-zA-z0-9", table));
        isInvalid = true;
      }

      if (topic2Table.containsKey(topic))
      {
        LOGGER.error(Logging.logMessage("topic name {} is duplicated",
          topic));
        isInvalid = true;
      }

      if (topic2Table.containsValue(table))
      {
        //todo: support multiple topics map to one table ?
        LOGGER.error(Logging.logMessage("table name {} is duplicated",
          table));
        isInvalid = true;
      }
      topic2Table.put(tt[0].trim(), tt[1].trim());
    }
    if(isInvalid)
    {
      throw SnowflakeErrors.ERROR_0021.getException();
    }
    return topic2Table;
  }


  static final String loginPropList[] =
    {
      SF_URL,
      SF_USER,
      SF_SCHEMA,
      SF_DATABASE
    };

  public static boolean isSingleFieldValid(Config result)
  {
    // if any single field validation failed
    for (ConfigValue v : result.configValues())
    {
      if (!v.errorMessages().isEmpty())
      {
        return false;
      }
    }
    // if any of url, user, schema, database or password is empty
    // update error message and return false
    boolean isValidate = true;
    final String errorMsg = " must be provided";
    Map<String, ConfigValue> validateMap = validateConfigToMap(result);
    //
    for (String prop : loginPropList)
    {
      if (validateMap.get(prop).value() == null)
      {
        updateConfigErrorMessage(result, prop, errorMsg);
        isValidate = false;
      }
    }

    return isValidate;
  }

  public static Map<String, ConfigValue> validateConfigToMap(final Config result)
  {
    Map<String, ConfigValue> validateMap = new HashMap<>();
    for (ConfigValue v : result.configValues())
    {
      validateMap.put(v.name(), v);
    }
    return validateMap;
  }

  public static void updateConfigErrorMessage(Config result, String key, String msg)
  {
    for (ConfigValue v : result.configValues())
    {
      if (v.name().equals(key))
      {
        v.addErrorMessage(key + msg);
      }
    }
  }

}
