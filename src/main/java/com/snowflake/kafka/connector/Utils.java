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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Various arbitrary helper functions
 */
public class Utils
{

  //Connector version, change every release
  public static final String VERSION = "1.1.0";

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
  private static final String HTTP_USE_PROXY = "http.useProxy";
  private static final String HTTPS_PROXY_HOST = "https.proxyHost";
  private static final String HTTPS_PROXY_PORT = "https.proxyPort";
  private static final String HTTP_PROXY_HOST = "http.proxyHost";
  private static final String HTTP_PROXY_PORT = "http.proxyPort";

  //mvn repo
  private static final String MVN_REPO =
    "https://repo1.maven.org/maven2/com/snowflake/snowflake-kafka-connector/";

  private static final Logger LOGGER =
    LoggerFactory.getLogger(Utils.class.getName());

  /**
   * check the connector version from Maven repo, report if any update
   * version is available.
   */
  static boolean checkConnectorVersion()
  {
    LOGGER.info(Logging.logMessage("Snowflake Kafka Connector Version: {}",
      VERSION));
    try
    {
      String latestVersion = null;
      int largestNumber = 0;
      URL url = new URL(MVN_REPO);
      InputStream input = url.openStream();
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
   * Enable JVM proxy
   *
   * @param config connector configuration
   * @return false if wrong config
   */
  static boolean enableJVMProxy(Map<String, String> config)
  {
    if (config.containsKey(SnowflakeSinkConnectorConfig.JVM_PROXY_HOST) &&
      !config.get(SnowflakeSinkConnectorConfig.JVM_PROXY_HOST).isEmpty())
    {
      if (!config.containsKey(SnowflakeSinkConnectorConfig.JVM_PROXY_PORT) ||
        config.get(SnowflakeSinkConnectorConfig.JVM_PROXY_PORT).isEmpty())
      {
        LOGGER.error(Logging.logMessage("{} is empty",
          SnowflakeSinkConnectorConfig.JVM_PROXY_PORT));
        return false;
      }
      else
      {
        String host = config.get(SnowflakeSinkConnectorConfig.JVM_PROXY_HOST);
        String port = config.get(SnowflakeSinkConnectorConfig.JVM_PROXY_PORT);
        LOGGER.info(Logging.logMessage("enable jvm proxy: {}:{}",
          host, port));

        //enable https proxy
        System.setProperty(HTTP_USE_PROXY, "true");
        System.setProperty(HTTP_PROXY_HOST, host);
        System.setProperty(HTTP_PROXY_PORT, port);
        System.setProperty(HTTPS_PROXY_HOST, host);
        System.setProperty(HTTPS_PROXY_PORT, port);
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
    if (connectorName.isEmpty() || !isValidSnowflakeObjectIdentifier(connectorName))
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
        if (bsb > SnowflakeSinkConnectorConfig.BUFFER_SIZE_BYTES_MAX)   // 100mb
        {
          LOGGER.error(Logging.logMessage("{} is too high at {}. It must be " +
              "{} or smaller.",
            SnowflakeSinkConnectorConfig.BUFFER_SIZE_BYTES, bsb,
            SnowflakeSinkConnectorConfig.BUFFER_SIZE_BYTES_MAX));
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
    configIsValid = Utils.enableJVMProxy(config) && configIsValid;

    //schemaRegistry

    String authSource = config.getOrDefault(
      SnowflakeSinkConnectorConfig.SCHEMA_REGISTRY_AUTH_CREDENTIALS_SOURCE, "");
    String userInfo = config.getOrDefault(
      SnowflakeSinkConnectorConfig.SCHEMA_REGISTRY_AUTH_USER_INFO, "");

    if (authSource.isEmpty() ^ userInfo.isEmpty())
    {
      configIsValid = false;
      LOGGER.error(Logging.logMessage("Parameters {} and {} should be defined" +
          " at the same time",
        SnowflakeSinkConnectorConfig.SCHEMA_REGISTRY_AUTH_USER_INFO,
        SnowflakeSinkConnectorConfig.SCHEMA_REGISTRY_AUTH_CREDENTIALS_SOURCE));
    }

    if (!configIsValid)
    {
      throw SnowflakeErrors.ERROR_0001.getException();
    }

    return connectorName;
  }

  /**
   * verify topic name, and generate valid table name
   * @param topic input topic name
   * @param topic2table topic to table map
   * @return table name
   */
  public static String tableName(String topic, Map<String, String> topic2table)
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

  static Map<String, String> parseTopicToTableMap(String input)
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

}
