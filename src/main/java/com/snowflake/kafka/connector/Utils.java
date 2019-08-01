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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

/**
 * Various arbitrary helper functions
 */
public class Utils
{

  //Connector version, change every release
  static final String VERSION = "0.4.0";

  //connector parameter list
  public static final String NAME = "name";
  public static final String SF_DATABASE = "snowflake.database.name";
  public static final String SF_SCHEMA = "snowflake.schema.name";
  public static final String SF_USER = "snowflake.user.name";
  public static final String SF_PRIVATE_KEY = "snowflake.private.key";
  public static final String SF_URL = "snowflake.url.name";
  public static final String SF_SSL = "sfssl";                 // for test only
  public static final String SF_WAREHOUSE = "sfwarehouse";     //for test only
  public static final String PRIVATE_KEY_PASSPHRASE = "snowflake.private.key.passphrase";

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

  private static final Logger LOGGER =
    LoggerFactory.getLogger(Utils.class.getName());

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
   * @param config connector configuration
   * @return false if wrong config
   */
  static boolean enableJVMProxy(Map<String, String> config)
  {
    if (config.containsKey(SnowflakeSinkConnectorConfig.JVM_PROXY_HOST)&&
      !config.get(SnowflakeSinkConnectorConfig.JVM_PROXY_HOST).isEmpty())
    {
      if (!config.containsKey(SnowflakeSinkConnectorConfig.JVM_PROXY_PORT)||
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
  private static boolean isValidSnowflakeObjectIdentifier(String objName)
  {
    return objName.matches("^[_a-zA-Z]{1}[_$a-zA-Z0-9]+$");
  }

  /**
   * Validate input configuration
   * @param config configuration Map
   * @return false if contains any invalid settings
   */
  static ParameterValidationResult validateConfig(Map<String, String> config)
  {
    boolean configIsValid = true; // verify all config

    // define the input parameters / keys in one place as static constants,
    // instead of using them directly
    // define the thresholds statically in one place as static constants,
    // instead of using the values directly

    // unique name of this connector instance
    String connectorName = config.getOrDefault(SnowflakeSinkConnectorConfig.NAME, "");
    if (connectorName.isEmpty() || !isValidSnowflakeObjectIdentifier(connectorName))
    {
      LOGGER.error(Logging.logMessage("{} is empty or invalid. It " +
        "should match Snowflake object identifier syntax. Please see the " +
        "documentation.", SnowflakeSinkConnectorConfig.NAME));
      configIsValid = false;
    }

    //verify buffer.flush.time
    if(!config.containsKey(SnowflakeSinkConnectorConfig.BUFFER_FLUSH_TIME_SEC))
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
        if(time < SnowflakeSinkConnectorConfig.BUFFER_FLUSH_TIME_SEC_MIN)
        {
          LOGGER.error((Logging.logMessage("{} is {}, it should be greater than {}",
            SnowflakeSinkConnectorConfig.BUFFER_FLUSH_TIME_SEC, time,
            SnowflakeSinkConnectorConfig.BUFFER_FLUSH_TIME_SEC_MIN)));
          configIsValid = false;
        }
      }
      catch (Exception e)
      {
        LOGGER.error(Logging.logMessage("{} should be an integer",
          SnowflakeSinkConnectorConfig.BUFFER_FLUSH_TIME_SEC));
        configIsValid = false;
      }
    }

    //verify buffer.count.records
    if(!config.containsKey(SnowflakeSinkConnectorConfig.BUFFER_COUNT_RECORDS))
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
        if(num < 0)
        {
          LOGGER.error(Logging.logMessage("{} is {}, it should not be negative",
            SnowflakeSinkConnectorConfig.BUFFER_COUNT_RECORDS, num));
          configIsValid = false;
        }
      }
      catch (Exception e)
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
      }
      catch (Exception e)
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


    // validate topics
    List<String> topics = new ArrayList<>();
    if (config.containsKey(SnowflakeSinkConnectorConfig.TOPICS))
    {
      topics = new ArrayList<>(Arrays.asList(config.get
        (SnowflakeSinkConnectorConfig.TOPICS).split(",")));

      LOGGER.info(Logging.logMessage("Connector {} consuming topics {}",
        connectorName, topics.toString()));

      // check for duplicates
      HashSet<String> topicsHashSet = new HashSet<>(Arrays.asList(config.get
        (SnowflakeSinkConnectorConfig.TOPICS).split(",")));
      if (topics.size() != topicsHashSet.size())
      {
        LOGGER.error(Logging.logMessage("{}: {} contains duplicate " +
            "entries.", SnowflakeSinkConnectorConfig.TOPICS,
          config.get(SnowflakeSinkConnectorConfig.TOPICS)));
        configIsValid = false;
      }
    }
    else
    {
      LOGGER.error(Logging.logMessage("{} cannot be empty.",
        SnowflakeSinkConnectorConfig.TOPICS));
      configIsValid = false;
    }

    // validate snowflake.topic2table.map (optional parameter)
    Map<String, String> topicsTablesMap = new HashMap<>();  // initialize even if not present in
    // config, as it is needed
    if (config.containsKey(SnowflakeSinkConnectorConfig.TOPICS_TABLES_MAP))
    {
      List<String> topicsTables = new ArrayList<>(Arrays.asList(config.get
        (SnowflakeSinkConnectorConfig.TOPICS_TABLES_MAP).split(",")));

      for (String topicTable : topicsTables)
      {
        String[] tt = topicTable.split(":");

        if (tt.length != 2 || tt[0].isEmpty() || tt[1].isEmpty())
        {
          LOGGER.error(Logging.logMessage("Invalid {} config format: {}",
            SnowflakeSinkConnectorConfig.TOPICS_TABLES_MAP, topicsTables));
          configIsValid = false;
          break;
        }

        if (!isValidSnowflakeObjectIdentifier(tt[1]))
        {
          LOGGER.error(
            Logging.logMessage("table name {} should have at least 2 " +
              "characters, start with _a-zA-Z, and only contains " +
              "_$a-zA-z0-9", tt[1]));
          configIsValid = false;
        }

        if (!topics.contains(tt[0]))
        {
          LOGGER.error(Logging.logMessage("topic name {} has not been " +
            "registered in this task", tt[0]));
          configIsValid = false;
        }

        if (topicsTablesMap.containsKey(tt[0]))
        {
          LOGGER.error(Logging.logMessage("topic name {} is duplicated",
            tt[0]));
          configIsValid = false;
        }

        if (topicsTablesMap.containsValue(tt[1]))
        {
          //todo: support multiple topics map to one table ?
          LOGGER.error(Logging.logMessage("table name {} is duplicated",
            tt[1]));
          configIsValid = false;
        }

        topicsTablesMap.put(tt[0], tt[1]);
      }
    }

    // validate that topics which don't have a table name mapped, have a
    // Snowflake compatible name
    for (String topic : topics)
    {
      if (!topicsTablesMap.containsKey(topic))
      {
        if (isValidSnowflakeObjectIdentifier(topic))
        {
          topicsTablesMap.put(topic, topic);  // use topic name as the table
          // name
        }
        else
        {
          LOGGER.error(Logging.logMessage("topic: {} in {}: {} config " +
              "should either match Snowflake object identifier syntax, or {}:" +
              " {} config should contain a mapping table name.", topic,
            SnowflakeSinkConnectorConfig.TOPICS,
            config.get(SnowflakeSinkConnectorConfig.TOPICS),
            SnowflakeSinkConnectorConfig.TOPICS_TABLES_MAP,
            config.get(SnowflakeSinkConnectorConfig.TOPICS_TABLES_MAP)));
          configIsValid = false;
        }
      }
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

    if(authSource.isEmpty() ^ userInfo.isEmpty())
    {
      configIsValid = false;
      LOGGER.error(Logging.logMessage("Parameters {} and {} should be defined at the same time",
        SnowflakeSinkConnectorConfig.SCHEMA_REGISTRY_AUTH_USER_INFO,
        SnowflakeSinkConnectorConfig.SCHEMA_REGISTRY_AUTH_CREDENTIALS_SOURCE));
    }

    if(!configIsValid)
    {
      throw SnowflakeErrors.ERROR_0001.getException();
    }

    return new ParameterValidationResult(connectorName, topics, topicsTablesMap);
  }

  static class ParameterValidationResult
  {
    final String connectorName;
    final List<String> topics;
    final Map<String, String> topicsTableMap;
    ParameterValidationResult(
      String connectorName,
      List<String> topics,
      Map<String, String> topicsTableMap)
    {
      this.connectorName = connectorName;
      this.topics = topics;
      this.topicsTableMap = topicsTableMap;
    }
  }

}
