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
import com.snowflake.kafka.connector.internal.SnowflakeJDBCWrapper;
import com.snowflake.kafka.connector.internal.SnowflakeTelemetry;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;


/**
 * SnowflakeSinkConnector implements SinkConnector for Kafka Connect framework.
 * expects configuration: including topic names, partition numbers, snowflake
 * connection info and credentials info
 * creates snowflake internal stages, snowflake tables
 * provides configuration to SinkTasks running on Kafka Connect Workers.
 */
public class SnowflakeSinkConnector extends SinkConnector
{
  private Map<String, String> config; // connector configuration, provided by
  // user through kafka connect framework
  private String connectorName;       // unique name of this connector instance
  private List<String> topics;        // list of topics to be processed by
  // this connector instance
  private Map<String, String> topicsTablesMap;    // map of topics to tables

  // SnowflakeJDBCWrapper provides methods to interact with user's snowflake
  // account and executes queries
  private SnowflakeJDBCWrapper snowflakeConnection;

  // Snowflake Telemetry provides methods to report usage statistics
  private SnowflakeTelemetry telemetry;
  private long connectorStartTime;

  private static final Logger LOGGER = LoggerFactory
    .getLogger(SnowflakeSinkConnector.class);

  // Kafka Connect starts sink tasks without waiting for setup in
  // SnowflakeSinkConnector to finish.
  // This causes race conditions for: config validation, tables and stages
  // creation, etc.
  // Using setupComplete to synchronize
  private boolean setupComplete = false;

  /**
   * No-Arg constructor.
   * Required by Kafka Connect framework
   */
  public SnowflakeSinkConnector()
  {
    setupComplete = false;
  }

  /**
   * start method will only be called on a clean connector,
   * i.e. it has either just been instantiated and initialized or stop ()
   * has been invoked.
   * loads configuration and validates
   * creates snowflake internal stages and snowflake tables
   *
   * @param parsedConfig has the configuration settings
   */
  @Override
  public void start(final Map<String, String> parsedConfig)
  {
    LOGGER.info(Logging.logMessage("SnowflakeSinkConnector:start"));
    setupComplete = false;
    connectorStartTime = System.currentTimeMillis();

    config = new HashMap<>(parsedConfig);
    if (!validateConfig())
    {
      throw SnowflakeErrors.ERROR_0001.getException();
    }

    // create a persisted connection, and validate snowflake connection
    // config as a side effect
    snowflakeConnection = new SnowflakeJDBCWrapper(parsedConfig);


    telemetry = snowflakeConnection.getTelemetry();

    // maxTasks value isn't visible if the user leaves it at default. So 0
    // means unknown.
    int maxTasks = (config.containsKey("tasks.max")) ? Integer.parseInt
      (config.get("tasks.max")) : 0;

    telemetry.reportKafkaStart(
      connectorStartTime,
      topics.size(),
      topicsTablesMap.size(),
      maxTasks,
      connectorName);

    for (String topic : topics)
    {
      // create or validate table
      String table = topicsTablesMap.get(topic);

      try
      {
        if (snowflakeConnection.tableExist(table))
        {
          if (snowflakeConnection.tableIsCompatible(table))
          {
            // connector may have been restarted or user has pre-created the
            // correct table
            LOGGER.info(Logging.logMessage("Using existing table {} for topic" +
              " {}.", table, topic));
            telemetry.reportKafkaReuseTable(table, connectorName);
          }
          else
          {

            telemetry.reportKafkaFatalError("Incompatible table: " + table,
              connectorName);
            throw SnowflakeErrors.ERROR_5003.getException("table name: " +
              table);
          }
        }
        else
        {
          LOGGER.info(Logging.logMessage("Creating new table {} for topic {}.",
            table, topic));
          snowflakeConnection.createTable(table);
          telemetry.reportKafkaCreateTable(table, connectorName);
        }

        // store topic and table name in config for use by SinkTask and
        // SnowflakeJDBCWrapper
        config.put(topic, table);
      } catch (Exception e)
      {
        telemetry.reportKafkaFatalError(e.getMessage(), connectorName);
        // cleanup
        stop();
        throw SnowflakeErrors.ERROR_5006.getException(e);
      }


      // create or validate stage
      String stageName = Utils.stageName(connectorName, table);
      try
      {
        if (snowflakeConnection.stageExist(stageName))
        {
          if (snowflakeConnection.stageIsCompatible(stageName))
          {
            // connector may have been restarted
            // user is NOT expected to pre-create any internal stages!
            LOGGER.info(Logging.logMessage("Connector recovery: using " +
              "existing internal stage {} for topic {}.", stageName, topic));
            telemetry.reportKafkaReuseStage(stageName, connectorName);
          }
          else
          {
            telemetry.reportKafkaFatalError("Invalid stage: " + stageName,
              connectorName);

            throw SnowflakeErrors.ERROR_5004.getException("Stage name: " +
              stageName);
          }
        }
        else
        {
          LOGGER.info(Logging.logMessage("Creating new internal stage {} for " +
            "topic {}.", stageName, topic));
          snowflakeConnection.createStage(stageName);
          telemetry.reportKafkaCreateStage(stageName, connectorName);

        }
      } catch (Exception e)
      {
        telemetry.reportKafkaFatalError(e.getMessage(), connectorName);
        // cleanup
        stop();
        throw SnowflakeErrors.ERROR_5006.getException(e);
      }
    }

    setupComplete = true;
  }

  /**
   * Validate the input configurations
   */
  private boolean validateConfig()
  {
    boolean configIsValid = true; // verify all config

    // define the input parameters / keys in one place as static constants,
    // instead of using them directly
    // define the thresholds statically in one place as static constants,
    // instead of using the values directly

    // unique name of this connector instance
    connectorName = config.get(SnowflakeSinkConnectorConfig.NAME);
    if (connectorName.isEmpty() || !isValidSnowflakeObjectIdentifier
      (connectorName))
    {
      LOGGER.error(Logging.logMessage("{}: {} is empty or invalid. It " +
        "should match Snowflake object identifier syntax. Please see the " +
        "documentation.", SnowflakeSinkConnectorConfig.NAME, connectorName));
      configIsValid = false;
    }


    // set buffer.count.records -- a Snowflake connector setting
    // default : 10000 records
    // Number of records buffered in memory per partition before ingesting to
    // Snowflake
    if (!config.containsKey(SnowflakeSinkConnectorConfig
      .BUFFER_COUNT_RECORDS))//buffer.count.records
    {
      config.put(SnowflakeSinkConnectorConfig.BUFFER_COUNT_RECORDS, "10000");
      LOGGER.info(Logging.logMessage("{} set to default 10000 records.",
        SnowflakeSinkConnectorConfig.BUFFER_COUNT_RECORDS));
    }

    // set buffer.size.bytes -- a Snowflake connector setting
    // default : 5000000 bytes
    // Cumulative size of records buffered in memory per partition before
    // ingesting to Snowflake
    if (config.containsKey(SnowflakeSinkConnectorConfig.BUFFER_SIZE_BYTES))
    //buffer.size.bytes
    {
      long bsb = Long.parseLong(config.get(SnowflakeSinkConnectorConfig
        .BUFFER_SIZE_BYTES));
      if (bsb > 100000000)   // 100mb
      {
        LOGGER.error(Logging.logMessage("{} is too high at {}. It must be " +
          "100000000 (100MB) or smaller.", SnowflakeSinkConnectorConfig
          .BUFFER_SIZE_BYTES, bsb));
        configIsValid = false;
      }
    }
    else
    {
      config.put(SnowflakeSinkConnectorConfig.BUFFER_SIZE_BYTES, "5000000");
      LOGGER.info(Logging.logMessage("{} set to default 5000000 bytes.",
        SnowflakeSinkConnectorConfig.BUFFER_SIZE_BYTES));
    }

    // validate topics
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
    topicsTablesMap = new HashMap<>();  // initialize even if not present in
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

        if (topicsTablesMap.containsKey(tt[1]))
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

    return configIsValid;
  }

  /**
   * validates that given name is a valid snowflake object identifier
   *
   * @param objName
   * @return
   */
  private boolean isValidSnowflakeObjectIdentifier(String objName)
  {
    return objName.matches("^[_a-zA-Z]{1}[_$a-zA-Z0-9]+$");
  }

  /**
   * stop method will be called to stop a connector,
   * cleans up snowflake internal stages, after making sure that there are
   * no pending files to ingest
   * cleans up pipes, after making sure there are no pending files to ingest
   * also ensures that there are no leaked stages, no leaked staged files,
   * and no leaked pipes
   */
  @Override
  public void stop()
  {
    setupComplete = false;
    LOGGER.info(Logging.logMessage("SnowflakeSinkConnector:stop"));

    // Get the list of topics and drop corresponding internal stages.
    // Ensure that each file in the internal stage has been ingested, OR
    // copied to 'table stage' and an error message is logged.
    for (String topic : topics)
    {
      String stageName = Utils.stageName(connectorName, topicsTablesMap.get
        (topic));
      try
      {
        if (snowflakeConnection.stageExist(stageName))
        {
          LOGGER.info(Logging.logMessage("Dropping internal stage: {} (if " +
            "empty).", stageName));
          snowflakeConnection.dropStageIfEmpty(stageName);
        }
        else
        {
          // User should not mess up with these stages outside the connector
          String errorMsg = "Attempting to drop a non-existant internal stage" +
            " " + stageName + ". This should not happen. This stage may " +
            "have been manually dropped, which may affect ingestion " +
            "guarantees.";
          LOGGER.error(Logging.logMessage(errorMsg));
          telemetry.reportKafkaNonFatalError(errorMsg, connectorName);

          // continue-on-error in the stop method to cleanup other objects
        }
      } catch (Exception ex)
      {
        String errorMsg = "Failed to drop empty internal stage: " + stageName
          + ". It can safely be removed. Exception: " + ex.toString();
        LOGGER.error(Logging.logMessage(errorMsg));
        telemetry.reportKafkaNonFatalError(errorMsg, connectorName);

        // continue-on-error in the stop method to cleanup other objects
      }

      // TODO (GA): discover if there are any leaked stages and clean them up
      // when possible
      // TODO (GA): discover if there are any leaked pipes and clean them up
      // when possible
      // We are potentially laking pipe objects.
    }
  }

  // TODO (post GA): override reconfigure(java.util.Map<java.lang.String,java
  // .lang.String> props)
  // Default implementation shuts down all external network connections.
  // We can make it more efficient by identifying configuration changes,
  // creating new snowflake internal stages, new snowflake tables, new pipes,
  // for newly added topics;
  // and cleaning up stages for topics that are not in the new configuration,
  // and
  // cleaning up pipes for partitions that are not in the new configuration.

  /**
   * @return Sink task class
   */
  @Override
  public Class<? extends Task> taskClass()
  {
    return SnowflakeSinkTask.class;
  }

  /**
   * taskConfigs method returns a set of configurations for SinkTasks based
   * on the current configuration,
   * producing at most 'maxTasks' configurations
   *
   * @param maxTasks maximum number of SinkTasks for this instance of
   *                 SnowflakeSinkConnector
   * @return a list containing 'maxTasks' copies of the configuration
   */
  @Override
  public List<Map<String, String>> taskConfigs(final int maxTasks)
  {
    // wait for setup to complete
    int counter = 0;
    while (counter < 120)    // poll for 120*5 seconds (10 mins) maximum
    {
      if (setupComplete)
      {
        break;
      }
      else
      {
        counter++;
        try
        {
          LOGGER.info(Logging.logMessage("Sleeping 5000ms to allow setup to " +
            "complete."));
          Thread.sleep(5000);
        } catch (InterruptedException ex)
        {
          LOGGER.warn(Logging.logMessage("Waiting for setup to complete got " +
            "interrupted"));
        }
      }
    }
    if (!setupComplete)
    {
      telemetry.reportKafkaFatalError(SnowflakeErrors.ERROR_5007.getDetail(),
        connectorName);

      throw SnowflakeErrors.ERROR_5007.getException();
    }

    List<Map<String, String>> taskConfigs = new ArrayList<>(maxTasks);
    for (int i = 0; i < maxTasks; i++)
    {
      Map<String, String> conf = new HashMap<>(config);
      conf.put(Utils.TASK_ID, i + "");
      taskConfigs.add(conf);
    }
    return taskConfigs;
  }

  /**
   * @return ConfigDef with original configuration properties
   */
  @Override
  public ConfigDef config()
  {
    return SnowflakeSinkConnectorConfig.newConfigDef();
  }

  /**
   * @return connector version
   */
  @Override
  public String version()
  {
    return Utils.VERSION;
  }
}
