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

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
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
    LOGGER.info("SnowflakeSinkConnector:start");
    setupComplete = false;
    connectorStartTime = System.currentTimeMillis();

    config = new HashMap<>(parsedConfig);
    if (!validateConfig())
    {
      String errorMsg = "One or more errors detected in the input " +
        "configuration. " +
        "All errors are logged to the connect cluster's log. " +
        "Please restart the connector after fixing the input configuration.";
      LOGGER.error(errorMsg);
      throw new ConnectException(errorMsg);
    }

    // create a persisted connection, and validate snowflake connection
    // config as a side effect
    try
    {
      snowflakeConnection = new SnowflakeJDBCWrapper(parsedConfig);
    } catch (SQLException ex)
    {
      LOGGER.error("Failed to connect to Snowflake with the provided " +
        "configuration. " +
        "Please see the documentation. Exception: {}", ex.toString());

      // stop the connector as we are unlikely to automatically recover from
      // this error
      throw new ConnectException("Failed to connect to Snowflake with the " +
        "provided configuration.");
    }

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


    // TODO (GA) (SNOW-64054): create a connector application name lock file
    // to avoid conflicts in the following scenario :
    // Two Kafka clusters with same topic name/s, using their own Connect
    // clusters
    // but sending data to one snowflake account.
    // Destination table name doesn't matter. The conflict comes from stage
    // name and snowpipe name.

    // Get the list of topics and create corresponding tables and internal
    // stages.
    // Both setup and recovery execute on the same code path. We need to handle
    // recovery of existing internal stages and table when applicable

    // topics and topicsTablesMap are initialized in validateConfig() method

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
            LOGGER.info("Using existing table {} for topic {}.", table, topic);
            telemetry.reportKafkaReuseTable(table, connectorName);
          }
          else
          {
            String errorMsg = "Table " + table +
              " is configured to receive records from topic " + topic +
              " but doesn't have a compatible schema. Please see the " +
              "documentation.";
            LOGGER.error(errorMsg);
            telemetry.reportKafkaFatalError(errorMsg, connectorName);

            // stop the connector as we are unlikely to automatically recover
            // from this error
            throw new ConnectException(errorMsg);
          }
        }
        else
        {
          LOGGER.info("Creating new table {} for topic {}.", table, topic);
          snowflakeConnection.createTable(table);
          telemetry.reportKafkaCreateTable(table, connectorName);
        }

        // store topic and table name in config for use by SinkTask and
        // SnowflakeJDBCWrapper
        config.put(topic, table);
      } catch (SQLException ex)
      {
        String errorMsg = "Exception while creating or validating table " +
          table + " for topic " + topic + ". " +
          "User may have insufficient privileges. If this persists, please " +
          "contact Snowflake support. " +
          "Exception: " + ex.toString();
        LOGGER.error(errorMsg);
        telemetry.reportKafkaFatalError(errorMsg, connectorName);

        // cleanup
        stop();

        // stop the connector as we are unlikely to automatically recover
        // from this error
        throw new ConnectException(errorMsg);
      }


      // create or validate stage
      String stageName = Utils.stageName(table);
      try
      {
        if (snowflakeConnection.stageExist(stageName))
        {
          if (snowflakeConnection.stageIsCompatible(stageName))
          {
            // connector may have been restarted
            // user is NOT expected to pre-create any internal stages!
            LOGGER.info("Connector recovery: using existing internal stage {}" +
                " for topic {}.",
              stageName, topic);
            telemetry.reportKafkaReuseStage(stageName, connectorName);
          }
          else
          {
            String errorMsg = "Stage " + stageName +
              " contains files that were not created by the Snowflake Kafka" +
              " Connector. " +
              " The stage needs a careful cleanup. Please see the " +
              "documentation.";
            LOGGER.error(errorMsg);
            telemetry.reportKafkaFatalError(errorMsg, connectorName);

            // stop the connector as we are unlikely to automatically recover
            // from this error
            throw new ConnectException(errorMsg);
          }
        }
        else
        {
          LOGGER.info("Creating new internal stage {} for topic {}.",
            stageName, topic);
          snowflakeConnection.createStage(stageName);
          telemetry.reportKafkaCreateStage(stageName, connectorName);

        }
      } catch (SQLException ex)
      {
        String errorMsg = "Exception while creating or validating internal " +
          "stage " + stageName + ". " +
          "If this persists, please contact Snowflake support. " +
          "Exception: " + ex.toString();
        LOGGER.error(errorMsg);
        telemetry.reportKafkaFatalError(errorMsg, connectorName);

        // cleanup
        stop();

        // stop the connector as we are unlikely to automatically recover
        // from this error
        throw new ConnectException(errorMsg);
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
    connectorName = config.get("name");
    if (!connectorName.isEmpty() && isValidSnowflakeObjectIdentifier
      (connectorName))
    {
      Utils.setAppName(connectorName);
    }
    else
    {
      LOGGER.error("name: {} is empty or invalid. " +
        "It should match Snowflake object identifier syntax. " +
        "Please see the documentation.", connectorName);
      configIsValid = false;
    }

    // validate offset.flush.interval.ms -- a Connect cluster setting
    // default : 60000 ms
    // Interval at which to try committing offsets for tasks
    if (config.containsKey(SnowflakeSinkConnectorConfig
      .OFFSET_FLUSH_INTERVAL_MS)) //offset.flush.interval.ms
    {
      long ofim = Long.parseLong(config.get(SnowflakeSinkConnectorConfig
        .OFFSET_FLUSH_INTERVAL_MS)); //offset.flush.interval.ms
      if (ofim < 60000)   // 60 seconds
      {
        LOGGER.error("{} is too low at {}. " +
            "It must be 60000 ms or larger.",
          SnowflakeSinkConnectorConfig.OFFSET_FLUSH_INTERVAL_MS, ofim);
        configIsValid = false;
      }
    }
    else
    {
      config.put(SnowflakeSinkConnectorConfig.OFFSET_FLUSH_INTERVAL_MS,
        "60000");
      LOGGER.info("{} set to default 60000ms.",
        SnowflakeSinkConnectorConfig.OFFSET_FLUSH_INTERVAL_MS);
    }

    // validate offset.flush.timeout.ms -- a Connect cluster setting
    // default : 60000 ms
    // Maximum number of milliseconds to wait for records to flush and
    // partition offset data to be committed to offset storage before
    // cancelling the process and
    // restoring the offset data to be committed in a future attempt
    if (config.containsKey(SnowflakeSinkConnectorConfig
      .OFFSET_FLUSH_TIMEOUT_MS)) //offset.flush.timeout.ms
    {
      long oftm = Long.parseLong(config.get(SnowflakeSinkConnectorConfig
        .OFFSET_FLUSH_TIMEOUT_MS));
      if (oftm < 60000)   // 60 seconds
      {
        LOGGER.error("{} is too low at {}. " +
            "It must be 60000 ms or larger.",
          SnowflakeSinkConnectorConfig.OFFSET_FLUSH_TIMEOUT_MS, oftm);
        configIsValid = false;
      }
    }
    else
    {
      config.put(SnowflakeSinkConnectorConfig.OFFSET_FLUSH_TIMEOUT_MS, "60000");
      LOGGER.info("{} set to default 60000ms.",
        SnowflakeSinkConnectorConfig.OFFSET_FLUSH_TIMEOUT_MS);
    }

    // set buffer.count.records -- a Snowflake connector setting
    // default : 10000 records
    // Number of records buffered in memory per partition before ingesting to
    // Snowflake
    if (!config.containsKey(SnowflakeSinkConnectorConfig
      .BUFFER_COUNT_RECORDS))//buffer.count.records
    {
      config.put(SnowflakeSinkConnectorConfig.BUFFER_COUNT_RECORDS, "10000");
      LOGGER.info("{} set to default 10000 records.",
        SnowflakeSinkConnectorConfig.BUFFER_COUNT_RECORDS);
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
        LOGGER.error("{} is too high at {}. It must be 100000000 (100MB) or " +
            "smaller.",
          SnowflakeSinkConnectorConfig.BUFFER_SIZE_BYTES, bsb);
        configIsValid = false;
      }
    }
    else
    {
      config.put(SnowflakeSinkConnectorConfig.BUFFER_SIZE_BYTES, "5000000");
      LOGGER.info("{} set to default 5000000 bytes.",
        SnowflakeSinkConnectorConfig.BUFFER_SIZE_BYTES);
    }


    /*** NOTE : we are using offset.flush.interval.ms to act as the buffer
     * .time.ms
     // set buffer.time.ms -- a Snowflake connector setting
     // default : 60000 ms
     // Maximum time to wait for new records per partition before ingesting
     to Snowflake
     ***/
    // validate topics
    if (config.containsKey("topics"))
    {
      topics = new ArrayList<>(Arrays.asList(config.get("topics").split(",")));

      // check for duplicates
      HashSet<String> topicsHashSet = new HashSet<>(Arrays.asList(config.get
        ("topics").split(",")));
      if (topics.size() != topicsHashSet.size())
      {
        LOGGER.error("topics: {} contains duplicate entries.", config.get
          ("topics"));
        configIsValid = false;
      }
    }
    else
    {
      LOGGER.error("topics cannot be empty.");
      configIsValid = false;
    }

    // validate topics.tables.map (optional parameter)
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
          LOGGER.error("Invalid {} config format: {}",
            SnowflakeSinkConnectorConfig.TOPICS_TABLES_MAP, topicsTables);
          configIsValid = false;
        }

        if (!isValidSnowflakeObjectIdentifier(tt[1]))
        {
          LOGGER.error(
            "table name {} should have at least 2 characters, start with " +
              "_a-zA-Z, and only contains _$a-zA-z0-9", tt[1]);
          configIsValid = false;
        }

        if (!topics.contains(tt[0]))
        {
          LOGGER.error("topic name {} has not been registered in this task",
            tt[0]);
          configIsValid = false;
        }

        if (topicsTablesMap.containsKey(tt[0]))
        {
          LOGGER.error("topic name {} is duplicated", tt[0]);
          configIsValid = false;
        }

        if (topicsTablesMap.containsKey(tt[1]))
        {
          //todo: support multiple topics map to one table ?
          LOGGER.error("table name {} is duplicated", tt[1]);
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
          LOGGER.error("topic: {} in topics: {} config should either match " +
              "Snowflake object identifier syntax, " +
              "or topics.tables.map: {} config should contain a mapping " +
              "table name.",
            topic, config.get("topics"), config.get("topics.tables.map"));
          configIsValid = false;
        }
      }
    }

    // sanity check
    if (!config.containsKey("snowflake.database.name"))
    {
      LOGGER.error("snowflake.database.name cannot be empty.");
      configIsValid = false;
    }

    // sanity check
    if (!config.containsKey("snowflake.schema.name"))
    {
      LOGGER.error("snowflake.schema.name cannot be empty.");
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
    return objName.matches("^[_a-zA-Z]{1}[_$a-zA-Z0-9]+");
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
    LOGGER.info("SnowflakeSinkConnector:stop");

    // Get the list of topics and drop corresponding internal stages.
    // Ensure that each file in the internal stage has been ingested, OR
    // copied to 'table stage' and an error message is logged.
    for (String topic : topics)
    {
      String stageName = Utils.stageName(topicsTablesMap.get(topic));
      try
      {
        if (snowflakeConnection.stageExist(stageName))
        {
          LOGGER.info("Dropping internal stage: {} (if empty).", stageName);
          snowflakeConnection.dropStageIfEmpty(stageName);
        }
        else
        {
          // User should not mess up with these stages outside the connector
          String errorMsg = "Attempting to drop a non-existant internal stage" +
            " " + stageName + ". " +
            "This should not happen. " +
            "This stage may have been manually dropped, which may affect " +
            "ingestion guarantees.";
          LOGGER.error(errorMsg);
          telemetry.reportKafkaNonFatalError(errorMsg, connectorName);

          // continue-on-error in the stop method to cleanup other objects
        }
      } catch (Exception ex)
      {
        String errorMsg = "Failed to drop empty internal stage: " + stageName
          + ". " +
          "It can safely be removed. " +
          "Exception: " + ex.toString();
        LOGGER.error(errorMsg);
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
          LOGGER.info("Sleeping 5000ms to allow setup to complete.");
          Thread.sleep(5000);
        } catch (InterruptedException ex)
        {
          LOGGER.warn("Waiting for setup to complete got interrupted");
        }
      }
    }
    if (!setupComplete)
    {
      String errorMsg = "SnowflakeSinkConnector timed out. " +
        "Tables or stages are not yet available for data ingestion to start" +
        ". " +
        "If this persists, please contact Snowflake support.";
      LOGGER.error(errorMsg);
      telemetry.reportKafkaFatalError(errorMsg, connectorName);

      // stop the connector as we are unlikely to automatically recover from
      // this error
      throw new ConnectException(errorMsg);
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
