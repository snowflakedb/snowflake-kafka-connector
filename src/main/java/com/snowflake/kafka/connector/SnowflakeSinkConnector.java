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
import com.snowflake.kafka.connector.internal.SnowflakeConnectionService;
import com.snowflake.kafka.connector.internal.SnowflakeConnectionServiceFactory;
import com.snowflake.kafka.connector.internal.SnowflakeErrors;
import com.snowflake.kafka.connector.internal.SnowflakeTelemetryService;
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
  private SnowflakeConnectionService conn;

  // Snowflake Telemetry provides methods to report usage statistics
  private SnowflakeTelemetryService telemetryClient;
  private long connectorStartTime;

  private static final Logger LOGGER = LoggerFactory
    .getLogger(SnowflakeSinkConnector.class);

  // Kafka Connect starts sink tasks without waiting for setup in
  // SnowflakeSinkConnector to finish.
  // This causes race conditions for: config validation, tables and stages
  // creation, etc.
  // Using setupComplete to synchronize
  private boolean setupComplete;

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

    // set buffer.count.records -- a Snowflake connector setting
    // default : 10000 records
    // Number of records buffered in memory per partition before ingesting to
    // Snowflake
    if (!config.containsKey(SnowflakeSinkConnectorConfig.BUFFER_COUNT_RECORDS))
    {
      config.put(SnowflakeSinkConnectorConfig.BUFFER_COUNT_RECORDS,
        SnowflakeSinkConnectorConfig.BUFFER_COUNT_RECORDS_DEFAULT + "");
      LOGGER.info(Logging.logMessage("{} set to default {} records.",
        SnowflakeSinkConnectorConfig.BUFFER_COUNT_RECORDS,
        SnowflakeSinkConnectorConfig.BUFFER_COUNT_RECORDS_DEFAULT));
    }

    // set buffer.size.bytes -- a Snowflake connector setting
    // default : 5000000 bytes
    // Cumulative size of records buffered in memory per partition before
    // ingesting to Snowflake
    if(!config.containsKey(SnowflakeSinkConnectorConfig.BUFFER_SIZE_BYTES))
    {
      config.put(SnowflakeSinkConnectorConfig.BUFFER_SIZE_BYTES,
        SnowflakeSinkConnectorConfig.BUFFER_SIZE_BYTES_DEFAULT + "");
      LOGGER.info(Logging.logMessage("{} set to default {} bytes.",
        SnowflakeSinkConnectorConfig.BUFFER_SIZE_BYTES,
        SnowflakeSinkConnectorConfig.BUFFER_SIZE_BYTES_DEFAULT));
    }

    // set buffer.flush.time -- a Snowflake connector setting
    // default: 30 seconds
    // time to flush cached data
    if(!config.containsKey(SnowflakeSinkConnectorConfig.BUFFER_FLUSH_TIME_SEC))
    {
      config.put(SnowflakeSinkConnectorConfig.BUFFER_FLUSH_TIME_SEC,
        SnowflakeSinkConnectorConfig.BUFFER_FLUSH_TIME_SEC_DEFAULT + "");
      LOGGER.info(Logging.logMessage("{} set to default {} seconds",
        SnowflakeSinkConnectorConfig.BUFFER_FLUSH_TIME_SEC,
        SnowflakeSinkConnectorConfig.BUFFER_FLUSH_TIME_SEC_DEFAULT));
    }

    Utils.ParameterValidationResult result =  Utils.validateConfig(config);

    connectorName = result.connectorName;
    topics = result.topics;
    topicsTablesMap = result.topicsTableMap;

    // create a persisted connection, and validate snowflake connection
    // config as a side effect
    conn = SnowflakeConnectionServiceFactory
      .builder()
      .setProperties(parsedConfig)
      .build();

    telemetryClient = conn.getTelemetryClient();

    // maxTasks value isn't visible if the user leaves it at default. So 0
    // means unknown.
    int maxTasks = (config.containsKey("tasks.max")) ? Integer.parseInt
      (config.get("tasks.max")) : 0;

    telemetryClient.reportKafkaStart(
      connectorStartTime,
      topics.size(),
      topicsTablesMap.size(),
      maxTasks);

    for (String topic : topics)
    {
      // create or validate table
      String table = topicsTablesMap.get(topic);

      try
      {
        if (conn.tableExist(table))
        {
          if (conn.isTableCompatible(table))
          {
            // connector may have been restarted or user has pre-created the
            // correct table
            LOGGER.info(Logging.logMessage("Using existing table {} for topic" +
              " {}.", table, topic));
            telemetryClient.reportKafkaReuseTable(table);
          }
          else
          {
            throw SnowflakeErrors.ERROR_5003.getException("table name: " +
              table, telemetryClient);
          }
        }
        else
        {
          LOGGER.info(Logging.logMessage("Creating new table {} for topic {}.",
            table, topic));
          conn.createTable(table);
        }

        // store topic and table name in config for use by SinkTask and
        // SnowflakeJDBCWrapper
        //todo: refactor it, it is an issue if topic name conflicts with config name.
        config.put(topic, table);
      } catch (Exception e)
      {
        // cleanup
        stop();
        throw SnowflakeErrors.ERROR_5006.getException(e, telemetryClient);
      }


      // create or validate stage
      String stageName = Utils.stageName(connectorName, table);
      try
      {
        if (conn.stageExist(stageName))
        {
          if (conn.isTableCompatible(stageName))
          {
            // connector may have been restarted
            // user is NOT expected to pre-create any internal stages!
            LOGGER.info(Logging.logMessage("Connector recovery: using " +
              "existing internal stage {} for topic {}.", stageName, topic));
            telemetryClient.reportKafkaReuseStage(stageName);
          }
          else
          {
            throw SnowflakeErrors.ERROR_5004.getException("Stage name: " +
              stageName, telemetryClient);
          }
        }
        else
        {
          LOGGER.info(Logging.logMessage("Creating new internal stage {} for " +
            "topic {}.", stageName, topic));
          conn.createStage(stageName);
        }
      } catch (Exception e)
      {
        // cleanup
        stop();
        throw SnowflakeErrors.ERROR_5006.getException(e, telemetryClient);
      }
    }

    setupComplete = true;
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
        if (conn.stageExist(stageName))
        {
          LOGGER.info(Logging.logMessage("Dropping internal stage: {} (if " +
            "empty).", stageName));
          conn.dropStageIfEmpty(stageName);
        }
        else
        {
          // User should not mess up with these stages outside the connector
          String errorMsg = "Attempting to drop a non-existant internal stage" +
            " " + stageName + ". This should not happen. This stage may " +
            "have been manually dropped, which may affect ingestion " +
            "guarantees.";
          LOGGER.error(Logging.logMessage(errorMsg));
          telemetryClient.reportKafkaNonFatalError(errorMsg);

          // continue-on-error in the stop method to cleanup other objects
        }
      } catch (Exception ex)
      {
        String errorMsg = "Failed to drop empty internal stage: " + stageName
          + ". It can safely be removed. Exception: " + ex.toString();
        LOGGER.error(Logging.logMessage(errorMsg));
        telemetryClient.reportKafkaNonFatalError(errorMsg);

        // continue-on-error in the stop method to cleanup other objects
      }

      // TODO (GA): discover if there are any leaked stages and clean them up
      // when possible
      // TODO (GA): discover if there are any leaked pipes and clean them up
      // when possible
      // We are potentially laking pipe objects.

      telemetryClient.reportKafkaStop(System.currentTimeMillis());
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
      throw SnowflakeErrors.ERROR_5007.getException(telemetryClient);
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
