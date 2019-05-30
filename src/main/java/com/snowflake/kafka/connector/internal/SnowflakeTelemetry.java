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
package com.snowflake.kafka.connector.internal;

import com.snowflake.kafka.connector.Utils;
import net.snowflake.client.jdbc.SnowflakeConnectionV1;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind
    .ObjectMapper;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.node
    .ArrayNode;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.node
    .ObjectNode;
import net.snowflake.client.jdbc.telemetry.Telemetry;
import net.snowflake.client.jdbc.telemetry.TelemetryData;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * Snowflake Telemetry API for Kafka Connector
 */
public class SnowflakeTelemetry extends Logging
{
  SnowflakeConnectionV1 conn;

  Telemetry telemetry;

  LinkedList<TelemetryData> logs;

  private static final ObjectMapper mapper = new ObjectMapper();

  //constant string list
  private static final String SOURCE = "source";
  private static final String TYPE = "type";
  private static final String KAFKA_CONNECTOR = "kafka_connector";
  private static final String DATA = "data";
  private static final String IS_RESTART = "is_restart";
  private static final String START_TIME = "start_time";
  private static final String END_TIME = "end_time";
  private static final String NUM_TOPICS = "num_topics";
  private static final String NUM_MAPPED_TABLES = "num_mapped_tables";
  private static final String MAX_TASKS = "max_tasks";
  private static final String APP_NAME = "app_name";
  private static final String RECORD_NUMBER = "record_number";
  private static final String BYTE_NUMBER = "byte_number";
  private static final String ERROR_NUMBER = "error_number";
  private static final String TABLE_NAME = "table_name";
  private static final String STAGE_NAME = "stage_name";
  private static final String PIPE_NAME = "pipe_name";
  private static final String TIME = "time";
  private static final String FILE_LIST = "file_list";


  private enum TelemetryType
  {
    KAFKA_START("kafka_start"),

    KAFKA_STOP("kafka_stop"),

    KAFKA_FATAL_ERROR("kafka_fatal_error"),

    KAFKA_NONFATAL_ERROR("kafka_nonfatal_error"),

    KAFKA_USAGE("kafka_usage"),

    KAFKA_CREATE_TABLE("kafka_create_table"),

    KAFKA_REUSE_TABLE("kafka_reuse_table"),

    KAFKA_CREATE_STAGE("kafka_create_stage"),

    KAFKA_REUSE_STAGE("kafka_reuse_stage"),

    KAFKA_CREATE_PIPE("kafka_create_pipe"),

    KAFKA_FILE_FAILED("kafka_file_failed");

    private final String name;

    TelemetryType(String name)
    {
      this.name = name;
    }

    @Override
    public String toString()
    {
      return this.name;
    }
  }

  /**
   * Constructor, used by SnowflakeJDBCWrapper only
   *
   * @param conn snowflake connection
   */
  public SnowflakeTelemetry(SnowflakeConnectionV1 conn)
  {
    this.conn = conn;

    this.telemetry = Telemetry.createTelemetry(conn);

    logs = new LinkedList<>();
  }

  //list all telemetry interface method here

  /**
   * Event of connector start
   * @param startTime task start time
   * @param numTopics number of topics
   * @param numMappedTables number of tables
   * @param maxTasks max number of tasks
   * @param appName instance name
   */
  public void reportKafkaStart(long startTime, int numTopics, int numMappedTables,
                        int maxTasks, String appName)
  {
    ObjectNode msg = mapper.createObjectNode();

    msg.put(START_TIME, startTime);

    msg.put(NUM_TOPICS, numTopics);

    msg.put(NUM_MAPPED_TABLES, numMappedTables);

    msg.put(MAX_TASKS, maxTasks);

    msg.put(APP_NAME, appName);

    addLog(TelemetryType.KAFKA_START, msg);

    //send telemetry to server directly
    send();

  }

  /**
   * Event of connector stop
   *
   * @param startTime start timestamp
   * @param appName   application name
   */
  public void reportKafkaStop(long startTime, String appName)
  {
    ObjectNode msg = mapper.createObjectNode();

    msg.put(START_TIME, startTime);

    msg.put(END_TIME, System.currentTimeMillis());

    msg.put(APP_NAME, appName);

    addLog(TelemetryType.KAFKA_STOP, msg);

    //send telemetry to server directly
    send();
  }

  /**
   * Event of a fatal error in the connector
   *
   * @param errorNumber error message
   * @param appName     application name
   */
  public void reportKafkaFatalError(String errorNumber, String appName)
  {
    ObjectNode msg = mapper.createObjectNode();

    msg.put(TIME, System.currentTimeMillis());

    msg.put(ERROR_NUMBER, errorNumber);

    msg.put(APP_NAME, appName);

    addLog(TelemetryType.KAFKA_FATAL_ERROR, msg);

    //send telemetry to server directly
    send();
  }

  /**
   * Event of a non fatal error in the connector
   *
   * @param errorNumber error message
   * @param appName     application name
   */
  public void reportKafkaNonFatalError(String errorNumber, String appName)
  {
    ObjectNode msg = mapper.createObjectNode();

    msg.put(TIME, System.currentTimeMillis());

    msg.put(ERROR_NUMBER, errorNumber);

    msg.put(APP_NAME, appName);

    addLog(TelemetryType.KAFKA_NONFATAL_ERROR, msg);

    //send telemetry to server directly
    send();
  }

  /**
   * report connector usage
   *
   * @param startTime    start time of reported period
   * @param endTime      end time of reported period
   * @param recordNumber number of records sent to SF
   * @param byteNumber   number of bytes sent to SF
   * @param appName      application name
   */
  public void reportKafkaUsage(long startTime, long endTime,
                        long recordNumber, long byteNumber, String appName)
  {
    ObjectNode msg = mapper.createObjectNode();

    msg.put(APP_NAME, appName);

    msg.put(START_TIME, startTime);

    msg.put(END_TIME, endTime);

    msg.put(RECORD_NUMBER, recordNumber);

    msg.put(BYTE_NUMBER, byteNumber);

    addLog(TelemetryType.KAFKA_USAGE, msg);

    //send telemetry to server directly
    send();

  }

  /**
   * report table creation
   *
   * @param tableName table name
   * @param appName   application name
   */
  public void reportKafkaCreateTable(String tableName, String appName)
  {
    ObjectNode msg = mapper.createObjectNode();

    msg.put(APP_NAME, appName);

    msg.put(TABLE_NAME, tableName);

    msg.put(TIME, System.currentTimeMillis());

    addLog(TelemetryType.KAFKA_CREATE_TABLE, msg);

    //send telemetry to server directly
    send();
  }

  /**
   * report table creation
   *
   * @param tableName table name
   * @param appName   application name
   */
  public void reportKafkaReuseTable(String tableName, String appName)
  {
    ObjectNode msg = mapper.createObjectNode();

    msg.put(APP_NAME, appName);

    msg.put(TABLE_NAME, tableName);

    msg.put(TIME, System.currentTimeMillis());

    addLog(TelemetryType.KAFKA_REUSE_TABLE, msg);

    //send telemetry to server directly
    send();
  }

  /**
   * report stage creation
   *
   * @param stageName stage name
   * @param appName   application name
   */
  public void reportKafkaCreateStage(String stageName, String appName)
  {
    ObjectNode msg = mapper.createObjectNode();

    msg.put(APP_NAME, appName);

    msg.put(STAGE_NAME, stageName);

    msg.put(TIME, System.currentTimeMillis());

    addLog(TelemetryType.KAFKA_CREATE_STAGE, msg);

    //send telemetry to server directly
    send();
  }

  /**
   * report stage reuse
   *
   * @param stageName stage name
   * @param appName   application name
   */
  public void reportKafkaReuseStage(String stageName, String appName)
  {
    ObjectNode msg = mapper.createObjectNode();

    msg.put(APP_NAME, appName);

    msg.put(STAGE_NAME, stageName);

    msg.put(TIME, System.currentTimeMillis());

    addLog(TelemetryType.KAFKA_REUSE_STAGE, msg);

    //send telemetry to server directly
    send();
  }

  /**
   * report pipe creation
   *
   * @param pipeName  pipe name
   * @param stageName stage name
   * @param tableName table name
   * @param appName   application name
   */
  public void reportKafkaCreatePipe(String pipeName, String stageName, String
      tableName,
                             String appName)
  {
    ObjectNode msg = mapper.createObjectNode();

    msg.put(APP_NAME, appName);

    msg.put(PIPE_NAME, pipeName);

    msg.put(TABLE_NAME, tableName);

    msg.put(STAGE_NAME, stageName);

    msg.put(TIME, System.currentTimeMillis());

    addLog(TelemetryType.KAFKA_CREATE_PIPE, msg);

    //send telemetry to server directly
    send();

  }


  /**
   * report file failed
   *
   * @param tableName table name
   * @param stageName stage name
   * @param filenames a list of file names
   * @param appName   application name
   */
  public void reportKafkaFileFailed(String tableName, String stageName,
                             List<String> filenames, String appName)
  {
    ObjectNode msg = mapper.createObjectNode();

    msg.put(TABLE_NAME, tableName);

    msg.put(STAGE_NAME, stageName);

    msg.put(TIME, System.currentTimeMillis());

    msg.put(APP_NAME, appName);

    ArrayNode names = msg.putArray(FILE_LIST);

    filenames.forEach(names::add);

    addLog(TelemetryType.KAFKA_FILE_FAILED, msg);

    //send telemetry to server directly
    send();

  }

  /**
   * insert a telemetry record to cache list
   *
   * @param type telemetry type
   * @param data message content
   */
  private void addLog(TelemetryType type, ObjectNode data)
  {
    ObjectNode msg = mapper.createObjectNode();

    msg.put(SOURCE, KAFKA_CONNECTOR);

    msg.put(TYPE, type.toString());

    msg.set(DATA, data);

    addLogToCacheList(msg, Utils.currentTime());
  }

  //generally interface

  /**
   * add one telemetry record into cache list
   *
   * @param log       log message
   * @param timestamp timestamp
   */
  public void addLogToCacheList(ObjectNode log, long timestamp)
  {
    synchronized (this)
    {
      logs.add(new TelemetryData(log, timestamp));
    }
  }

  /**
   * add one telemetry record into cache list
   *
   * @param data telemetry record object
   */
  public void addLogToCacheList(TelemetryData data)
  {
    synchronized (this)
    {
      logs.add(data);
    }
  }

  /**
   * send all cached telemetry data to server
   *
   * @return true if succeed, false otherwise
   */
  public boolean send()
  {
    LinkedList<TelemetryData> list;

    synchronized (this)
    {
      list = logs;

      logs = new LinkedList<>();
    }

    try
    {
      for (TelemetryData data : list)
      {
        telemetry.addLogToBatch(data);

        logDebug("sending telemetry data: {}", data.toString());
      }

      telemetry.sendBatch();
    } catch (IOException e)
    {
      logError("sending telemetry failed: {}", e.getMessage());

      return false;
    }

    return true;
  }

}
