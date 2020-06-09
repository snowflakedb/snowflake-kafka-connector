package com.snowflake.kafka.connector.internal;

import com.snowflake.kafka.connector.Utils;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.JsonNode;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.ObjectMapper;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.node.ArrayNode;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.node.ObjectNode;
import net.snowflake.client.jdbc.telemetry.Telemetry;
import net.snowflake.client.jdbc.telemetry.TelemetryClient;
import net.snowflake.client.jdbc.telemetry.TelemetryData;

import java.io.IOException;
import java.sql.Connection;
import java.util.List;

public class SnowflakeTelemetryServiceV1 extends Logging implements SnowflakeTelemetryService
{
  private static final ObjectMapper MAPPER = new ObjectMapper();

  //constant string list
  private static final String SOURCE = "source";
  private static final String TYPE = "type";
  private static final String KAFKA_CONNECTOR = "kafka_connector";
  private static final String DATA = "data";
  private static final String START_TIME = "start_time";
  private static final String END_TIME = "end_time";
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
  private static final String VERSION = "version";
  private static final String BACKOFF_TIME_BEFORE_EXECUTE = "backoff_time_before_execute";


  private final Telemetry telemetry;
  private String name = null;

  SnowflakeTelemetryServiceV1(Connection conn)
  {
    this.telemetry = TelemetryClient.createTelemetry(conn);
  }

  @Override
  public void setAppName(final String name)
  {
    this.name = name;
  }

  @Override
  public void reportKafkaStart(final long startTime, final int maxTasks)
  {
    ObjectNode msg = MAPPER.createObjectNode();

    msg.put(START_TIME, startTime);
    msg.put(MAX_TASKS, maxTasks);
    msg.put(APP_NAME, getAppName());

    send(TelemetryType.KAFKA_START, msg);
  }

  @Override
  public void reportKafkaStop(final long startTime)
  {
    ObjectNode msg = MAPPER.createObjectNode();
    msg.put(START_TIME, startTime);
    msg.put(END_TIME, System.currentTimeMillis());
    msg.put(APP_NAME, getAppName());

    send(TelemetryType.KAFKA_STOP, msg);
  }

  @Override
  public void reportKafkaFatalError(final String errorDetail)
  {
    ObjectNode msg = MAPPER.createObjectNode();
    msg.put(TIME, System.currentTimeMillis());
    msg.put(ERROR_NUMBER, errorDetail);
    msg.put(APP_NAME, getAppName());

    send(TelemetryType.KAFKA_FATAL_ERROR, msg);
  }

  @Override
  public void reportKafkaNonFatalError(final String errorDetail)
  {
    ObjectNode msg = MAPPER.createObjectNode();
    msg.put(TIME, System.currentTimeMillis());
    msg.put(ERROR_NUMBER, errorDetail);
    msg.put(APP_NAME, getAppName());

    send(TelemetryType.KAFKA_NONFATAL_ERROR, msg);
  }

  @Override
  public void reportKafkaUsage(final long startTime, final long endTime,
                               final long recordNumber, final long byteNumber)
  {

    ObjectNode msg = MAPPER.createObjectNode();
    msg.put(APP_NAME, getAppName());
    msg.put(START_TIME, startTime);
    msg.put(END_TIME, endTime);
    msg.put(RECORD_NUMBER, recordNumber);
    msg.put(BYTE_NUMBER, byteNumber);

    send(TelemetryType.KAFKA_USAGE, msg);
  }

  @Override
  public void reportKafkaCreateTable(final String tableName)
  {
    ObjectNode msg = MAPPER.createObjectNode();
    msg.put(APP_NAME, getAppName());
    msg.put(TABLE_NAME, tableName);
    msg.put(TIME, System.currentTimeMillis());

    send(TelemetryType.KAFKA_CREATE_TABLE, msg);
  }

  @Override
  public void reportKafkaReuseTable(final String tableName)
  {
    ObjectNode msg = MAPPER.createObjectNode();
    msg.put(APP_NAME, getAppName());
    msg.put(TABLE_NAME, tableName);
    msg.put(TIME, System.currentTimeMillis());

    send(TelemetryType.KAFKA_REUSE_TABLE, msg);
  }

  @Override
  public void reportKafkaCreateStage(final String stageName)
  {
    ObjectNode msg = MAPPER.createObjectNode();
    msg.put(APP_NAME, getAppName());
    msg.put(STAGE_NAME, stageName);
    msg.put(TIME, System.currentTimeMillis());

    send(TelemetryType.KAFKA_CREATE_STAGE, msg);
  }

  @Override
  public void reportKafkaReuseStage(final String stageName)
  {
    ObjectNode msg = MAPPER.createObjectNode();
    msg.put(APP_NAME, getAppName());
    msg.put(STAGE_NAME, stageName);
    msg.put(TIME, System.currentTimeMillis());

    send(TelemetryType.KAFKA_REUSE_STAGE, msg);
  }

  @Override
  public void reportKafkaCreatePipe(final String tableName,
                                    final String stageName,
                                    final String pipeName)
  {
    ObjectNode msg = MAPPER.createObjectNode();
    msg.put(APP_NAME, getAppName());
    msg.put(PIPE_NAME, pipeName);
    msg.put(TABLE_NAME, tableName);
    msg.put(STAGE_NAME, stageName);
    msg.put(TIME, System.currentTimeMillis());

    send(TelemetryType.KAFKA_CREATE_PIPE, msg);

  }

  @Override
  public void reportKafkaFileFailure(final String tableName,
                                     final String stageName,
                                     final List<String> filenames)
  {
    ObjectNode msg = MAPPER.createObjectNode();
    msg.put(TABLE_NAME, tableName);
    msg.put(STAGE_NAME, stageName);
    msg.put(TIME, System.currentTimeMillis());
    msg.put(APP_NAME, getAppName());
    ArrayNode names = msg.putArray(FILE_LIST);
    filenames.forEach(names::add);

    send(TelemetryType.KAFKA_FILE_FAILED, msg);
  }

  @Override
  public void reportKafkaSnowflakeThrottle(final String errorDetail, int iteration)
  {
    ObjectNode msg = MAPPER.createObjectNode();
    msg.put(ERROR_NUMBER, errorDetail);
    msg.put(TIME, System.currentTimeMillis());
    msg.put(APP_NAME, getAppName());
    msg.put(BACKOFF_TIME_BEFORE_EXECUTE, iteration);

    send(TelemetryType.KAFKA_SNOWFLAKE_THROTTLE, msg);
  }

  private void send(TelemetryType type, JsonNode data)
  {
    ObjectNode msg = MAPPER.createObjectNode();
    msg.put(SOURCE, KAFKA_CONNECTOR);
    msg.put(TYPE, type.toString());
    msg.set(DATA, data);
    msg.put(VERSION, Utils.VERSION); //version number
    try
    {
      telemetry.addLogToBatch(new TelemetryData(msg, System.currentTimeMillis()));
      logDebug("sending telemetry data: {}", data.toString());
      telemetry.sendBatchAsync();
    } catch (Exception e)
    {
      logError("Failed to send telemetry data: {}", data.toString());
    }
  }

  private String getAppName()
  {
    if (name == null || name.isEmpty())
    {
      logWarn("appName in telemetry service is empty");
      return "empty_appName";
    }
    return name;
  }

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
    KAFKA_FILE_FAILED("kafka_file_failed"),
    KAFKA_SNOWFLAKE_THROTTLE("kafka_snowflake_throttle");

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
}
