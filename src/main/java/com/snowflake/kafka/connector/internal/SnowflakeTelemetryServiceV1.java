package com.snowflake.kafka.connector.internal;

import com.snowflake.kafka.connector.Utils;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.JsonNode;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.ObjectMapper;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.node.ArrayNode;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.node.ObjectNode;
import net.snowflake.client.jdbc.telemetry.Telemetry;
import net.snowflake.client.jdbc.telemetry.TelemetryClient;
import net.snowflake.client.jdbc.telemetry.TelemetryData;
import net.snowflake.client.jdbc.telemetry.TelemetryUtil;
import org.apache.kafka.common.utils.AppInfoParser;

import java.sql.Connection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class SnowflakeTelemetryServiceV1 extends Logging implements SnowflakeTelemetryService
{
  private static final ObjectMapper MAPPER = new ObjectMapper();

  //constant string list
  private static final String SOURCE = "source";
  private static final String TYPE = "type";
  private static final String KAFKA_CONNECTOR = "kafka_connector";
  private static final String DATA = "data";
  private static final String MAX_TASKS = "max_tasks";
  private static final String START_TIME = "start_time";
  private static final String END_TIME = "end_time";
  private static final String APP_NAME = "app_name";
  private static final String TASK_ID = "task_id";
  private static final String ERROR_NUMBER = "error_number";
  private static final String TIME = "time";
  private static final String VERSION = "version";
  private static final String KAFKA_VERSION = "kafka_version";
  private static final String IS_PIPE_CLOSING = "is_pipe_closing";


  private final Telemetry telemetry;
  private String name = null;
  private String taskID = null;

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
  public void setTaskID(final String taskID)
  {
    this.taskID = taskID;
  }

  private ObjectNode getObjectNode()
  {
    ObjectNode msg = MAPPER.createObjectNode();
    msg.put(APP_NAME, getAppName());
    msg.put(TASK_ID, getTaskID());
    return msg;
  }

  @Override
  public void reportKafkaStart(final long startTime, final int maxTasks)
  {
    ObjectNode msg = getObjectNode();

    msg.put(START_TIME, startTime);
    msg.put(MAX_TASKS, maxTasks);
    msg.put(KAFKA_VERSION, AppInfoParser.getVersion());

    send(TelemetryType.KAFKA_START, msg);
  }

  @Override
  public void reportKafkaStop(final long startTime)
  {
    ObjectNode msg = getObjectNode();

    msg.put(START_TIME, startTime);
    msg.put(END_TIME, System.currentTimeMillis());

    send(TelemetryType.KAFKA_STOP, msg);
  }

  @Override
  public void reportKafkaFatalError(final String errorDetail)
  {
    ObjectNode msg = getObjectNode();

    msg.put(TIME, System.currentTimeMillis());
    msg.put(ERROR_NUMBER, errorDetail);

    send(TelemetryType.KAFKA_FATAL_ERROR, msg);
  }

  @Override
  public void reportKafkaPipeUsage(final SnowflakeTelemetryPipeStatus pipeStatus, boolean isClosing)
  {
    if (pipeStatus.empty())
    {
      return;
    }
    ObjectNode msg = getObjectNode();

    pipeStatus.dumpTo(msg);
    msg.put(IS_PIPE_CLOSING, isClosing);

    send(TelemetryType.KAFKA_PIPE_USAGE, msg);
  }

  @Override
  public void reportKafkaPipeStart(final SnowflakeTelemetryPipeCreation pipeCreation)
  {
    ObjectNode msg = getObjectNode();

    pipeCreation.dumpTo(msg);

    send(TelemetryType.KAFKA_PIPE_START, msg);
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
      telemetry.addLogToBatch(TelemetryUtil.buildJobData(msg));
      logDebug("sending telemetry data: {}", data.toString());
      telemetry.sendBatchAsync();
    } catch (Exception e)
    {
      logError("Failed to send telemetry data: {}, Error: {}", data.toString(), e.getMessage());
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

  private String getTaskID()
  {
    if (taskID == null || taskID.isEmpty())
    {
      logWarn("taskID in telemetry service is empty");
      return "empty_taskID";
    }
    return taskID;
  }

  private enum TelemetryType
  {
    KAFKA_START("kafka_start"),
    KAFKA_STOP("kafka_stop"),
    KAFKA_FATAL_ERROR("kafka_fatal_error"),
    KAFKA_PIPE_USAGE("kafka_pipe_usage"),
    KAFKA_PIPE_START("kafka_pipe_start");

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
