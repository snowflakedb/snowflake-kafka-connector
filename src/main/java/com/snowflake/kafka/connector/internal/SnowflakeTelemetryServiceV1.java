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
  private static final String START_TIME = "start_time";
  private static final String END_TIME = "end_time";
  private static final String MAX_TASKS = "max_tasks";
  private static final String APP_NAME = "app_name";
  private static final String TASK_ID = "task_id";
  private static final String ERROR_NUMBER = "error_number";
  private static final String TIME = "time";
  private static final String VERSION = "version";
  private static final String KAFKA_VERSION = "kafka_version";
  private static final String BACKOFF_TIME_BEFORE_EXECUTE = "backoff_time_before_execute";


  private final Telemetry telemetry;
  private String name = null;
  private String taskID = null;

  static class SnowflakeBasicInfo
  {
    String tableName;
    String stageName;
    String pipeName;
    static final String TABLE_NAME = "table_name";
    static final String STAGE_NAME = "stage_name";
    static final String PIPE_NAME  = "pipe_name";
  }

  // This object is send every minute for each pipe
  static class SnowflakePipeStatus extends SnowflakeBasicInfo
  {
    // Offset info
    long processedOffset;           // processed offset (offset that is most recent in buffer)
    long flushedOffset;             // flushed offset (files on stage)
    long committedOffset;           // loaded offset (files being ingested)
    long purgedOffset;              // purged offset (files purged or moved to table stage)
    static final String PROCESSED_OFFSET  = "processed_offset";
    static final String FLUSHED_OFFSET    = "flushed_offset";
    static final String COMMITTED_OFFSET  = "committed_offset";
    static final String PURGED_OFFSET     = "purged_offset";

    // File count info
    int fileCountOnStage;               // files that are currently on stage
    int fileCountOnIngestion;           // files that are being ingested
    int fileCountPurged;                // files that are purged
    int fileCountTableStage;            // files that are moved to table stage
    static final String FILE_COUNT_ON_STAGE       = "file_count_on_stage";
    static final String FILE_COUNT_ON_INGESTION   = "file_count_on_ingestion";
    static final String FILE_COUNT_PURGED         = "file_count_purged";
    static final String FILE_COUNT_TABLE_STAGE    = "file_count_table_stage";

    // Cleaner restart count
    int cleanerRestartCount;          // how many times the cleaner restarted
    static final String CLEANER_RESTART_COUNT   = "cleaner_restart_count";

    // Average lag of Kafka
    long averageKafkaLag;                                // average lag on Kafka side
    int averageKafkaLagRecordCount;                      // record count
    static final String AVERAGE_KAFKA_LAG                = "average_kafka_lag";
    static final String AVERAGE_KAFKA_LAG_RECORD_COUNT   = "average_kafka_lag_record_count";

    // Average lag of ingestion
    long averageIngestionLag;                             // average lag on Ingestion side
    int averageIngestionLagFileCount;                     // file count
    static final String AVERAGE_INGESTION_LAG             = "average_ingestion_lag";
    static final String AVERAGE_INGESTION_LAG_FILE_COUNT  = "average_ingestion_lag_file_count";

    // Legacy metrix
    long totalNumberOfRecord;
    long totalSizeOfData;
    static final String RECORD_NUMBER = "record_number";
    static final String BYTE_NUMBER = "byte_number";

    long startTime;
    long endTime;

    SnowflakePipeStatus(final String tableName, final String stageName, final String pipeName)
    {
      this.tableName = tableName;
      this.stageName = stageName;
      this.pipeName = pipeName;
    }

    void dumpTo(ObjectNode msg)
    {
      msg.put(TABLE_NAME, tableName);
      msg.put(STAGE_NAME, stageName);
      msg.put(PIPE_NAME, pipeName);

      msg.put(PROCESSED_OFFSET, processedOffset);
      msg.put(FLUSHED_OFFSET, flushedOffset);
      msg.put(COMMITTED_OFFSET, committedOffset);
      msg.put(PURGED_OFFSET, purgedOffset);
      msg.put(FILE_COUNT_ON_STAGE, fileCountOnStage);
      msg.put(FILE_COUNT_ON_INGESTION, fileCountOnIngestion);
      msg.put(FILE_COUNT_PURGED, fileCountPurged);
      msg.put(FILE_COUNT_TABLE_STAGE, fileCountTableStage);
      msg.put(CLEANER_RESTART_COUNT, cleanerRestartCount);
      msg.put(AVERAGE_KAFKA_LAG, averageKafkaLag);
      msg.put(AVERAGE_KAFKA_LAG_RECORD_COUNT, averageKafkaLagRecordCount);
      msg.put(AVERAGE_INGESTION_LAG, averageIngestionLag);
      msg.put(AVERAGE_INGESTION_LAG_FILE_COUNT, averageIngestionLagFileCount);
      msg.put(RECORD_NUMBER, totalNumberOfRecord);
      msg.put(BYTE_NUMBER, totalSizeOfData);
      msg.put(START_TIME, startTime);
      msg.put(END_TIME, endTime);
    }
  }

  // This object is send only once when pipe starts
  static class SnowflakeObjectCreation extends SnowflakeBasicInfo
  {
    boolean isReuseTable;                          // is the create reusing existing table
    boolean isReuseStage;                          // is the create reusing existing stage
    boolean isReusePipe;                           // is the create reusing existing pipe
    int fileCountRestart;                          // files on stage when cleaner starts
    int fileCountReprocessPurge;                   // files on stage that are purged due to reprocessing when cleaner starts
    static final String IS_REUSE_TABLE             = "is_reuse_table";
    static final String IS_REUSE_STAGE             = "is_reuse_stage";
    static final String IS_REUSE_PIPE              = "is_reuse_pipe";
    static final String FILE_COUNT_RESTART         = "file_count_restart";
    static final String FILE_COUNT_REPROCESS_PURGE = "file_count_reprocess_purge";

    SnowflakeObjectCreation(final String tableName, final String stageName, final String pipeName)
    {
      this.tableName = tableName;
      this.stageName = stageName;
      this.pipeName = pipeName;
    }

    void dumpTo(ObjectNode msg)
    {
      msg.put(TABLE_NAME, tableName);
      msg.put(STAGE_NAME, stageName);
      msg.put(PIPE_NAME, pipeName);

      msg.put(IS_REUSE_TABLE, isReuseTable);
      msg.put(IS_REUSE_STAGE, isReuseStage);
      msg.put(IS_REUSE_PIPE, isReusePipe);
      msg.put(FILE_COUNT_RESTART, fileCountRestart);
      msg.put(FILE_COUNT_REPROCESS_PURGE, fileCountReprocessPurge);
    }
  }


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
  public void reportKafkaPipeUsage(final SnowflakePipeStatus pipeStatus)
  {

    ObjectNode msg = getObjectNode();

    pipeStatus.dumpTo(msg);

    send(TelemetryType.KAFKA_PIPE_USAGE, msg);
  }

  @Override
  public void reportKafkaPipeStart(final SnowflakeObjectCreation objectCreation)
  {
    ObjectNode msg = getObjectNode();

    objectCreation.dumpTo(msg);

    send(TelemetryType.KAFKA_PIPE_START, msg);
  }

  private void send(TelemetryType type, JsonNode data)
  {
    send(type, data, true);
  }

  private void send(TelemetryType type, JsonNode data, boolean flush)
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
      if (flush)
      {
        telemetry.sendBatchAsync();
      }
    } catch (Exception e)
    {
      logError("Failed to send telemetry data: {}, Error: {}", data.toString(), e.getMessage());
    }
  }

  @Override
  public void flushTelemetry()
  {
    try
    {
      telemetry.sendBatchAsync();
    } catch (Exception e)
    {
      logError("Failed to send telemetry data, Error: {}", e.getMessage());
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
