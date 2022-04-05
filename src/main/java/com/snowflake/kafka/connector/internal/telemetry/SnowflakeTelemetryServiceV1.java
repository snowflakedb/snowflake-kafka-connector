package com.snowflake.kafka.connector.internal.telemetry;

import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.BUFFER_COUNT_RECORDS;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.BUFFER_FLUSH_TIME_SEC;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.BUFFER_SIZE_BYTES;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.DELIVERY_GUARANTEE;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.INGESTION_METHOD_DEFAULT_SNOWPIPE;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.INGESTION_METHOD_OPT;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.IngestionDeliveryGuarantee;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.KEY_CONVERTER_CONFIG_FIELD;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.VALUE_CONVERTER_CONFIG_FIELD;

import com.google.common.annotations.VisibleForTesting;
import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.internal.Logging;
import java.sql.Connection;
import java.util.Map;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.JsonNode;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.ObjectMapper;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.node.ObjectNode;
import net.snowflake.client.jdbc.telemetry.Telemetry;
import net.snowflake.client.jdbc.telemetry.TelemetryClient;
import net.snowflake.client.jdbc.telemetry.TelemetryUtil;
import org.apache.kafka.common.utils.AppInfoParser;

public class SnowflakeTelemetryServiceV1 extends Logging implements SnowflakeTelemetryService {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  // constant string list
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

  SnowflakeTelemetryServiceV1(Connection conn) {
    this.telemetry = TelemetryClient.createTelemetry(conn);
  }

  @VisibleForTesting
  SnowflakeTelemetryServiceV1(Telemetry telemetry) {
    this.telemetry = telemetry;
  }

  @Override
  public void setAppName(final String name) {
    this.name = name;
  }

  @Override
  public void setTaskID(final String taskID) {
    this.taskID = taskID;
  }

  /**
   * This is the minimum JsonNode which will be present in each telemetry Payload. Format:
   *
   * <pre>
   * {
   *  "app_name": "<connector_app_name>",
   *  "task_id": 1,
   * }
   * </pre>
   *
   * @return An ObjectNode which is by default always created with certain defined properties in it.
   */
  private ObjectNode getObjectNode() {
    ObjectNode msg = MAPPER.createObjectNode();
    msg.put(APP_NAME, getAppName());
    msg.put(TASK_ID, getTaskID());
    return msg;
  }

  @Override
  public void reportKafkaConnectStart(
      final long startTime, final Map<String, String> userProvidedConfig) {
    ObjectNode dataObjectNode = getObjectNode();

    dataObjectNode.put(START_TIME, startTime);
    dataObjectNode.put(KAFKA_VERSION, AppInfoParser.getVersion());
    addUserConnectorPropertiesToDataNode(userProvidedConfig, dataObjectNode);

    send(TelemetryType.KAFKA_START, dataObjectNode);
  }

  @Override
  public void reportKafkaConnectStop(final long startTime) {
    ObjectNode msg = getObjectNode();

    msg.put(START_TIME, startTime);
    msg.put(END_TIME, System.currentTimeMillis());

    send(TelemetryType.KAFKA_STOP, msg);
  }

  @Override
  public void reportKafkaConnectFatalError(final String errorDetail) {
    ObjectNode msg = getObjectNode();

    msg.put(TIME, System.currentTimeMillis());
    msg.put(ERROR_NUMBER, errorDetail);

    send(TelemetryType.KAFKA_FATAL_ERROR, msg);
  }

  @Override
  public void reportKafkaPartitionUsage(
      final SnowflakeTelemetryBasicInfo partitionStatus, boolean isClosing) {
    if (partitionStatus.isEmpty()) {
      return;
    }
    ObjectNode msg = getObjectNode();

    partitionStatus.dumpTo(msg);
    msg.put(IS_PIPE_CLOSING, isClosing);

    send(TelemetryType.KAFKA_PIPE_USAGE, msg);
  }

  @Override
  public void reportKafkaPartitionStart(final SnowflakeTelemetryBasicInfo pipeCreation) {
    ObjectNode msg = getObjectNode();

    pipeCreation.dumpTo(msg);

    send(TelemetryType.KAFKA_PIPE_START, msg);
  }

  /**
   * JsonNode data is wrapped into another ObjectNode which looks like this:
   *
   * <pre>
   *   {
   *   "data": {
   *     "app_name": "<app_name>",
   *     "task_id": "-1"
   *   },
   *   "source": "kafka_connector",
   *   "type": "kafka_start/<One of TelemetryType Enums>",
   *   "version": "snowflake_kc_version"
   * }
   *
   * </pre>
   *
   * @param type type of Data
   * @param data JsonData to wrap in a json field called data
   */
  private void send(TelemetryType type, JsonNode data) {
    ObjectNode msg = MAPPER.createObjectNode();
    msg.put(SOURCE, KAFKA_CONNECTOR);
    msg.put(TYPE, type.toString());
    msg.set(DATA, data);
    msg.put(VERSION, Utils.VERSION); // version number
    try {
      telemetry.addLogToBatch(TelemetryUtil.buildJobData(msg));
      logDebug("sending telemetry data: {} of type:{}", data.toString(), type.toString());
      telemetry.sendBatchAsync();
    } catch (Exception e) {
      logError("Failed to send telemetry data: {}, Error: {}", data.toString(), e.getMessage());
    }
  }

  private String getAppName() {
    if (name == null || name.isEmpty()) {
      logWarn("appName in telemetry service is empty");
      return "empty_appName";
    }
    return name;
  }

  private String getTaskID() {
    if (taskID == null || taskID.isEmpty()) {
      logWarn("taskID in telemetry service is empty");
      return "empty_taskID";
    }
    return taskID;
  }

  /**
   * Adds specific user provided connector properties to ObjectNode
   *
   * @param userProvidedConfig user provided key value pairs in a Map
   * @param dataObjectNode Object node in which specific properties to add
   */
  protected void addUserConnectorPropertiesToDataNode(
      final Map<String, String> userProvidedConfig, ObjectNode dataObjectNode) {
    // maxTasks value isn't visible if the user leaves it at default. So, null means not set
    dataObjectNode.put(MAX_TASKS, userProvidedConfig.get("tasks.max"));

    dataObjectNode.put(BUFFER_SIZE_BYTES, userProvidedConfig.get(BUFFER_SIZE_BYTES));
    dataObjectNode.put(BUFFER_COUNT_RECORDS, userProvidedConfig.get(BUFFER_COUNT_RECORDS));
    dataObjectNode.put(BUFFER_FLUSH_TIME_SEC, userProvidedConfig.get(BUFFER_FLUSH_TIME_SEC));

    // Set default to Snowpipe if not provided.
    dataObjectNode.put(
        INGESTION_METHOD_OPT,
        userProvidedConfig.getOrDefault(INGESTION_METHOD_OPT, INGESTION_METHOD_DEFAULT_SNOWPIPE));

    // put delivery guarantee only when ingestion method is snowpipe.
    // For SNOWPIPE_STREAMING, delivery guarantee is always EXACTLY_ONCE
    if (userProvidedConfig
        .getOrDefault(INGESTION_METHOD_OPT, INGESTION_METHOD_DEFAULT_SNOWPIPE)
        .equalsIgnoreCase(INGESTION_METHOD_DEFAULT_SNOWPIPE)) {
      dataObjectNode.put(
          DELIVERY_GUARANTEE,
          userProvidedConfig.getOrDefault(
              DELIVERY_GUARANTEE, IngestionDeliveryGuarantee.AT_LEAST_ONCE.toString()));
    }

    // Key and value converters to gauge if Snowflake Native converters are used.
    dataObjectNode.put(
        KEY_CONVERTER_CONFIG_FIELD, userProvidedConfig.get(KEY_CONVERTER_CONFIG_FIELD));
    dataObjectNode.put(
        VALUE_CONVERTER_CONFIG_FIELD, userProvidedConfig.get(VALUE_CONVERTER_CONFIG_FIELD));
  }

  private enum TelemetryType {
    KAFKA_START("kafka_start"),
    KAFKA_STOP("kafka_stop"),
    KAFKA_FATAL_ERROR("kafka_fatal_error"),
    KAFKA_PIPE_USAGE("kafka_pipe_usage"),
    KAFKA_PIPE_START("kafka_pipe_start");

    private final String name;

    TelemetryType(String name) {
      this.name = name;
    }

    @Override
    public String toString() {
      return this.name;
    }
  }
}
