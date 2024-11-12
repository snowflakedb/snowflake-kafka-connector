package com.snowflake.kafka.connector.internal.telemetry;

import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.*;

import com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig;
import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.internal.KCLogger;
import com.snowflake.kafka.connector.internal.streaming.IngestionMethodConfig;
import java.util.Map;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.JsonNode;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.ObjectMapper;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.node.ObjectNode;
import net.snowflake.client.jdbc.telemetry.Telemetry;
import net.snowflake.client.jdbc.telemetry.TelemetryUtil;
import org.apache.kafka.common.utils.AppInfoParser;

/**
 * Abstract class handling basics of sending telemetry information to snowflake. Please note, this
 * is only for debugging purposes and data is not exposed to customers.
 */
public abstract class SnowflakeTelemetryService {

  private final KCLogger LOGGER = new KCLogger(SnowflakeTelemetryService.class.getName());

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
  protected static final String IS_PIPE_CLOSING = "is_pipe_closing";
  protected static final String IS_CHANNEL_CLOSING = "is_channel_closing";
  public static final String JDK_VERSION = "jdk_version";
  public static final String JDK_DISTRIBUTION = "jdk_distribution";

  // Telemetry instance fetched from JDBC
  protected Telemetry telemetry;
  // Snowflake Kafka connector name defined in JSON
  private String name = null;
  private String taskID = null;

  protected SnowflakeTelemetryService() {}

  /**
   * set app name
   *
   * @param name app name
   */
  public void setAppName(String name) {
    this.name = name;
  }

  /**
   * set task id
   *
   * @param taskID task id
   */
  public void setTaskID(String taskID) {
    this.taskID = taskID;
  }

  /**
   * Event of connector start
   *
   * @param startTime task start time
   * @param userProvidedConfig max number of tasks
   */
  public void reportKafkaConnectStart(
      final long startTime, final Map<String, String> userProvidedConfig) {
    ObjectNode dataObjectNode = getObjectNode();

    String jdkVersion = System.getProperty("java.version");
    String jdkDistribution = System.getProperty("java.vendor");

    dataObjectNode.put(START_TIME, startTime);
    dataObjectNode.put(KAFKA_VERSION, AppInfoParser.getVersion());
    dataObjectNode.put(JDK_VERSION, jdkVersion);
    dataObjectNode.put(JDK_DISTRIBUTION, jdkDistribution);
    addUserConnectorPropertiesToDataNode(userProvidedConfig, dataObjectNode);

    send(SnowflakeTelemetryService.TelemetryType.KAFKA_START, dataObjectNode);
  }

  /**
   * Event of connector stop
   *
   * @param startTime start timestamp
   */
  public void reportKafkaConnectStop(final long startTime) {
    ObjectNode msg = getObjectNode();

    msg.put(START_TIME, startTime);
    msg.put(END_TIME, System.currentTimeMillis());

    send(SnowflakeTelemetryService.TelemetryType.KAFKA_STOP, msg);
  }

  /**
   * Event of a fatal error in the connector
   *
   * @param errorDetail error message
   */
  public void reportKafkaConnectFatalError(final String errorDetail) {
    ObjectNode msg = getObjectNode();

    msg.put(TIME, System.currentTimeMillis());
    msg.put(ERROR_NUMBER, errorDetail);

    send(SnowflakeTelemetryService.TelemetryType.KAFKA_FATAL_ERROR, msg);
  }

  /**
   * report connector's partition usage.
   *
   * @param partitionStatus SnowflakePipeStatus object
   * @param isClosing is the underlying pipe/channel closing
   */
  public void reportKafkaPartitionUsage(
      final SnowflakeTelemetryBasicInfo partitionStatus, boolean isClosing) {
    ObjectNode msg = getObjectNode();

    partitionStatus.dumpTo(msg);
    msg.put(
        partitionStatus.telemetryType == TelemetryType.KAFKA_PIPE_USAGE
            ? IS_PIPE_CLOSING
            : IS_CHANNEL_CLOSING,
        isClosing);

    send(partitionStatus.telemetryType, msg);
  }

  /**
   * Get default object Node which will be part of every telemetry being sent to snowflake. Based on
   * the underlying implementation, node fields might change.
   *
   * @return ObjectNode in Json Format
   */
  public abstract ObjectNode getObjectNode();

  /**
   * report connector partition start
   *
   * @param partitionCreation SnowflakeTelemetryBasicInfo object
   */
  public void reportKafkaPartitionStart(final SnowflakeTelemetryBasicInfo partitionCreation) {
    ObjectNode msg = getObjectNode();

    partitionCreation.dumpTo(msg);

    send(partitionCreation.telemetryType, msg);
  }

  /**
   * This is the minimum JsonNode which will be present in each telemetry Payload. Format:
   *
   * <pre>
   * {
   *  "app_name": "<connector_app_name>",
   *  "task_id": 1,
   *  "snowflake.ingestion.method": "<Enum Ordinal>" for {@link IngestionMethodConfig}
   * }
   * </pre>
   *
   * @return An ObjectNode which is by default always created with certain defined properties in it.
   */
  protected ObjectNode getDefaultObjectNode(IngestionMethodConfig ingestionMethodConfig) {
    ObjectNode msg = MAPPER.createObjectNode();
    msg.put(APP_NAME, getAppName());
    msg.put(TASK_ID, getTaskID());
    msg.put(SnowflakeSinkConnectorConfig.INGESTION_METHOD_OPT, ingestionMethodConfig.ordinal());
    return msg;
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
  protected void send(SnowflakeTelemetryService.TelemetryType type, JsonNode data) {
    ObjectNode msg = MAPPER.createObjectNode();
    msg.put(SOURCE, KAFKA_CONNECTOR);
    msg.put(TYPE, type.toString());
    msg.set(DATA, data);
    msg.put(VERSION, Utils.VERSION); // version number
    try {
      telemetry.addLogToBatch(TelemetryUtil.buildJobData(msg));
      LOGGER.debug("sending telemetry data: {} of type:{}", data.toString(), type.toString());
      telemetry.sendBatchAsync();
    } catch (Exception e) {
      LOGGER.error("Failed to send telemetry data: {}, Error: {}", data.toString(), e.getMessage());
    }
  }

  private String getAppName() {
    if (name == null || name.isEmpty()) {
      LOGGER.warn("appName in telemetry service is empty");
      return "empty_appName";
    }
    return name;
  }

  private String getTaskID() {
    if (taskID == null || taskID.isEmpty()) {
      LOGGER.warn("taskID in telemetry service is empty");
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

    // Key and value converters to gauge if Snowflake Native converters are used.
    dataObjectNode.put(
        KEY_CONVERTER_CONFIG_FIELD, userProvidedConfig.get(KEY_CONVERTER_CONFIG_FIELD));
    dataObjectNode.put(
        VALUE_CONVERTER_CONFIG_FIELD, userProvidedConfig.get(VALUE_CONVERTER_CONFIG_FIELD));

    // Record whether schematization is used
    dataObjectNode.put(
        ENABLE_SCHEMATIZATION_CONFIG,
        userProvidedConfig.getOrDefault(
            ENABLE_SCHEMATIZATION_CONFIG, ENABLE_SCHEMATIZATION_DEFAULT));

    // Record whether streaming client optimization is enabled
    dataObjectNode.put(
        ENABLE_STREAMING_CLIENT_OPTIMIZATION_CONFIG,
        userProvidedConfig.getOrDefault(
            ENABLE_STREAMING_CLIENT_OPTIMIZATION_CONFIG,
            ENABLE_STREAMING_CLIENT_OPTIMIZATION_DEFAULT + ""));
    if (userProvidedConfig.containsKey(ENABLE_CHANNEL_OFFSET_TOKEN_MIGRATION_CONFIG)) {
      dataObjectNode.put(
          ENABLE_CHANNEL_OFFSET_TOKEN_MIGRATION_CONFIG,
          userProvidedConfig.get(ENABLE_CHANNEL_OFFSET_TOKEN_MIGRATION_CONFIG));
    }
    // These are Optional, so we add only if it's provided in user config
    if (userProvidedConfig.containsKey(SNOWPIPE_STREAMING_MAX_CLIENT_LAG)) {
      dataObjectNode.put(
          SNOWPIPE_STREAMING_MAX_CLIENT_LAG,
          userProvidedConfig.get(SNOWPIPE_STREAMING_MAX_CLIENT_LAG));
    }
    if (userProvidedConfig.containsKey(SNOWPIPE_STREAMING_CLIENT_PROVIDER_OVERRIDE_MAP)) {
      dataObjectNode.put(
          SNOWPIPE_STREAMING_CLIENT_PROVIDER_OVERRIDE_MAP,
          userProvidedConfig.get(SNOWPIPE_STREAMING_CLIENT_PROVIDER_OVERRIDE_MAP));
    }
    if (userProvidedConfig.containsKey(SNOWPIPE_STREAMING_ENABLE_SINGLE_BUFFER)) {
      dataObjectNode.put(
          SNOWPIPE_STREAMING_ENABLE_SINGLE_BUFFER,
          userProvidedConfig.getOrDefault(
              SNOWPIPE_STREAMING_ENABLE_SINGLE_BUFFER,
              String.valueOf(SNOWPIPE_STREAMING_ENABLE_SINGLE_BUFFER_DEFAULT)));
    }
  }

  public enum TelemetryType {
    KAFKA_START("kafka_start"),
    KAFKA_STOP("kafka_stop"),
    KAFKA_FATAL_ERROR("kafka_fatal_error"),
    KAFKA_PIPE_USAGE("kafka_pipe_usage"),
    KAFKA_PIPE_START("kafka_pipe_start"),
    KAFKA_CHANNEL_USAGE("kafka_channel_usage"),
    KAFKA_CHANNEL_START("kafka_channel_start");

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
