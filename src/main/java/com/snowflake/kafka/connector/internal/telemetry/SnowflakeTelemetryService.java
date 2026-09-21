package com.snowflake.kafka.connector.internal.telemetry;

import com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams;
import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.internal.KCLogger;
import com.snowflake.kafka.connector.internal.streaming.IngestionMethodConfig;
import java.sql.Connection;
import java.util.Map;
import java.util.Set;
import net.snowflake.client.internal.jdbc.telemetry.Telemetry;
import net.snowflake.client.internal.jdbc.telemetry.TelemetryClient;
import net.snowflake.client.internal.jdbc.telemetry.TelemetryUtil;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.JsonNode;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.ObjectMapper;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.common.utils.AppInfoParser;

public class SnowflakeTelemetryService {

  private final KCLogger LOGGER = new KCLogger(SnowflakeTelemetryService.class.getName());

  private static final ObjectMapper MAPPER = new ObjectMapper();

  // constant string list
  private static final String SOURCE = "source";
  private static final String TYPE = "type";
  private static final String KAFKA_CONNECTOR = "kafka_connector";
  static final String INGESTION_METHOD = "snowflake.ingestion.method";
  private static final String DATA = "data";
  private static final String START_TIME = "start_time";
  private static final String END_TIME = "end_time";
  private static final String APP_NAME = "app_name";
  private static final String TASK_ID = "task_id";
  private static final String ERROR_DETAIL = "error_detail";
  private static final String TIME = "unix_time";
  private static final String VERSION = "version";
  private static final String KAFKA_VERSION = "kafka_version";
  private static final String IS_CHANNEL_CLOSING = "is_channel_closing";
  public static final String JDK_VERSION = "jdk_version";
  public static final String JDK_DISTRIBUTION = "jdk_distribution";
  static final String TASKS_MAX = "tasks.max";

  // Telemetry instance fetched from JDBC
  private final Telemetry telemetry;

  // Snowflake Kafka connector name defined in JSON
  private String name = null;
  private String taskID = null;

  public SnowflakeTelemetryService(Connection conn) {
    this.telemetry = TelemetryClient.createTelemetry(conn);
  }

  public SnowflakeTelemetryService(Telemetry telemetry) {
    this.telemetry = telemetry;
  }

  public void setAppName(String name) {
    this.name = name;
  }

  public void setTaskID(String taskID) {
    this.taskID = taskID;
  }

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

    send(TelemetryType.KAFKA_START, dataObjectNode);
  }

  public void reportKafkaConnectStop(final long startTime) {
    ObjectNode msg = getObjectNode();

    msg.put(START_TIME, startTime);
    msg.put(END_TIME, System.currentTimeMillis());

    send(TelemetryType.KAFKA_STOP, msg);
  }

  public void reportKafkaConnectFatalError(final String errorDetail) {
    ObjectNode msg = getObjectNode();

    msg.put(TIME, System.currentTimeMillis());
    msg.put(ERROR_DETAIL, errorDetail);

    send(TelemetryType.KAFKA_FATAL_ERROR, msg);
  }

  public void reportKafkaConnectFatalError(
      final String errorDetail,
      final String channelName,
      final String tableName,
      final String pipeName) {
    ObjectNode msg = getObjectNode();

    msg.put(TIME, System.currentTimeMillis());
    msg.put(ERROR_DETAIL, errorDetail);
    if (channelName != null) {
      msg.put(TelemetryConstants.TOPIC_PARTITION_CHANNEL_NAME, channelName);
    }
    if (tableName != null) {
      msg.put(TelemetryConstants.TABLE_NAME, tableName);
    }
    if (pipeName != null) {
      msg.put(TelemetryConstants.PIPE_NAME, pipeName);
    }

    send(TelemetryType.KAFKA_FATAL_ERROR, msg);
  }

  /**
   * Reports connector's partition usage.
   *
   * @param partitionStatus SnowflakeTelemetryBasicInfo object
   * @param isClosing is the underlying channel closing
   */
  public void reportKafkaPartitionUsage(
      final SnowflakeTelemetryBasicInfo partitionStatus, boolean isClosing) {
    ObjectNode msg = getObjectNode();

    partitionStatus.dumpTo(msg);
    msg.put(IS_CHANNEL_CLOSING, isClosing);

    send(partitionStatus.telemetryType, msg);
  }

  /**
   * Reports connector partition start.
   *
   * @param partitionCreation SnowflakeTelemetryBasicInfo object
   */
  public void reportKafkaPartitionStart(final SnowflakeTelemetryBasicInfo partitionCreation) {
    ObjectNode msg = getObjectNode();

    partitionCreation.dumpTo(msg);

    send(partitionCreation.telemetryType, msg);
  }

  /** Reports a one-shot SSv1 offset migration attempt and its outcome for a single channel. */
  public void reportSsv1Migration(final SnowflakeTelemetryBasicInfo migration) {
    ObjectNode msg = getObjectNode();
    migration.dumpTo(msg);
    send(TelemetryType.KAFKA_SSV1_MIGRATION, msg);
  }

  /**
   * Creates the default ObjectNode which will be part of every telemetry being sent to Snowflake.
   *
   * <p>Format:
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
  ObjectNode getObjectNode() {
    ObjectNode msg = MAPPER.createObjectNode();
    msg.put(APP_NAME, getAppName());
    msg.put(TASK_ID, getTaskID());
    msg.put(INGESTION_METHOD, IngestionMethodConfig.SNOWPIPE_STREAMING.toString());
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
  private void send(TelemetryType type, JsonNode data) {
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
   * User-provided connector config keys that may be copied into kafka_start. Service-owned fields
   * ({@code app_name}, {@code task_id}, {@code start_time}, JDK/Kafka versions, ingestion method)
   * are written separately and are not listed here so a colliding config key cannot overwrite them.
   * Unknown keys are dropped. Historical names used by older connector versions are not listed
   * here. Add a key only after the server-side persist allowlist already includes it; if we stop
   * collecting a key, remove it there too.
   */
  private static final Set<String> KAFKA_START_ALLOWED_DATA_KEYS =
      Set.of(
          TASKS_MAX,
          KafkaConnectorConfigParams.TOPICS,
          KafkaConnectorConfigParams.KEY_CONVERTER,
          KafkaConnectorConfigParams.VALUE_CONVERTER,
          KafkaConnectorConfigParams.VALUE_CONVERTER_SCHEMAS_ENABLE,
          KafkaConnectorConfigParams.ERRORS_TOLERANCE_CONFIG,
          KafkaConnectorConfigParams.ERRORS_LOG_ENABLE_CONFIG,
          KafkaConnectorConfigParams.ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_CONFIG,
          KafkaConnectorConfigParams.BEHAVIOR_ON_NULL_VALUES,
          KafkaConnectorConfigParams.SNOWFLAKE_TOPICS2TABLE_MAP,
          KafkaConnectorConfigParams.SNOWFLAKE_METADATA_ALL,
          KafkaConnectorConfigParams.JMX_OPT,
          KafkaConnectorConfigParams.ENABLE_MDC_LOGGING_CONFIG,
          KafkaConnectorConfigParams.ENABLE_TASK_FAIL_ON_AUTHORIZATION_ERRORS,
          KafkaConnectorConfigParams.SNOWFLAKE_VALIDATION,
          KafkaConnectorConfigParams.CACHE_TABLE_EXISTS,
          KafkaConnectorConfigParams.CACHE_PIPE_EXISTS);

  /**
   * Copies allowlisted user-provided connector config into the kafka_start telemetry payload.
   * Unlisted keys are omitted.
   */
  private void addUserConnectorPropertiesToDataNode(
      final Map<String, String> userProvidedConfig, final ObjectNode dataObjectNode) {
    for (Map.Entry<String, String> entry : userProvidedConfig.entrySet()) {
      if (KAFKA_START_ALLOWED_DATA_KEYS.contains(entry.getKey())) {
        dataObjectNode.put(entry.getKey(), entry.getValue());
      }
    }
  }

  /** Types of telemetry events that can be sent. */
  public enum TelemetryType {
    KAFKA_START("kafka_start"),
    KAFKA_STOP("kafka_stop"),
    KAFKA_FATAL_ERROR("kafka_fatal_error"),
    KAFKA_CHANNEL_USAGE("kafka_channel_usage"),
    KAFKA_CHANNEL_START("kafka_channel_start"),
    KAFKA_SSV1_MIGRATION("kafka_ssv1_migration");

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
