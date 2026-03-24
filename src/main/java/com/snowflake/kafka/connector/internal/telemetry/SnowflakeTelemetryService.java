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
  private static final String MAX_TASKS = "max_tasks";
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
  private static final String TOPICS = "topics";

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

  // IMPORTANT: update this set when adding new credential/secret config params.
  private static final Set<String> SENSITIVE_KEYS =
      Set.of(
          KafkaConnectorConfigParams.SNOWFLAKE_PRIVATE_KEY,
          KafkaConnectorConfigParams.SNOWFLAKE_PRIVATE_KEY_PASSPHRASE,
          KafkaConnectorConfigParams.JVM_PROXY_USERNAME,
          KafkaConnectorConfigParams.JVM_PROXY_PASSWORD,
          KafkaConnectorConfigParams.HTTPS_PROXY_USER,
          KafkaConnectorConfigParams.HTTPS_PROXY_PASSWORD,
          KafkaConnectorConfigParams.HTTP_PROXY_USER,
          KafkaConnectorConfigParams.HTTP_PROXY_PASSWORD);

  /**
   * Adds all user-provided connector config to the telemetry payload, excluding sensitive keys
   * (credentials, passwords). Future config additions are automatically included.
   */
  private void addUserConnectorPropertiesToDataNode(
      final Map<String, String> userProvidedConfig, final ObjectNode dataObjectNode) {
    for (Map.Entry<String, String> entry : userProvidedConfig.entrySet()) {
      if (!SENSITIVE_KEYS.contains(entry.getKey())) {
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
