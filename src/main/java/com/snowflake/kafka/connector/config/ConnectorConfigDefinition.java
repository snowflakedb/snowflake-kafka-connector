package com.snowflake.kafka.connector.config;

import static org.apache.kafka.common.config.ConfigDef.Importance.*;
import static org.apache.kafka.common.config.ConfigDef.Range.*;
import static org.apache.kafka.common.config.ConfigDef.Type.*;

import com.snowflake.kafka.connector.ConnectorConfigTools;
import com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams;
import com.snowflake.kafka.connector.internal.streaming.StreamingUtils;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Width;

/** This class is a placeholder for config definition in Apache Kafka specific format */
public class ConnectorConfigDefinition {

  private static final String SNOWFLAKE_LOGIN_INFO_DOC = "Snowflake Login Info";
  private static final String PROXY_INFO_DOC = "Proxy Info";
  private static final String CONNECTOR_CONFIG_DOC = "Connector Config";
  private static final String SNOWFLAKE_METADATA_FLAGS_DOC = "Snowflake Metadata Flags";
  private static final String ERRORS = "ERRORS";

  private static final ConfigDef.Validator NON_EMPTY_STRING_VALIDATOR =
      new ConfigDef.NonEmptyString();
  private static final ConfigDef.Validator TOPIC_TO_TABLE_VALIDATOR = new TopicToTableValidator();
  private static final ConfigDef.Validator STREAMING_CLIENT_PROVIDER_OVERRIDE_MAP_VALIDATOR =
      new CommaSeparatedKeyValueValidator();

  public static ConfigDef getConfig() {
    return new ConfigDef()
        // snowflake login info
        .define(
            KafkaConnectorConfigParams.SNOWFLAKE_URL_NAME,
            STRING,
            null,
            NON_EMPTY_STRING_VALIDATOR,
            HIGH,
            "Snowflake account url",
            SNOWFLAKE_LOGIN_INFO_DOC,
            0,
            Width.NONE,
            KafkaConnectorConfigParams.SNOWFLAKE_URL_NAME)
        .define(
            KafkaConnectorConfigParams.SNOWFLAKE_USER_NAME,
            STRING,
            null,
            NON_EMPTY_STRING_VALIDATOR,
            HIGH,
            "Snowflake user name",
            SNOWFLAKE_LOGIN_INFO_DOC,
            1,
            Width.NONE,
            KafkaConnectorConfigParams.SNOWFLAKE_USER_NAME)
        .define(
            KafkaConnectorConfigParams.SNOWFLAKE_PRIVATE_KEY,
            PASSWORD,
            "",
            HIGH,
            "Private key for Snowflake user",
            SNOWFLAKE_LOGIN_INFO_DOC,
            2,
            Width.NONE,
            KafkaConnectorConfigParams.SNOWFLAKE_PRIVATE_KEY)
        .define(
            KafkaConnectorConfigParams.SNOWFLAKE_PRIVATE_KEY_PASSPHRASE,
            PASSWORD,
            "",
            LOW,
            "Passphrase of private key if encrypted",
            SNOWFLAKE_LOGIN_INFO_DOC,
            3,
            Width.NONE,
            KafkaConnectorConfigParams.SNOWFLAKE_PRIVATE_KEY_PASSPHRASE)
        .define(
            KafkaConnectorConfigParams.SNOWFLAKE_DATABASE_NAME,
            STRING,
            null,
            NON_EMPTY_STRING_VALIDATOR,
            HIGH,
            "Snowflake database name",
            SNOWFLAKE_LOGIN_INFO_DOC,
            4,
            Width.NONE,
            KafkaConnectorConfigParams.SNOWFLAKE_DATABASE_NAME)
        .define(
            KafkaConnectorConfigParams.SNOWFLAKE_SCHEMA_NAME,
            STRING,
            null,
            NON_EMPTY_STRING_VALIDATOR,
            HIGH,
            "Snowflake database schema name",
            SNOWFLAKE_LOGIN_INFO_DOC,
            5,
            Width.NONE,
            KafkaConnectorConfigParams.SNOWFLAKE_SCHEMA_NAME)
        .define(
            KafkaConnectorConfigParams.SNOWFLAKE_ROLE_NAME,
            STRING,
            null,
            NON_EMPTY_STRING_VALIDATOR,
            HIGH,
            "Snowflake role: snowflake.role.name",
            SNOWFLAKE_LOGIN_INFO_DOC,
            6,
            Width.NONE,
            KafkaConnectorConfigParams.SNOWFLAKE_ROLE_NAME)
        // proxy
        .define(
            KafkaConnectorConfigParams.JVM_PROXY_HOST,
            STRING,
            "",
            LOW,
            "JVM option: https.proxyHost",
            PROXY_INFO_DOC,
            0,
            Width.NONE,
            KafkaConnectorConfigParams.JVM_PROXY_HOST)
        .define(
            KafkaConnectorConfigParams.JVM_PROXY_PORT,
            STRING,
            "",
            LOW,
            "JVM option: https.proxyPort",
            PROXY_INFO_DOC,
            1,
            Width.NONE,
            KafkaConnectorConfigParams.JVM_PROXY_PORT)
        .define(
            KafkaConnectorConfigParams.JVM_NON_PROXY_HOSTS,
            STRING,
            "",
            LOW,
            "JVM option: http.nonProxyHosts",
            PROXY_INFO_DOC,
            2,
            Width.NONE,
            KafkaConnectorConfigParams.JVM_NON_PROXY_HOSTS)
        .define(
            KafkaConnectorConfigParams.JVM_PROXY_USERNAME,
            STRING,
            "",
            LOW,
            "JVM proxy username",
            PROXY_INFO_DOC,
            3,
            Width.NONE,
            KafkaConnectorConfigParams.JVM_PROXY_USERNAME)
        .define(
            KafkaConnectorConfigParams.JVM_PROXY_PASSWORD,
            PASSWORD,
            "",
            LOW,
            "JVM proxy password",
            PROXY_INFO_DOC,
            4,
            Width.NONE,
            KafkaConnectorConfigParams.JVM_PROXY_PASSWORD)
        // Connector Config
        .define(
            KafkaConnectorConfigParams.SNOWFLAKE_TOPICS2TABLE_MAP,
            STRING,
            "",
            TOPIC_TO_TABLE_VALIDATOR,
            LOW,
            "Map of topics to tables (optional). Format : comma-separated tuples, e.g."
                + " <topic-1>:<table-1>,<topic-2>:<table-2>,... ",
            CONNECTOR_CONFIG_DOC,
            0,
            Width.NONE,
            KafkaConnectorConfigParams.SNOWFLAKE_TOPICS2TABLE_MAP)
        .define(
            KafkaConnectorConfigParams.SNOWFLAKE_METADATA_ALL,
            BOOLEAN,
            KafkaConnectorConfigParams.SNOWFLAKE_METADATA_ALL_DEFAULT,
            LOW,
            "Flag to control whether there is metadata collected. If set to false, all metadata"
                + " will be dropped",
            SNOWFLAKE_METADATA_FLAGS_DOC,
            0,
            Width.NONE,
            KafkaConnectorConfigParams.SNOWFLAKE_METADATA_ALL)
        .define(
            KafkaConnectorConfigParams.SNOWFLAKE_METADATA_CREATETIME,
            BOOLEAN,
            KafkaConnectorConfigParams.SNOWFLAKE_METADATA_ALL_DEFAULT,
            LOW,
            "Flag to control whether createtime is collected in snowflake metadata",
            SNOWFLAKE_METADATA_FLAGS_DOC,
            1,
            Width.NONE,
            KafkaConnectorConfigParams.SNOWFLAKE_METADATA_CREATETIME)
        .define(
            KafkaConnectorConfigParams.SNOWFLAKE_METADATA_TOPIC,
            BOOLEAN,
            KafkaConnectorConfigParams.SNOWFLAKE_METADATA_ALL_DEFAULT,
            LOW,
            "Flag to control whether kafka topic name is collected in snowflake metadata",
            SNOWFLAKE_METADATA_FLAGS_DOC,
            2,
            Width.NONE,
            KafkaConnectorConfigParams.SNOWFLAKE_METADATA_TOPIC)
        .define(
            KafkaConnectorConfigParams.SNOWFLAKE_METADATA_OFFSET_AND_PARTITION,
            BOOLEAN,
            KafkaConnectorConfigParams.SNOWFLAKE_METADATA_ALL_DEFAULT,
            LOW,
            "Flag to control whether kafka partition and offset are collected in snowflake"
                + " metadata",
            SNOWFLAKE_METADATA_FLAGS_DOC,
            3,
            Width.NONE,
            KafkaConnectorConfigParams.SNOWFLAKE_METADATA_OFFSET_AND_PARTITION)
        .define(
            KafkaConnectorConfigParams.SNOWFLAKE_STREAMING_METADATA_CONNECTOR_PUSH_TIME,
            BOOLEAN,
            KafkaConnectorConfigParams.SNOWFLAKE_STREAMING_METADATA_CONNECTOR_PUSH_TIME_DEFAULT,
            LOW,
            "Flag to control whether ConnectorPushTime is collected in snowflake metadata for"
                + " Snowpipe Streaming",
            SNOWFLAKE_METADATA_FLAGS_DOC,
            4,
            Width.NONE,
            KafkaConnectorConfigParams.SNOWFLAKE_STREAMING_METADATA_CONNECTOR_PUSH_TIME)
        .define(
            KafkaConnectorConfigParams.BEHAVIOR_ON_NULL_VALUES,
            STRING,
            ConnectorConfigTools.BehaviorOnNullValues.DEFAULT.toString(),
            ConnectorConfigTools.BehaviorOnNullValues.VALIDATOR,
            LOW,
            "How to handle records with a null value (i.e. Kafka tombstone records)."
                + " Valid options are 'DEFAULT' and 'IGNORE'.",
            CONNECTOR_CONFIG_DOC,
            4,
            Width.NONE,
            KafkaConnectorConfigParams.BEHAVIOR_ON_NULL_VALUES)
        .define(
            KafkaConnectorConfigParams.JMX_OPT,
            BOOLEAN,
            KafkaConnectorConfigParams.JMX_OPT_DEFAULT,
            HIGH,
            "Whether to enable JMX MBeans for custom SF metrics")
        .define(
            KafkaConnectorConfigParams.SNOWFLAKE_STREAMING_MAX_CLIENT_LAG,
            LONG,
            StreamingUtils.STREAMING_BUFFER_FLUSH_TIME_MINIMUM_SEC,
            atLeast(StreamingUtils.STREAMING_BUFFER_FLUSH_TIME_MINIMUM_SEC),
            LOW,
            "Decide how often the buffer in the Ingest SDK will be flushed",
            CONNECTOR_CONFIG_DOC,
            6,
            Width.NONE,
            KafkaConnectorConfigParams.SNOWFLAKE_STREAMING_MAX_CLIENT_LAG)
        .define(
            KafkaConnectorConfigParams.SNOWFLAKE_STREAMING_CLIENT_PROVIDER_OVERRIDE_MAP,
            STRING,
            "",
            STREAMING_CLIENT_PROVIDER_OVERRIDE_MAP_VALIDATOR,
            LOW,
            "Map of Key value pairs representing Streaming Client Properties to Override. These are"
                + " optional and recommended to use ONLY after consulting Snowflake Support. Format"
                + " : comma-separated tuples, e.g.:"
                + " MAX_CLIENT_LAG:5000,other_key:value...",
            CONNECTOR_CONFIG_DOC,
            0,
            Width.NONE,
            KafkaConnectorConfigParams.SNOWFLAKE_STREAMING_CLIENT_PROVIDER_OVERRIDE_MAP)
        .define(
            KafkaConnectorConfigParams.ERRORS_TOLERANCE_CONFIG,
            STRING,
            KafkaConnectorConfigParams.ERRORS_TOLERANCE_DEFAULT,
            ConnectorConfigTools.ErrorTolerance.VALIDATOR,
            LOW,
            "Behavior for tolerating errors during Sink connector's operation. 'NONE' is set as"
                + " default and denotes that it will be fail fast. i.e any error will result in an"
                + " immediate task failure. 'ALL'  skips over problematic records.",
            ERRORS,
            0,
            Width.NONE,
            "Error Tolerance")
        .define(
            KafkaConnectorConfigParams.ERRORS_LOG_ENABLE_CONFIG,
            BOOLEAN,
            KafkaConnectorConfigParams.ERRORS_LOG_ENABLE_DEFAULT,
            LOW,
            "If true, write/log each error along with details of the failed operation and record"
                + " properties to the Connect log. Default is 'false', so that only errors that are"
                + " not tolerated are reported.",
            "ERRORS",
            1,
            Width.NONE,
            "Log Errors")
        .define(
            KafkaConnectorConfigParams.ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_CONFIG,
            STRING,
            KafkaConnectorConfigParams.ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_DEFAULT,
            LOW,
            "Whether to output conversion errors to the dead letter queue "
                + "By default messages are not sent to the dead letter queue. "
                + "Requires property `errors.tolerance=all`.",
            ERRORS,
            2,
            Width.NONE,
            "Send error records to the Dead Letter Queue (DLQ)")
        .define(
            KafkaConnectorConfigParams.ENABLE_MDC_LOGGING_CONFIG,
            BOOLEAN,
            KafkaConnectorConfigParams.ENABLE_MDC_LOGGING_DEFAULT,
            LOW,
            "Enable MDC context to prepend log messages. Note that this is only available after"
                + " Apache Kafka 2.3",
            CONNECTOR_CONFIG_DOC,
            8,
            Width.NONE,
            "Enable MDC logging")
        .define(
            KafkaConnectorConfigParams.ENABLE_TASK_FAIL_ON_AUTHORIZATION_ERRORS,
            BOOLEAN,
            KafkaConnectorConfigParams.ENABLE_TASK_FAIL_ON_AUTHORIZATION_ERRORS_DEFAULT,
            LOW,
            "If set to true the Connector will fail its tasks when authorization error from"
                + " Snowflake occurred")
        .define(
            KafkaConnectorConfigParams.SNOWFLAKE_ENABLE_QUOTED_IDENTIFIERS,
            BOOLEAN,
            KafkaConnectorConfigParams.SNOWFLAKE_ENABLE_QUOTED_IDENTIFIERS_DEFAULT,
            LOW,
            "When enabled, auto-generated table names preserve special characters and case"
                + " by wrapping in double quotes instead of sanitizing. Does not affect explicit"
                + " topic2table.map mappings, which always honor quoted identifiers.",
            CONNECTOR_CONFIG_DOC,
            9,
            Width.NONE,
            KafkaConnectorConfigParams.SNOWFLAKE_ENABLE_QUOTED_IDENTIFIERS)
        .define(
            KafkaConnectorConfigParams.CACHE_TABLE_EXISTS,
            BOOLEAN,
            KafkaConnectorConfigParams.CACHE_TABLE_EXISTS_DEFAULT,
            LOW,
            "Enable caching for Snowflake table existence checks to reduce database queries",
            CONNECTOR_CONFIG_DOC,
            10,
            Width.NONE,
            KafkaConnectorConfigParams.CACHE_TABLE_EXISTS)
        .define(
            KafkaConnectorConfigParams.CACHE_TABLE_EXISTS_EXPIRE_MS,
            LONG,
            KafkaConnectorConfigParams.CACHE_TABLE_EXISTS_EXPIRE_MS_DEFAULT,
            atLeast(KafkaConnectorConfigParams.CACHE_TABLE_EXISTS_EXPIRE_MS_MIN),
            LOW,
            "Cache expiration time in milliseconds for table existence checks. Must be a positive"
                + " number.",
            CONNECTOR_CONFIG_DOC,
            11,
            Width.NONE,
            KafkaConnectorConfigParams.CACHE_TABLE_EXISTS_EXPIRE_MS)
        .define(
            KafkaConnectorConfigParams.CACHE_PIPE_EXISTS,
            BOOLEAN,
            KafkaConnectorConfigParams.CACHE_PIPE_EXISTS_DEFAULT,
            LOW,
            "Enable caching for pipe existence checks to reduce database queries",
            CONNECTOR_CONFIG_DOC,
            12,
            Width.NONE,
            KafkaConnectorConfigParams.CACHE_PIPE_EXISTS)
        .define(
            KafkaConnectorConfigParams.CACHE_PIPE_EXISTS_EXPIRE_MS,
            LONG,
            KafkaConnectorConfigParams.CACHE_PIPE_EXISTS_EXPIRE_MS_DEFAULT,
            atLeast(KafkaConnectorConfigParams.CACHE_PIPE_EXISTS_EXPIRE_MS_MIN),
            LOW,
            "Cache expiration time in milliseconds for pipe existence checks. Must be a positive"
                + " number.",
            CONNECTOR_CONFIG_DOC,
            13,
            Width.NONE,
            KafkaConnectorConfigParams.CACHE_PIPE_EXISTS_EXPIRE_MS);
  }
}
