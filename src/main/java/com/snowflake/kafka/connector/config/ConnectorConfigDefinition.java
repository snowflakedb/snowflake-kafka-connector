package com.snowflake.kafka.connector.config;

import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.*;
import static org.apache.kafka.common.config.ConfigDef.Importance.*;
import static org.apache.kafka.common.config.ConfigDef.Range.*;
import static org.apache.kafka.common.config.ConfigDef.Type.*;

import com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig;
import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.internal.streaming.StreamingUtils;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Width;

/** This class is a placeholder for config definition in Apache Kafka specific format */
public class ConnectorConfigDefinition {

  private static final String SNOWFLAKE_LOGIN_INFO_DOC = "Snowflake Login Info";
  private static final String PROXY_INFO_DOC = "Proxy Info";
  private static final String CONNECTOR_CONFIG_DOC = "Connector Config";
  private static final String SNOWFLAKE_METADATA_FLAGS_DOC = "Snowflake Metadata Flags";

  private static final ConfigDef.Validator NON_EMPTY_STRING_VALIDATOR =
      new ConfigDef.NonEmptyString();
  private static final ConfigDef.Validator TOPIC_TO_TABLE_VALIDATOR = new TopicToTableValidator();
  private static final ConfigDef.Validator KAFKA_PROVIDER_VALIDATOR = new KafkaProviderValidator();
  private static final ConfigDef.Validator STREAMING_CLIENT_PROVIDER_OVERRIDE_MAP_VALIDATOR =
      new CommaSeparatedKeyValueValidator();

  public static ConfigDef getConfig() {
    return new ConfigDef()
        // snowflake login info
        .define(
            SNOWFLAKE_URL,
            STRING,
            null,
            NON_EMPTY_STRING_VALIDATOR,
            HIGH,
            "Snowflake account url",
            SNOWFLAKE_LOGIN_INFO_DOC,
            0,
            Width.NONE,
            SNOWFLAKE_URL)
        .define(
            SNOWFLAKE_USER,
            STRING,
            null,
            NON_EMPTY_STRING_VALIDATOR,
            HIGH,
            "Snowflake user name",
            SNOWFLAKE_LOGIN_INFO_DOC,
            1,
            Width.NONE,
            SNOWFLAKE_USER)
        .define(
            SNOWFLAKE_PRIVATE_KEY,
            PASSWORD,
            "",
            HIGH,
            "Private key for Snowflake user",
            SNOWFLAKE_LOGIN_INFO_DOC,
            2,
            Width.NONE,
            SNOWFLAKE_PRIVATE_KEY)
        .define(
            SNOWFLAKE_PRIVATE_KEY_PASSPHRASE,
            PASSWORD,
            "",
            LOW,
            "Passphrase of private key if encrypted",
            SNOWFLAKE_LOGIN_INFO_DOC,
            3,
            Width.NONE,
            SNOWFLAKE_PRIVATE_KEY_PASSPHRASE)
        .define(
            SNOWFLAKE_DATABASE,
            STRING,
            null,
            NON_EMPTY_STRING_VALIDATOR,
            HIGH,
            "Snowflake database name",
            SNOWFLAKE_LOGIN_INFO_DOC,
            4,
            Width.NONE,
            SNOWFLAKE_DATABASE)
        .define(
            SNOWFLAKE_SCHEMA,
            STRING,
            null,
            NON_EMPTY_STRING_VALIDATOR,
            HIGH,
            "Snowflake database schema name",
            SNOWFLAKE_LOGIN_INFO_DOC,
            5,
            Width.NONE,
            SNOWFLAKE_SCHEMA)
        .define(
            SNOWFLAKE_ROLE,
            STRING,
            null,
            NON_EMPTY_STRING_VALIDATOR,
            HIGH,
            "Snowflake role: snowflake.role.name",
            SNOWFLAKE_LOGIN_INFO_DOC,
            6,
            Width.NONE,
            SNOWFLAKE_ROLE)
        .define(
            AUTHENTICATOR_TYPE,
            STRING, // TODO: SNOW-889748 change to enum and add validator
            Utils.SNOWFLAKE_JWT,
            LOW,
            "Authenticator for JDBC and streaming ingest sdk",
            SNOWFLAKE_LOGIN_INFO_DOC,
            7,
            Width.NONE,
            AUTHENTICATOR_TYPE)
        .define(
            OAUTH_CLIENT_ID,
            STRING,
            "",
            HIGH,
            "Client id of target OAuth integration",
            SNOWFLAKE_LOGIN_INFO_DOC,
            8,
            Width.NONE,
            OAUTH_CLIENT_ID)
        .define(
            OAUTH_CLIENT_SECRET,
            PASSWORD,
            "",
            HIGH,
            "Client secret of target OAuth integration",
            SNOWFLAKE_LOGIN_INFO_DOC,
            9,
            Width.NONE,
            OAUTH_CLIENT_SECRET)
        .define(
            OAUTH_REFRESH_TOKEN,
            PASSWORD,
            "",
            HIGH,
            "Refresh token for OAuth",
            SNOWFLAKE_LOGIN_INFO_DOC,
            10,
            Width.NONE,
            OAUTH_REFRESH_TOKEN)
        .define(
            OAUTH_TOKEN_ENDPOINT,
            STRING,
            null,
            HIGH,
            "OAuth token endpoint url",
            SNOWFLAKE_LOGIN_INFO_DOC,
            11,
            Width.NONE,
            OAUTH_TOKEN_ENDPOINT)
        // proxy
        .define(
            JVM_PROXY_HOST,
            STRING,
            "",
            LOW,
            "JVM option: https.proxyHost",
            PROXY_INFO_DOC,
            0,
            Width.NONE,
            JVM_PROXY_HOST)
        .define(
            JVM_PROXY_PORT,
            STRING,
            "",
            LOW,
            "JVM option: https.proxyPort",
            PROXY_INFO_DOC,
            1,
            Width.NONE,
            JVM_PROXY_PORT)
        .define(
            JVM_NON_PROXY_HOSTS,
            STRING,
            "",
            LOW,
            "JVM option: http.nonProxyHosts",
            PROXY_INFO_DOC,
            2,
            Width.NONE,
            JVM_NON_PROXY_HOSTS)
        .define(
            JVM_PROXY_USERNAME,
            STRING,
            "",
            LOW,
            "JVM proxy username",
            PROXY_INFO_DOC,
            3,
            Width.NONE,
            JVM_PROXY_USERNAME)
        .define(
            JVM_PROXY_PASSWORD,
            PASSWORD,
            "",
            LOW,
            "JVM proxy password",
            PROXY_INFO_DOC,
            4,
            Width.NONE,
            JVM_PROXY_PASSWORD)
        // Connector Config
        .define(
            TOPICS_TABLES_MAP,
            STRING,
            "",
            TOPIC_TO_TABLE_VALIDATOR,
            LOW,
            "Map of topics to tables (optional). Format : comma-separated tuples, e.g."
                + " <topic-1>:<table-1>,<topic-2>:<table-2>,... ",
            CONNECTOR_CONFIG_DOC,
            0,
            Width.NONE,
            TOPICS_TABLES_MAP)
        .define(
            SNOWFLAKE_METADATA_ALL,
            BOOLEAN,
            SNOWFLAKE_METADATA_DEFAULT,
            LOW,
            "Flag to control whether there is metadata collected. If set to false, all metadata"
                + " will be dropped",
            SNOWFLAKE_METADATA_FLAGS_DOC,
            0,
            Width.NONE,
            SNOWFLAKE_METADATA_ALL)
        .define(
            SNOWFLAKE_METADATA_CREATETIME,
            BOOLEAN,
            SNOWFLAKE_METADATA_DEFAULT,
            LOW,
            "Flag to control whether createtime is collected in snowflake metadata",
            SNOWFLAKE_METADATA_FLAGS_DOC,
            1,
            Width.NONE,
            SNOWFLAKE_METADATA_CREATETIME)
        .define(
            SNOWFLAKE_METADATA_TOPIC,
            BOOLEAN,
            SNOWFLAKE_METADATA_DEFAULT,
            LOW,
            "Flag to control whether kafka topic name is collected in snowflake metadata",
            SNOWFLAKE_METADATA_FLAGS_DOC,
            2,
            Width.NONE,
            SNOWFLAKE_METADATA_TOPIC)
        .define(
            SNOWFLAKE_METADATA_OFFSET_AND_PARTITION,
            BOOLEAN,
            SNOWFLAKE_METADATA_DEFAULT,
            LOW,
            "Flag to control whether kafka partition and offset are collected in snowflake"
                + " metadata",
            SNOWFLAKE_METADATA_FLAGS_DOC,
            3,
            Width.NONE,
            SNOWFLAKE_METADATA_OFFSET_AND_PARTITION)
        .define(
            SNOWFLAKE_STREAMING_METADATA_CONNECTOR_PUSH_TIME,
            BOOLEAN,
            SNOWFLAKE_STREAMING_METADATA_CONNECTOR_PUSH_TIME_DEFAULT,
            LOW,
            "Flag to control whether ConnectorPushTime is collected in snowflake metadata for"
                + " Snowpipe Streaming",
            SNOWFLAKE_METADATA_FLAGS_DOC,
            4,
            Width.NONE,
            SNOWFLAKE_STREAMING_METADATA_CONNECTOR_PUSH_TIME)
        .define(
            PROVIDER_CONFIG,
            STRING,
            SnowflakeSinkConnectorConfig.KafkaProvider.UNKNOWN.name(),
            KAFKA_PROVIDER_VALIDATOR,
            LOW,
            "Whether kafka is running on Confluent code, self hosted or other managed service")
        .define(
            BEHAVIOR_ON_NULL_VALUES_CONFIG,
            STRING,
            SnowflakeSinkConnectorConfig.BehaviorOnNullValues.DEFAULT.toString(),
            SnowflakeSinkConnectorConfig.BehaviorOnNullValues.VALIDATOR,
            LOW,
            "How to handle records with a null value (i.e. Kafka tombstone records)."
                + " Valid options are 'DEFAULT' and 'IGNORE'.",
            CONNECTOR_CONFIG_DOC,
            4,
            Width.NONE,
            BEHAVIOR_ON_NULL_VALUES_CONFIG)
        .define(
            JMX_OPT,
            BOOLEAN,
            JMX_OPT_DEFAULT,
            HIGH,
            "Whether to enable JMX MBeans for custom SF metrics")
        .define(
            REBALANCING,
            BOOLEAN,
            REBALANCING_DEFAULT,
            LOW,
            "Whether to trigger a rebalancing by exceeding the max poll interval (Used only in"
                + " testing)")
        .define(
            SNOWPIPE_STREAMING_MAX_CLIENT_LAG,
            LONG,
            StreamingUtils.STREAMING_BUFFER_FLUSH_TIME_MINIMUM_SEC,
            atLeast(StreamingUtils.STREAMING_BUFFER_FLUSH_TIME_MINIMUM_SEC),
            LOW,
            "Decide how often the buffer in the Ingest SDK will be flushed",
            CONNECTOR_CONFIG_DOC,
            6,
            Width.NONE,
            SNOWPIPE_STREAMING_MAX_CLIENT_LAG)
        .define(
            SNOWPIPE_STREAMING_MAX_MEMORY_LIMIT_IN_BYTES,
            LONG,
            SNOWPIPE_STREAMING_MAX_MEMORY_LIMIT_IN_BYTES_DEFAULT,
            LOW,
            "Memory limit for ingest sdk client in bytes.")
        .define(
            SNOWPIPE_STREAMING_CLIENT_PROVIDER_OVERRIDE_MAP,
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
            SNOWPIPE_STREAMING_CLIENT_PROVIDER_OVERRIDE_MAP)
        .define(
            ERRORS_TOLERANCE_CONFIG,
            STRING,
            ERRORS_TOLERANCE_DEFAULT,
            SnowflakeSinkConnectorConfig.ErrorTolerance.VALIDATOR,
            LOW,
            ERRORS_TOLERANCE_DOC,
            ERROR_GROUP,
            0,
            Width.NONE,
            ERRORS_TOLERANCE_DISPLAY)
        .define(
            ERRORS_LOG_ENABLE_CONFIG,
            BOOLEAN,
            ERRORS_LOG_ENABLE_DEFAULT,
            LOW,
            ERRORS_LOG_ENABLE_DOC,
            ERROR_GROUP,
            1,
            Width.NONE,
            ERRORS_LOG_ENABLE_DISPLAY)
        .define(
            ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_CONFIG,
            STRING,
            ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_DEFAULT,
            LOW,
            ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_DOC,
            ERROR_GROUP,
            2,
            Width.NONE,
            ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_DISPLAY)
        .define(
            ENABLE_MDC_LOGGING_CONFIG,
            BOOLEAN,
            ENABLE_MDC_LOGGING_DEFAULT,
            LOW,
            ENABLE_MDC_LOGGING_DOC,
            CONNECTOR_CONFIG_DOC,
            8,
            Width.NONE,
            ENABLE_MDC_LOGGING_DISPLAY)
        .define(
            ENABLE_TASK_FAIL_ON_AUTHORIZATION_ERRORS,
            BOOLEAN,
            ENABLE_TASK_FAIL_ON_AUTHORIZATION_ERRORS_DEFAULT,
            LOW,
            "If set to true the Connector will fail its tasks when authorization error from"
                + " Snowflake occurred")
        .define(
            ICEBERG_ENABLED,
            BOOLEAN,
            ICEBERG_ENABLED_DEFAULT_VALUE,
            HIGH,
            "When set to true the connector will ingest data into the Iceberg table. Check the"
                + " official Snowflake documentation for the prerequisites.")
        .define(
            CACHE_TABLE_EXISTS,
            BOOLEAN,
            CACHE_TABLE_EXISTS_DEFAULT,
            LOW,
            "Enable caching for Snowflake table existence checks to reduce database queries",
            CONNECTOR_CONFIG_DOC,
            9,
            Width.NONE,
            CACHE_TABLE_EXISTS)
        .define(
            CACHE_TABLE_EXISTS_EXPIRE_MS,
            LONG,
            CACHE_TABLE_EXISTS_EXPIRE_MS_DEFAULT,
            atLeast(CACHE_TABLE_EXISTS_EXPIRE_MS_MIN),
            LOW,
            "Cache expiration time in milliseconds for table existence checks. Must be a positive"
                + " number.",
            CONNECTOR_CONFIG_DOC,
            10,
            Width.NONE,
            CACHE_TABLE_EXISTS_EXPIRE_MS)
        .define(
            CACHE_PIPE_EXISTS,
            BOOLEAN,
            CACHE_PIPE_EXISTS_DEFAULT,
            LOW,
            "Enable caching for pipe existence checks to reduce database queries",
            CONNECTOR_CONFIG_DOC,
            11,
            Width.NONE,
            CACHE_PIPE_EXISTS)
        .define(
            CACHE_PIPE_EXISTS_EXPIRE_MS,
            LONG,
            CACHE_PIPE_EXISTS_EXPIRE_MS_DEFAULT,
            atLeast(CACHE_PIPE_EXISTS_EXPIRE_MS_MIN),
            LOW,
            "Cache expiration time in milliseconds for pipe existence checks. Must be a positive"
                + " number.",
            CONNECTOR_CONFIG_DOC,
            12,
            Width.NONE,
            CACHE_PIPE_EXISTS_EXPIRE_MS);
  }
}
