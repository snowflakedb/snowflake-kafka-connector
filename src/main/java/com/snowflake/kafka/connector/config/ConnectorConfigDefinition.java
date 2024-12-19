package com.snowflake.kafka.connector.config;

import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.*;

import com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig;
import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.internal.streaming.IngestionMethodConfig;
import com.snowflake.kafka.connector.internal.streaming.StreamingUtils;
import org.apache.kafka.common.config.ConfigDef;

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
            ConfigDef.Type.STRING,
            null,
            NON_EMPTY_STRING_VALIDATOR,
            ConfigDef.Importance.HIGH,
            "Snowflake account url",
            SNOWFLAKE_LOGIN_INFO_DOC,
            0,
            ConfigDef.Width.NONE,
            SNOWFLAKE_URL)
        .define(
            SNOWFLAKE_USER,
            ConfigDef.Type.STRING,
            null,
            NON_EMPTY_STRING_VALIDATOR,
            ConfigDef.Importance.HIGH,
            "Snowflake user name",
            SNOWFLAKE_LOGIN_INFO_DOC,
            1,
            ConfigDef.Width.NONE,
            SNOWFLAKE_USER)
        .define(
            SNOWFLAKE_PRIVATE_KEY,
            ConfigDef.Type.PASSWORD,
            "",
            ConfigDef.Importance.HIGH,
            "Private key for Snowflake user",
            SNOWFLAKE_LOGIN_INFO_DOC,
            2,
            ConfigDef.Width.NONE,
            SNOWFLAKE_PRIVATE_KEY)
        .define(
            SNOWFLAKE_PRIVATE_KEY_PASSPHRASE,
            ConfigDef.Type.PASSWORD,
            "",
            ConfigDef.Importance.LOW,
            "Passphrase of private key if encrypted",
            SNOWFLAKE_LOGIN_INFO_DOC,
            3,
            ConfigDef.Width.NONE,
            SNOWFLAKE_PRIVATE_KEY_PASSPHRASE)
        .define(
            SNOWFLAKE_DATABASE,
            ConfigDef.Type.STRING,
            null,
            NON_EMPTY_STRING_VALIDATOR,
            ConfigDef.Importance.HIGH,
            "Snowflake database name",
            SNOWFLAKE_LOGIN_INFO_DOC,
            4,
            ConfigDef.Width.NONE,
            SNOWFLAKE_DATABASE)
        .define(
            SNOWFLAKE_SCHEMA,
            ConfigDef.Type.STRING,
            null,
            NON_EMPTY_STRING_VALIDATOR,
            ConfigDef.Importance.HIGH,
            "Snowflake database schema name",
            SNOWFLAKE_LOGIN_INFO_DOC,
            5,
            ConfigDef.Width.NONE,
            SNOWFLAKE_SCHEMA)
        .define(
            SNOWFLAKE_ROLE,
            ConfigDef.Type.STRING,
            null,
            NON_EMPTY_STRING_VALIDATOR,
            ConfigDef.Importance.LOW,
            "Snowflake role: snowflake.role.name",
            SNOWFLAKE_LOGIN_INFO_DOC,
            6,
            ConfigDef.Width.NONE,
            SNOWFLAKE_ROLE)
        .define(
            AUTHENTICATOR_TYPE,
            ConfigDef.Type.STRING, // TODO: SNOW-889748 change to enum and add validator
            Utils.SNOWFLAKE_JWT,
            ConfigDef.Importance.LOW,
            "Authenticator for JDBC and streaming ingest sdk",
            SNOWFLAKE_LOGIN_INFO_DOC,
            7,
            ConfigDef.Width.NONE,
            AUTHENTICATOR_TYPE)
        .define(
            OAUTH_CLIENT_ID,
            ConfigDef.Type.STRING,
            "",
            ConfigDef.Importance.HIGH,
            "Client id of target OAuth integration",
            SNOWFLAKE_LOGIN_INFO_DOC,
            8,
            ConfigDef.Width.NONE,
            OAUTH_CLIENT_ID)
        .define(
            OAUTH_CLIENT_SECRET,
            ConfigDef.Type.PASSWORD,
            "",
            ConfigDef.Importance.HIGH,
            "Client secret of target OAuth integration",
            SNOWFLAKE_LOGIN_INFO_DOC,
            9,
            ConfigDef.Width.NONE,
            OAUTH_CLIENT_SECRET)
        .define(
            OAUTH_REFRESH_TOKEN,
            ConfigDef.Type.PASSWORD,
            "",
            ConfigDef.Importance.HIGH,
            "Refresh token for OAuth",
            SNOWFLAKE_LOGIN_INFO_DOC,
            10,
            ConfigDef.Width.NONE,
            OAUTH_REFRESH_TOKEN)
        .define(
            OAUTH_TOKEN_ENDPOINT,
            ConfigDef.Type.STRING,
            null,
            ConfigDef.Importance.HIGH,
            "OAuth token endpoint url",
            SNOWFLAKE_LOGIN_INFO_DOC,
            11,
            ConfigDef.Width.NONE,
            OAUTH_TOKEN_ENDPOINT)
        // proxy
        .define(
            JVM_PROXY_HOST,
            ConfigDef.Type.STRING,
            "",
            ConfigDef.Importance.LOW,
            "JVM option: https.proxyHost",
            PROXY_INFO_DOC,
            0,
            ConfigDef.Width.NONE,
            JVM_PROXY_HOST)
        .define(
            JVM_PROXY_PORT,
            ConfigDef.Type.STRING,
            "",
            ConfigDef.Importance.LOW,
            "JVM option: https.proxyPort",
            PROXY_INFO_DOC,
            1,
            ConfigDef.Width.NONE,
            JVM_PROXY_PORT)
        .define(
            JVM_NON_PROXY_HOSTS,
            ConfigDef.Type.STRING,
            "",
            ConfigDef.Importance.LOW,
            "JVM option: http.nonProxyHosts",
            PROXY_INFO_DOC,
            2,
            ConfigDef.Width.NONE,
            JVM_NON_PROXY_HOSTS)
        .define(
            JVM_PROXY_USERNAME,
            ConfigDef.Type.STRING,
            "",
            ConfigDef.Importance.LOW,
            "JVM proxy username",
            PROXY_INFO_DOC,
            3,
            ConfigDef.Width.NONE,
            JVM_PROXY_USERNAME)
        .define(
            JVM_PROXY_PASSWORD,
            ConfigDef.Type.PASSWORD,
            "",
            ConfigDef.Importance.LOW,
            "JVM proxy password",
            PROXY_INFO_DOC,
            4,
            ConfigDef.Width.NONE,
            JVM_PROXY_PASSWORD)
        // Connector Config
        .define(
            TOPICS_TABLES_MAP,
            ConfigDef.Type.STRING,
            "",
            TOPIC_TO_TABLE_VALIDATOR,
            ConfigDef.Importance.LOW,
            "Map of topics to tables (optional). Format : comma-separated tuples, e.g."
                + " <topic-1>:<table-1>,<topic-2>:<table-2>,... ",
            CONNECTOR_CONFIG_DOC,
            0,
            ConfigDef.Width.NONE,
            TOPICS_TABLES_MAP)
        .define(
            BUFFER_COUNT_RECORDS,
            ConfigDef.Type.LONG,
            BUFFER_COUNT_RECORDS_DEFAULT,
            ConfigDef.Range.atLeast(1),
            ConfigDef.Importance.LOW,
            "Number of records buffered in memory per partition before triggering Snowflake"
                + " ingestion",
            CONNECTOR_CONFIG_DOC,
            1,
            ConfigDef.Width.NONE,
            BUFFER_COUNT_RECORDS)
        .define(
            BUFFER_SIZE_BYTES,
            ConfigDef.Type.LONG,
            BUFFER_SIZE_BYTES_DEFAULT,
            ConfigDef.Range.atLeast(1),
            ConfigDef.Importance.LOW,
            "Cumulative size of records buffered in memory per partition before triggering"
                + " Snowflake ingestion",
            CONNECTOR_CONFIG_DOC,
            2,
            ConfigDef.Width.NONE,
            BUFFER_SIZE_BYTES)
        .define(
            BUFFER_FLUSH_TIME_SEC,
            ConfigDef.Type.LONG,
            BUFFER_FLUSH_TIME_SEC_DEFAULT,
            ConfigDef.Range.atLeast(StreamingUtils.STREAMING_BUFFER_FLUSH_TIME_MINIMUM_SEC),
            ConfigDef.Importance.LOW,
            "The time in seconds to flush cached data",
            CONNECTOR_CONFIG_DOC,
            3,
            ConfigDef.Width.NONE,
            BUFFER_FLUSH_TIME_SEC)
        .define(
            SNOWFLAKE_METADATA_ALL,
            ConfigDef.Type.BOOLEAN,
            SNOWFLAKE_METADATA_DEFAULT,
            ConfigDef.Importance.LOW,
            "Flag to control whether there is metadata collected. If set to false, all metadata"
                + " will be dropped",
            SNOWFLAKE_METADATA_FLAGS_DOC,
            0,
            ConfigDef.Width.NONE,
            SNOWFLAKE_METADATA_ALL)
        .define(
            SNOWFLAKE_METADATA_CREATETIME,
            ConfigDef.Type.BOOLEAN,
            SNOWFLAKE_METADATA_DEFAULT,
            ConfigDef.Importance.LOW,
            "Flag to control whether createtime is collected in snowflake metadata",
            SNOWFLAKE_METADATA_FLAGS_DOC,
            1,
            ConfigDef.Width.NONE,
            SNOWFLAKE_METADATA_CREATETIME)
        .define(
            SNOWFLAKE_METADATA_TOPIC,
            ConfigDef.Type.BOOLEAN,
            SNOWFLAKE_METADATA_DEFAULT,
            ConfigDef.Importance.LOW,
            "Flag to control whether kafka topic name is collected in snowflake metadata",
            SNOWFLAKE_METADATA_FLAGS_DOC,
            2,
            ConfigDef.Width.NONE,
            SNOWFLAKE_METADATA_TOPIC)
        .define(
            SNOWFLAKE_METADATA_OFFSET_AND_PARTITION,
            ConfigDef.Type.BOOLEAN,
            SNOWFLAKE_METADATA_DEFAULT,
            ConfigDef.Importance.LOW,
            "Flag to control whether kafka partition and offset are collected in snowflake"
                + " metadata",
            SNOWFLAKE_METADATA_FLAGS_DOC,
            3,
            ConfigDef.Width.NONE,
            SNOWFLAKE_METADATA_OFFSET_AND_PARTITION)
        .define(
            SNOWFLAKE_STREAMING_METADATA_CONNECTOR_PUSH_TIME,
            ConfigDef.Type.BOOLEAN,
            SNOWFLAKE_STREAMING_METADATA_CONNECTOR_PUSH_TIME_DEFAULT,
            ConfigDef.Importance.LOW,
            "Flag to control whether ConnectorPushTime is collected in snowflake metadata for"
                + " Snowpipe Streaming",
            SNOWFLAKE_METADATA_FLAGS_DOC,
            4,
            ConfigDef.Width.NONE,
            SNOWFLAKE_STREAMING_METADATA_CONNECTOR_PUSH_TIME)
        .define(
            PROVIDER_CONFIG,
            ConfigDef.Type.STRING,
            SnowflakeSinkConnectorConfig.KafkaProvider.UNKNOWN.name(),
            KAFKA_PROVIDER_VALIDATOR,
            ConfigDef.Importance.LOW,
            "Whether kafka is running on Confluent code, self hosted or other managed service")
        .define(
            BEHAVIOR_ON_NULL_VALUES_CONFIG,
            ConfigDef.Type.STRING,
            SnowflakeSinkConnectorConfig.BehaviorOnNullValues.DEFAULT.toString(),
            SnowflakeSinkConnectorConfig.BehaviorOnNullValues.VALIDATOR,
            ConfigDef.Importance.LOW,
            "How to handle records with a null value (i.e. Kafka tombstone records)."
                + " Valid options are 'DEFAULT' and 'IGNORE'.",
            CONNECTOR_CONFIG_DOC,
            4,
            ConfigDef.Width.NONE,
            BEHAVIOR_ON_NULL_VALUES_CONFIG)
        .define(
            JMX_OPT,
            ConfigDef.Type.BOOLEAN,
            JMX_OPT_DEFAULT,
            ConfigDef.Importance.HIGH,
            "Whether to enable JMX MBeans for custom SF metrics")
        .define(
            REBALANCING,
            ConfigDef.Type.BOOLEAN,
            REBALANCING_DEFAULT,
            ConfigDef.Importance.LOW,
            "Whether to trigger a rebalancing by exceeding the max poll interval (Used only in"
                + " testing)")
        .define(
            INGESTION_METHOD_OPT,
            ConfigDef.Type.STRING,
            INGESTION_METHOD_DEFAULT_SNOWPIPE,
            IngestionMethodConfig.VALIDATOR,
            ConfigDef.Importance.LOW,
            "Acceptable values for Ingestion: SNOWPIPE or Streaming ingest respectively",
            CONNECTOR_CONFIG_DOC,
            5,
            ConfigDef.Width.NONE,
            INGESTION_METHOD_OPT)
        .define(
            SNOWPIPE_FILE_CLEANER_FIX_ENABLED,
            ConfigDef.Type.BOOLEAN,
            SNOWPIPE_FILE_CLEANER_FIX_ENABLED_DEFAULT,
            ConfigDef.Importance.LOW,
            "Whether to use new file cleaner for snowpipe data ingestion")
        .define(
            SNOWPIPE_FILE_CLEANER_THREADS,
            ConfigDef.Type.INT,
            SNOWPIPE_FILE_CLEANER_THREADS_DEFAULT,
            ConfigDef.Importance.LOW,
            "Defines number of worker threads to associate with the cleaner task. By default there"
                + " is one cleaner per topic's partition and they all share one worker thread")
        .define(
            SNOWPIPE_SINGLE_TABLE_MULTIPLE_TOPICS_FIX_ENABLED,
            ConfigDef.Type.BOOLEAN,
            SNOWPIPE_SINGLE_TABLE_MULTIPLE_TOPICS_FIX_ENABLED_DEFAULT,
            ConfigDef.Importance.LOW,
            "Defines whether stage file names should be prefixed with source topic's name hash."
                + " This is required in scenarios, when there are multiple topics configured to"
                + " ingest data into a single table via topic2table map. If disabled, there is a"
                + " risk that files from various topics may collide with each other and be deleted"
                + " before ingestion.")
        .define(
            SNOWPIPE_STREAMING_CLOSE_CHANNELS_IN_PARALLEL,
            ConfigDef.Type.BOOLEAN,
            SNOWPIPE_STREAMING_CLOSE_CHANNELS_IN_PARALLEL_DEFAULT,
            ConfigDef.Importance.MEDIUM,
            "Whether to close Snowpipe Streaming channels in parallel during task shutdown or"
                + " rebalancing")
        .define(
            SNOWPIPE_STREAMING_MAX_CLIENT_LAG,
            ConfigDef.Type.LONG,
            StreamingUtils.STREAMING_BUFFER_FLUSH_TIME_MINIMUM_SEC,
            ConfigDef.Range.atLeast(StreamingUtils.STREAMING_BUFFER_FLUSH_TIME_MINIMUM_SEC),
            ConfigDef.Importance.LOW,
            "Decide how often the buffer in the Ingest SDK will be flushed",
            CONNECTOR_CONFIG_DOC,
            6,
            ConfigDef.Width.NONE,
            SNOWPIPE_STREAMING_MAX_CLIENT_LAG)
        .define(
            SNOWPIPE_STREAMING_MAX_MEMORY_LIMIT_IN_BYTES,
            ConfigDef.Type.LONG,
            SNOWPIPE_STREAMING_MAX_MEMORY_LIMIT_IN_BYTES_DEFAULT,
            ConfigDef.Importance.LOW,
            "Memory limit for ingest sdk client in bytes.")
        .define(
            SNOWPIPE_STREAMING_ENABLE_SINGLE_BUFFER,
            ConfigDef.Type.BOOLEAN,
            SNOWPIPE_STREAMING_ENABLE_SINGLE_BUFFER_DEFAULT,
            ConfigDef.Importance.LOW,
            "When enabled, it will disable kafka connector buffer and only use ingest sdk buffer"
                + " instead of both.")
        .define(
            SNOWPIPE_STREAMING_CLIENT_PROVIDER_OVERRIDE_MAP,
            ConfigDef.Type.STRING,
            "",
            STREAMING_CLIENT_PROVIDER_OVERRIDE_MAP_VALIDATOR,
            ConfigDef.Importance.LOW,
            "Map of Key value pairs representing Streaming Client Properties to Override. These are"
                + " optional and recommended to use ONLY after consulting Snowflake Support. Format"
                + " : comma-separated tuples, e.g.:"
                + " MAX_CLIENT_LAG:5000,other_key:value...",
            CONNECTOR_CONFIG_DOC,
            0,
            ConfigDef.Width.NONE,
            SNOWPIPE_STREAMING_CLIENT_PROVIDER_OVERRIDE_MAP)
        .define(
            ERRORS_TOLERANCE_CONFIG,
            ConfigDef.Type.STRING,
            ERRORS_TOLERANCE_DEFAULT,
            SnowflakeSinkConnectorConfig.ErrorTolerance.VALIDATOR,
            ConfigDef.Importance.LOW,
            ERRORS_TOLERANCE_DOC,
            ERROR_GROUP,
            0,
            ConfigDef.Width.NONE,
            ERRORS_TOLERANCE_DISPLAY)
        .define(
            ERRORS_LOG_ENABLE_CONFIG,
            ConfigDef.Type.BOOLEAN,
            ERRORS_LOG_ENABLE_DEFAULT,
            ConfigDef.Importance.LOW,
            ERRORS_LOG_ENABLE_DOC,
            ERROR_GROUP,
            1,
            ConfigDef.Width.NONE,
            ERRORS_LOG_ENABLE_DISPLAY)
        .define(
            ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_CONFIG,
            ConfigDef.Type.STRING,
            ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_DEFAULT,
            ConfigDef.Importance.LOW,
            ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_DOC,
            ERROR_GROUP,
            2,
            ConfigDef.Width.NONE,
            ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_DISPLAY)
        .define(
            ENABLE_STREAMING_CLIENT_OPTIMIZATION_CONFIG,
            ConfigDef.Type.BOOLEAN,
            ENABLE_STREAMING_CLIENT_OPTIMIZATION_DEFAULT,
            ConfigDef.Importance.LOW,
            ENABLE_STREAMING_CLIENT_OPTIMIZATION_DOC,
            CONNECTOR_CONFIG_DOC,
            7,
            ConfigDef.Width.NONE,
            ENABLE_STREAMING_CLIENT_OPTIMIZATION_DISPLAY)
        .define(
            ENABLE_MDC_LOGGING_CONFIG,
            ConfigDef.Type.BOOLEAN,
            ENABLE_MDC_LOGGING_DEFAULT,
            ConfigDef.Importance.LOW,
            ENABLE_MDC_LOGGING_DOC,
            CONNECTOR_CONFIG_DOC,
            8,
            ConfigDef.Width.NONE,
            ENABLE_MDC_LOGGING_DISPLAY)
        .define(
            ENABLE_CHANNEL_OFFSET_TOKEN_MIGRATION_CONFIG,
            ConfigDef.Type.BOOLEAN,
            ENABLE_CHANNEL_OFFSET_TOKEN_MIGRATION_DEFAULT,
            ConfigDef.Importance.LOW,
            ENABLE_CHANNEL_OFFSET_TOKEN_MIGRATION_DOC,
            CONNECTOR_CONFIG_DOC,
            9,
            ConfigDef.Width.NONE,
            ENABLE_CHANNEL_OFFSET_TOKEN_MIGRATION_DISPLAY)
        .define(
            ENABLE_TASK_FAIL_ON_AUTHORIZATION_ERRORS,
            ConfigDef.Type.BOOLEAN,
            ENABLE_TASK_FAIL_ON_AUTHORIZATION_ERRORS_DEFAULT,
            ConfigDef.Importance.LOW,
            "If set to true the Connector will fail its tasks when authorization error from"
                + " Snowflake occurred")
        .define(
            ICEBERG_ENABLED,
            ConfigDef.Type.BOOLEAN,
            ICEBERG_ENABLED_DEFAULT_VALUE,
            ConfigDef.Importance.HIGH,
            "When set to true the connector will ingest data into the Iceberg table. Check the"
                + " official Snowflake documentation for the prerequisites.")
        .define(
            ENABLE_CHANNEL_OFFSET_TOKEN_VERIFICATION_FUNCTION_CONFIG,
            ConfigDef.Type.BOOLEAN,
            ENABLE_CHANNEL_OFFSET_TOKEN_VERIFICATION_FUNCTION_DEFAULT,
            ConfigDef.Importance.LOW,
            ENABLE_CHANNEL_OFFSET_TOKEN_VERIFICATION_FUNCTION_DOC,
            CONNECTOR_CONFIG_DOC,
            11,
            ConfigDef.Width.NONE,
            ENABLE_CHANNEL_OFFSET_TOKEN_VERIFICATION_FUNCTION_DISPLAY);
  }
}
