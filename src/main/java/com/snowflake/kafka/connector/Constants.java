package com.snowflake.kafka.connector;

public final class Constants {
  public static final String DEFAULT_PIPE_NAME_SUFFIX = "-STREAMING";

  public static final class KafkaConnectorConfigParams {

    // connector parameter list
    public static final String NAME = "name";
    public static final String TOPICS = "topics";
    public static final String SNOWFLAKE_TOPICS2TABLE_MAP = "snowflake.topic2table.map";
    public static final String SNOWFLAKE_URL_NAME = "snowflake.url.name";
    public static final String SNOWFLAKE_USER_NAME = "snowflake.user.name";
    public static final String SNOWFLAKE_PRIVATE_KEY = "snowflake.private.key";
    public static final String SNOWFLAKE_DATABASE_NAME = "snowflake.database.name";
    public static final String SNOWFLAKE_SCHEMA_NAME = "snowflake.schema.name";
    public static final String SNOWFLAKE_PRIVATE_KEY_PASSPHRASE =
        "snowflake.private.key.passphrase";
    public static final String SNOWFLAKE_ROLE_NAME = "snowflake.role.name";
    public static final String SNOWFLAKE_AUTHENTICATOR = "snowflake.authenticator";
    public static final String SNOWFLAKE_OAUTH_CLIENT_ID = "snowflake.oauth.client.id";
    public static final String SNOWFLAKE_OAUTH_CLIENT_SECRET = "snowflake.oauth.client.secret";
    public static final String SNOWFLAKE_OAUTH_REFRESH_TOKEN = "snowflake.oauth.refresh.token";
    public static final String SNOWFLAKE_OAUTH_TOKEN_ENDPOINT = "snowflake.oauth.token.endpoint";

    // authenticator type values
    public static final String AUTHENTICATOR_SNOWFLAKE_JWT = "snowflake_jwt";
    public static final String AUTHENTICATOR_OAUTH = "oauth";

    public static final String SNOWFLAKE_JDBC_MAP = "snowflake.jdbc.map";
    public static final String SNOWFLAKE_METADATA_CREATETIME = "snowflake.metadata.createtime";
    public static final String SNOWFLAKE_METADATA_TOPIC = "snowflake.metadata.topic";
    public static final String SNOWFLAKE_METADATA_OFFSET_AND_PARTITION =
        "snowflake.metadata.offset.and.partition";
    public static final String SNOWFLAKE_METADATA_ALL = "snowflake.metadata.all";
    public static final String SNOWFLAKE_METADATA_ALL_DEFAULT = "true";
    public static final String SNOWFLAKE_STREAMING_METADATA_CONNECTOR_PUSH_TIME =
        "snowflake.streaming.metadata.connectorPushTime";
    public static final boolean SNOWFLAKE_STREAMING_METADATA_CONNECTOR_PUSH_TIME_DEFAULT = true;
    public static final String SNOWFLAKE_STREAMING_CLIENT_PROVIDER_OVERRIDE_MAP =
        "snowflake.streaming.client.provider.override.map";
    public static final String SNOWFLAKE_OPEN_CHANNEL_IO_THREADS =
        "snowflake.open.channel.io.threads";
    public static final int SNOWFLAKE_OPEN_CHANNEL_IO_THREADS_DEFAULT = 50;

    // Validation
    public static final String SNOWFLAKE_VALIDATION = "snowflake.validation";
    public static final String SNOWFLAKE_VALIDATION_DEFAULT = "server_side";

    // Snowpipe Streaming Classic (SSv1) offset migration
    public static final String SNOWFLAKE_SSV1_OFFSET_MIGRATION =
        "snowflake.streaming.classic.offset.migration";
    public static final String SNOWFLAKE_SSV1_OFFSET_MIGRATION_DEFAULT = "skip";
    public static final String SNOWFLAKE_SSV1_OFFSET_MIGRATION_INCLUDE_CONNECTOR_NAME =
        "snowflake.streaming.classic.offset.migration.include.connector.name";
    public static final boolean SNOWFLAKE_SSV1_OFFSET_MIGRATION_INCLUDE_CONNECTOR_NAME_DEFAULT =
        false;

    // Caching
    public static final String CACHE_TABLE_EXISTS = "snowflake.cache.table.exists";
    public static final boolean CACHE_TABLE_EXISTS_DEFAULT = true;
    public static final String CACHE_TABLE_EXISTS_EXPIRE_MS =
        "snowflake.cache.table.exists.expire.ms";
    public static final long CACHE_TABLE_EXISTS_EXPIRE_MS_DEFAULT = 5 * 60 * 1000L;
    public static final long CACHE_TABLE_EXISTS_EXPIRE_MS_MIN = 1L;
    public static final String CACHE_PIPE_EXISTS = "snowflake.cache.pipe.exists";
    public static final boolean CACHE_PIPE_EXISTS_DEFAULT = true;
    public static final String CACHE_PIPE_EXISTS_EXPIRE_MS =
        "snowflake.cache.pipe.exists.expire.ms";
    public static final long CACHE_PIPE_EXISTS_EXPIRE_MS_DEFAULT = 5 * 60 * 1000L;
    public static final long CACHE_PIPE_EXISTS_EXPIRE_MS_MIN = 1L;

    public static final String BEHAVIOR_ON_NULL_VALUES = "behavior.on.null.values";
    public static final String VALUE_CONVERTER_SCHEMAS_ENABLE = "value.converter.schemas.enable";

    // metrics
    public static final String JMX_OPT = "jmx";
    public static final boolean JMX_OPT_DEFAULT = true;

    public static final String ERRORS_TOLERANCE_CONFIG = "errors.tolerance";
    public static final String ERRORS_TOLERANCE_DEFAULT =
        ConnectorConfigTools.ErrorTolerance.NONE.toString();
    public static final String ERRORS_LOG_ENABLE_CONFIG = "errors.log.enable";
    public static final boolean ERRORS_LOG_ENABLE_DEFAULT = false;
    public static final String ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_CONFIG =
        "errors.deadletterqueue.topic.name";
    public static final String ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_DEFAULT = "";
    public static final String ENABLE_TASK_FAIL_ON_AUTHORIZATION_ERRORS =
        "enable.task.fail.on.authorization.errors";
    public static final boolean ENABLE_TASK_FAIL_ON_AUTHORIZATION_ERRORS_DEFAULT = false;
    // Compatibility validation
    public static final String SNOWFLAKE_STREAMING_VALIDATE_COMPATIBILITY_WITH_CLASSIC =
        "snowflake.streaming.validate.compatibility.with.classic";
    public static final boolean SNOWFLAKE_STREAMING_VALIDATE_COMPATIBILITY_WITH_CLASSIC_DEFAULT =
        true;

    public static final String
        SNOWFLAKE_COMPATIBILITY_ENABLE_AUTOGENERATED_TABLE_NAME_SANITIZATION =
            "snowflake.compatibility.enable.autogenerated.table.name.sanitization";
    public static final boolean
        SNOWFLAKE_COMPATIBILITY_ENABLE_AUTOGENERATED_TABLE_NAME_SANITIZATION_DEFAULT = false;
    public static final String SNOWFLAKE_COMPATIBILITY_ENABLE_COLUMN_IDENTIFIER_NORMALIZATION =
        "snowflake.compatibility.enable.column.identifier.normalization";
    public static final boolean
        SNOWFLAKE_COMPATIBILITY_ENABLE_COLUMN_IDENTIFIER_NORMALIZATION_DEFAULT = false;
    public static final String SNOWFLAKE_ENABLE_SCHEMATIZATION = "snowflake.enable.schematization";
    public static final boolean SNOWFLAKE_ENABLE_SCHEMATIZATION_DEFAULT = true;

    // MDC logging header
    public static final String ENABLE_MDC_LOGGING_CONFIG = "enable.mdc.logging";
    public static final String ENABLE_MDC_LOGGING_DEFAULT = "false";
    public static final String KEY_CONVERTER = "key.converter";
    public static final String VALUE_CONVERTER = "value.converter";
    public static final String VALUE_CONVERTER_SCHEMA_REGISTRY_URL =
        "value.converter.schema.registry.url";
    // Proxy Info
    public static final String JVM_PROXY_HOST = "jvm.proxy.host";
    public static final String JVM_PROXY_PORT = "jvm.proxy.port";
    public static final String JVM_NON_PROXY_HOSTS = "jvm.nonProxy.hosts";
    public static final String JVM_PROXY_USERNAME = "jvm.proxy.username";
    public static final String JVM_PROXY_PASSWORD = "jvm.proxy.password";

    // jvm proxy
    public static final String HTTP_USE_PROXY = "http.useProxy";
    public static final String HTTPS_PROXY_HOST = "https.proxyHost";
    public static final String HTTPS_PROXY_PORT = "https.proxyPort";
    public static final String HTTP_PROXY_HOST = "http.proxyHost";
    public static final String HTTP_PROXY_PORT = "http.proxyPort";
    public static final String HTTP_NON_PROXY_HOSTS = "http.nonProxyHosts";
    public static final String HTTPS_PROXY_USER = "https.proxyUser";
    public static final String HTTPS_PROXY_PASSWORD = "https.proxyPassword";
    public static final String HTTP_PROXY_USER = "http.proxyUser";
    public static final String HTTP_PROXY_PASSWORD = "http.proxyPassword";
  }

  public static final class StreamingIngestClientConfigParams {

    public static final String AUTHORIZATION_TYPE = "authorization_type";
    public static final String OAUTH_CLIENT_ID = "oauth_client_id";
    public static final String OAUTH_CLIENT_SECRET = "oauth_client_secret";
    public static final String OAUTH_REFRESH_TOKEN = "oauth_refresh_token";
    public static final String OAUTH_TOKEN_ENDPOINT = "oauth_token_endpoint";
  }
}
