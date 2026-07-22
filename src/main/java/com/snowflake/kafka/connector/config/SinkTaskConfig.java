package com.snowflake.kafka.connector.config;

import com.google.auto.value.AutoValue;
import com.google.common.annotations.VisibleForTesting;
import com.snowflake.kafka.connector.ConnectorConfigTools;
import com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams;
import com.snowflake.kafka.connector.RegexTopicToTableResolver;
import com.snowflake.kafka.connector.StaticTopicToTableResolver;
import com.snowflake.kafka.connector.TopicToTableParser;
import com.snowflake.kafka.connector.TopicToTableResolver;
import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.internal.CachingConfig;
import com.snowflake.kafka.connector.internal.SnowflakeErrors;
import com.snowflake.kafka.connector.internal.streaming.v2.migration.Ssv1MigrationMode;
import com.snowflake.kafka.connector.records.SnowflakeMetadataConfig;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.kafka.common.config.types.Password;

/**
 * Parsed, typed configuration for the sink task. Built once from the raw connector config map in
 * {@link com.snowflake.kafka.connector.SnowflakeSinkTask#start(Map)} and passed through the task
 * and streaming layer so call sites use accessors instead of string keys and repeated defaults.
 */
@AutoValue
public abstract class SinkTaskConfig {

  public abstract String getConnectorName();

  public abstract String getTaskId();

  public abstract TopicToTableResolver getTopicToTableResolver();

  public abstract ConnectorConfigTools.BehaviorOnNullValues getBehaviorOnNullValues();

  public abstract boolean isJmxEnabled();

  public abstract boolean isTolerateErrors();

  public abstract boolean isErrorsLogEnable();

  @Nullable
  public abstract String getDlqTopicName();

  public abstract boolean isEnableSanitization();

  public abstract boolean isEnableSchematization();

  public abstract TableType getAutocreatedTableType();

  public abstract Optional<String> getIcebergCreateTableOptions();

  public abstract boolean isEnableColumnIdentifierNormalization();

  public abstract SnowflakeValidation getValidation();

  public abstract int getOpenChannelIoThreads();

  @Nullable
  public abstract String getStreamingClientProviderOverrideMap();

  public abstract CachingConfig getCachingConfig();

  public abstract SnowflakeMetadataConfig getMetadataConfig();

  @Nullable
  public abstract String getSnowflakeUrl();

  @Nullable
  public abstract String getSnowflakeUser();

  @Nullable
  public abstract String getSnowflakeRole();

  public abstract Optional<Password> getSnowflakePrivateKey();

  public abstract Optional<Password> getSnowflakePrivateKeyPassphrase();

  public abstract AuthenticatorType getAuthenticator();

  public abstract Optional<String> getOauthClientId();

  public abstract Optional<Password> getOauthClientSecret();

  public abstract Optional<Password> getOauthRefreshToken();

  public abstract Optional<String> getOauthTokenEndpoint();

  public abstract boolean getOauthIncludeScope();

  public abstract Optional<String> getOauthScope();

  @Nullable
  public abstract String getSnowflakeDatabase();

  @Nullable
  public abstract String getSnowflakeSchema();

  @Nullable
  public abstract String getProxyHost();

  @Nullable
  public abstract String getProxyPort();

  @Nullable
  public abstract String getNonProxyHosts();

  @Nullable
  public abstract String getProxyUsername();

  @Nullable
  public abstract String getProxyPassword();

  @Nullable
  public abstract String getJdbcMap();

  public abstract Ssv1MigrationMode getSsv1MigrationMode();

  public abstract boolean isSsv1MigrationIncludeConnectorName();

  /**
   * Whether the partition-assignment invariant assertions in {@code
   * SnowflakeSinkServiceV2.insert(Collection)} are enabled. See {@link
   * KafkaConnectorConfigParams#SNOWFLAKE_FEATURE_ASSERT_PARTITION_ASSIGNMENT}.
   */
  public abstract boolean isAssertPartitionAssignmentEnabled();

  /**
   * Whether the preCommit offset-fetch path triggers a channel reopen when it detects an invalid
   * SDK client, so recovery starts even without appendRow traffic. See {@link
   * KafkaConnectorConfigParams#SNOWFLAKE_FEATURE_PRECOMMIT_CLIENT_RECOVERY}.
   */
  public abstract boolean isPrecommitClientRecoveryEnabled();

  /** Whether the Snowpipe Streaming SDK Prometheus metrics endpoint is enabled. */
  public abstract boolean isPrometheusMetricsEnabled();

  /** Port for the Snowpipe Streaming SDK Prometheus metrics endpoint. Empty when not configured. */
  public abstract Optional<Integer> getPrometheusMetricsPort();

  /**
   * Bind address for the Snowpipe Streaming SDK Prometheus metrics endpoint. Empty when not
   * configured.
   */
  public abstract Optional<String> getPrometheusMetricsHost();

  /**
   * Whether client-side structural validation error messages routed to the DLQ include the target
   * table name. See {@link
   * KafkaConnectorConfigParams#SNOWFLAKE_FEATURE_VALIDATION_ERROR_TABLE_NAME}.
   */
  public abstract boolean isValidationErrorTableNameEnabled();

  /**
   * Whether TIME strings with a UTC offset are normalized to {@link java.time.LocalTime} before
   * passing to the SDK. See {@link KafkaConnectorConfigParams#SNOWFLAKE_FEATURE_NORMALIZE_TIME}.
   */
  public abstract boolean isNormalizeTimeEnabled();

  /**
   * Whether the managed-Iceberg structured-OBJECT {@code RECORD_METADATA} handling is enabled:
   * probing whether a table's {@code RECORD_METADATA} is a structured OBJECT, validating an
   * existing structured schema at startup, and conforming the metadata map for ingestion. When
   * {@code false}, {@code RECORD_METADATA} is always treated as VARIANT (4.0.x behavior).
   * Kill-switch for existing workloads. See {@link
   * KafkaConnectorConfigParams#SNOWFLAKE_FEATURE_STRUCTURED_RECORD_METADATA}.
   */
  public abstract boolean isStructuredRecordMetadataEnabled();

  /** Convenience overload that calls {@link #from(Map, boolean)} with {@code false}. */
  public static SinkTaskConfig from(Map<String, String> raw) {
    return from(raw, false);
  }

  /**
   * Parses the raw connector config map into an immutable SinkTaskConfig. Applies defaults for
   * missing optional keys.
   *
   * @param raw raw config from the connector (typically after setDefaultValues)
   * @param skipTaskSpecificConfig if true, task ID and connector name default to "" when absent
   *     instead of throwing. Use this when building a config outside of task startup -- e.g. in
   *     {@code validate()} or connection factory setup -- where task ID is not yet assigned.
   * @return parsed config
   * @throws IllegalArgumentException if required fields are missing or invalid
   */
  public static SinkTaskConfig from(Map<String, String> raw, boolean skipTaskSpecificConfig) {
    return builderFrom(raw, skipTaskSpecificConfig).build();
  }

  @VisibleForTesting
  public static Builder builderFrom(Map<String, String> raw) {
    return builderFrom(raw, false);
  }

  @VisibleForTesting
  public static Builder builderFrom(Map<String, String> raw, boolean skipTaskSpecificConfig) {
    if (raw == null) {
      raw = new HashMap<>();
    }
    Map<String, String> config = new HashMap<>(raw);

    String connectorName = config.getOrDefault(KafkaConnectorConfigParams.NAME, "");
    String taskId = config.getOrDefault(Utils.TASK_ID, "");

    if (!skipTaskSpecificConfig) {
      if (connectorName == null || connectorName.trim().isEmpty()) {
        throw new IllegalArgumentException(
            "Connector name ('"
                + KafkaConnectorConfigParams.NAME
                + "') must be set and cannot be empty");
      }
      if (taskId == null || taskId.trim().isEmpty()) {
        throw new IllegalArgumentException(
            "Task ID ('" + Utils.TASK_ID + "') must be set and cannot be null or empty");
      }
    }

    boolean regexReplacement =
        Boolean.parseBoolean(
            config.getOrDefault(
                KafkaConnectorConfigParams.SNOWFLAKE_TOPIC2TABLE_MAP_REGEX_REPLACEMENT,
                String.valueOf(
                    KafkaConnectorConfigParams
                        .SNOWFLAKE_TOPIC2TABLE_MAP_REGEX_REPLACEMENT_DEFAULT)));

    TopicToTableResolver topicToTableResolver =
        Optional.ofNullable(config.get(KafkaConnectorConfigParams.SNOWFLAKE_TOPICS2TABLE_MAP))
            .map(
                value -> {
                  try {
                    List<TopicToTableParser.Entry> entries =
                        TopicToTableParser.parseAndValidate(value);
                    return regexReplacement
                        ? RegexTopicToTableResolver.from(entries)
                        : StaticTopicToTableResolver.from(entries);
                  } catch (IllegalArgumentException e) {
                    throw SnowflakeErrors.ERROR_0021.getException(e.getMessage());
                  }
                })
            .orElseGet(() -> new StaticTopicToTableResolver(Map.of()));

    ConnectorConfigTools.BehaviorOnNullValues behaviorOnNullValues =
        ConnectorConfigTools.BehaviorOnNullValues.DEFAULT;
    if (config.containsKey(KafkaConnectorConfigParams.BEHAVIOR_ON_NULL_VALUES)) {
      behaviorOnNullValues =
          ConnectorConfigTools.BehaviorOnNullValues.valueOf(
              config
                  .get(KafkaConnectorConfigParams.BEHAVIOR_ON_NULL_VALUES)
                  .toUpperCase(java.util.Locale.ROOT));
    }

    boolean jmxEnabled =
        Optional.ofNullable(config.get(KafkaConnectorConfigParams.JMX_OPT))
            .map(Boolean::parseBoolean)
            .orElse(KafkaConnectorConfigParams.JMX_OPT_DEFAULT);

    String errorsTolerance =
        config.getOrDefault(
            KafkaConnectorConfigParams.ERRORS_TOLERANCE_CONFIG,
            KafkaConnectorConfigParams.ERRORS_TOLERANCE_DEFAULT);
    boolean tolerateErrors =
        ConnectorConfigTools.ErrorTolerance.valueOf(
                errorsTolerance.toUpperCase(java.util.Locale.ROOT))
            .equals(ConnectorConfigTools.ErrorTolerance.ALL);

    boolean errorsLogEnable =
        Boolean.parseBoolean(
            config.getOrDefault(
                KafkaConnectorConfigParams.ERRORS_LOG_ENABLE_CONFIG,
                String.valueOf(KafkaConnectorConfigParams.ERRORS_LOG_ENABLE_DEFAULT)));

    String dlqTopicName =
        config.get(KafkaConnectorConfigParams.ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_CONFIG);

    boolean enableSanitization =
        Boolean.parseBoolean(
            config.getOrDefault(
                KafkaConnectorConfigParams
                    .SNOWFLAKE_COMPATIBILITY_ENABLE_AUTOGENERATED_TABLE_NAME_SANITIZATION,
                String.valueOf(
                    KafkaConnectorConfigParams
                        .SNOWFLAKE_COMPATIBILITY_ENABLE_AUTOGENERATED_TABLE_NAME_SANITIZATION_DEFAULT)));

    boolean enableSchematization =
        Boolean.parseBoolean(
            config.getOrDefault(
                KafkaConnectorConfigParams.SNOWFLAKE_ENABLE_SCHEMATIZATION,
                String.valueOf(
                    KafkaConnectorConfigParams.SNOWFLAKE_ENABLE_SCHEMATIZATION_DEFAULT)));

    boolean enableColumnIdentifierNormalization =
        Boolean.parseBoolean(
            config.getOrDefault(
                KafkaConnectorConfigParams
                    .SNOWFLAKE_COMPATIBILITY_ENABLE_COLUMN_IDENTIFIER_NORMALIZATION,
                String.valueOf(
                    KafkaConnectorConfigParams
                        .SNOWFLAKE_COMPATIBILITY_ENABLE_COLUMN_IDENTIFIER_NORMALIZATION_DEFAULT)));

    SnowflakeValidation validation =
        SnowflakeValidation.fromConfig(
            config.getOrDefault(
                KafkaConnectorConfigParams.SNOWFLAKE_VALIDATION,
                KafkaConnectorConfigParams.SNOWFLAKE_VALIDATION_DEFAULT));

    int openChannelIoThreads =
        Optional.ofNullable(
                config.get(KafkaConnectorConfigParams.SNOWFLAKE_OPEN_CHANNEL_IO_THREADS))
            .map(Integer::parseInt)
            .orElse(KafkaConnectorConfigParams.SNOWFLAKE_OPEN_CHANNEL_IO_THREADS_DEFAULT);

    String streamingClientProviderOverrideMap =
        config.get(KafkaConnectorConfigParams.SNOWFLAKE_STREAMING_CLIENT_PROVIDER_OVERRIDE_MAP);

    CachingConfig cachingConfig = CachingConfig.fromConfig(config);
    SnowflakeMetadataConfig metadataConfig = new SnowflakeMetadataConfig(config);

    Ssv1MigrationMode ssv1MigrationMode =
        Ssv1MigrationMode.fromConfig(
            config.getOrDefault(
                KafkaConnectorConfigParams.SNOWFLAKE_SSV1_OFFSET_MIGRATION,
                KafkaConnectorConfigParams.SNOWFLAKE_SSV1_OFFSET_MIGRATION_DEFAULT));

    boolean ssv1MigrationIncludeConnectorName =
        Boolean.parseBoolean(
            config.getOrDefault(
                KafkaConnectorConfigParams.SNOWFLAKE_SSV1_OFFSET_MIGRATION_INCLUDE_CONNECTOR_NAME,
                String.valueOf(
                    KafkaConnectorConfigParams
                        .SNOWFLAKE_SSV1_OFFSET_MIGRATION_INCLUDE_CONNECTOR_NAME_DEFAULT)));

    boolean precommitClientRecoveryEnabled =
        Boolean.parseBoolean(
            config.getOrDefault(
                KafkaConnectorConfigParams.SNOWFLAKE_FEATURE_PRECOMMIT_CLIENT_RECOVERY,
                String.valueOf(
                    KafkaConnectorConfigParams
                        .SNOWFLAKE_FEATURE_PRECOMMIT_CLIENT_RECOVERY_DEFAULT)));

    boolean prometheusMetricsEnabled =
        Optional.ofNullable(config.get(KafkaConnectorConfigParams.PROMETHEUS_ENABLE))
            .map(Boolean::parseBoolean)
            .orElse(KafkaConnectorConfigParams.PROMETHEUS_ENABLE_DEFAULT);
    Optional<Integer> prometheusMetricsPort =
        Optional.ofNullable(config.get(KafkaConnectorConfigParams.PROMETHEUS_PORT))
            .map(Integer::parseInt);
    Optional<String> prometheusMetricsHost =
        optionalString(config.get(KafkaConnectorConfigParams.PROMETHEUS_HOST));

    String snowflakeUrl = config.get(KafkaConnectorConfigParams.SNOWFLAKE_URL_NAME);
    String snowflakeUser = config.get(KafkaConnectorConfigParams.SNOWFLAKE_USER_NAME);
    String snowflakeRole = config.get(KafkaConnectorConfigParams.SNOWFLAKE_ROLE_NAME);
    String snowflakeDatabase = config.get(KafkaConnectorConfigParams.SNOWFLAKE_DATABASE_NAME);
    String snowflakeSchema = config.get(KafkaConnectorConfigParams.SNOWFLAKE_SCHEMA_NAME);

    AuthenticatorType authenticator =
        AuthenticatorType.fromConfig(
            config.get(KafkaConnectorConfigParams.SNOWFLAKE_AUTHENTICATOR));

    Optional<Password> snowflakePrivateKey =
        optionalPassword(config.get(KafkaConnectorConfigParams.SNOWFLAKE_PRIVATE_KEY));
    Optional<Password> snowflakePrivateKeyPassphrase =
        optionalPassword(config.get(KafkaConnectorConfigParams.SNOWFLAKE_PRIVATE_KEY_PASSPHRASE));

    Optional<String> oauthClientId =
        optionalString(config.get(KafkaConnectorConfigParams.SNOWFLAKE_OAUTH_CLIENT_ID));
    Optional<Password> oauthClientSecret =
        optionalPassword(config.get(KafkaConnectorConfigParams.SNOWFLAKE_OAUTH_CLIENT_SECRET));
    Optional<Password> oauthRefreshToken =
        optionalPassword(config.get(KafkaConnectorConfigParams.SNOWFLAKE_OAUTH_REFRESH_TOKEN));
    Optional<String> oauthTokenEndpoint =
        optionalString(config.get(KafkaConnectorConfigParams.SNOWFLAKE_OAUTH_TOKEN_ENDPOINT));
    boolean oauthIncludeScope =
        Optional.ofNullable(config.get(KafkaConnectorConfigParams.SNOWFLAKE_OAUTH_INCLUDE_SCOPE))
            .map(Boolean::parseBoolean)
            .orElse(KafkaConnectorConfigParams.SNOWFLAKE_OAUTH_INCLUDE_SCOPE_DEFAULT);
    Optional<String> oauthScope =
        optionalString(config.get(KafkaConnectorConfigParams.SNOWFLAKE_OAUTH_SCOPE));

    String proxyHost = config.get(KafkaConnectorConfigParams.JVM_PROXY_HOST);
    String proxyPort = config.get(KafkaConnectorConfigParams.JVM_PROXY_PORT);
    String nonProxyHosts = config.get(KafkaConnectorConfigParams.JVM_NON_PROXY_HOSTS);
    String proxyUsername = config.get(KafkaConnectorConfigParams.JVM_PROXY_USERNAME);
    String proxyPassword = config.get(KafkaConnectorConfigParams.JVM_PROXY_PASSWORD);
    String jdbcMap = config.get(KafkaConnectorConfigParams.SNOWFLAKE_JDBC_MAP);

    boolean assertPartitionAssignmentEnabled =
        Optional.ofNullable(
                config.get(
                    KafkaConnectorConfigParams.SNOWFLAKE_FEATURE_ASSERT_PARTITION_ASSIGNMENT))
            .map(Boolean::parseBoolean)
            .orElse(
                KafkaConnectorConfigParams.SNOWFLAKE_FEATURE_ASSERT_PARTITION_ASSIGNMENT_DEFAULT);

    boolean validationErrorTableNameEnabled =
        Optional.ofNullable(
                config.get(
                    KafkaConnectorConfigParams.SNOWFLAKE_FEATURE_VALIDATION_ERROR_TABLE_NAME))
            .map(Boolean::parseBoolean)
            .orElse(
                KafkaConnectorConfigParams.SNOWFLAKE_FEATURE_VALIDATION_ERROR_TABLE_NAME_DEFAULT);

    boolean normalizeTimeEnabled =
        Optional.ofNullable(config.get(KafkaConnectorConfigParams.SNOWFLAKE_FEATURE_NORMALIZE_TIME))
            .map(Boolean::parseBoolean)
            .orElse(KafkaConnectorConfigParams.SNOWFLAKE_FEATURE_NORMALIZE_TIME_DEFAULT);

    boolean structuredRecordMetadataEnabled =
        Optional.ofNullable(
                config.get(KafkaConnectorConfigParams.SNOWFLAKE_FEATURE_STRUCTURED_RECORD_METADATA))
            .map(Boolean::parseBoolean)
            .orElse(
                KafkaConnectorConfigParams.SNOWFLAKE_FEATURE_STRUCTURED_RECORD_METADATA_DEFAULT);

    TableType autocreatedTableType =
        TableType.fromConfig(config.get(KafkaConnectorConfigParams.SNOWFLAKE_AUTOCREATE_TABLE_TYPE))
            .orElse(KafkaConnectorConfigParams.SNOWFLAKE_AUTOCREATE_TABLE_TYPE_DEFAULT);

    Optional<String> icebergCreateTableOptions =
        Optional.ofNullable(
                config.get(KafkaConnectorConfigParams.SNOWFLAKE_ICEBERG_CREATE_TABLE_OPTIONS))
            .map(String::trim)
            .filter(s -> !s.isEmpty());
    if (icebergCreateTableOptions.isPresent() && autocreatedTableType != TableType.ICEBERG) {
      throw new IllegalArgumentException(
          KafkaConnectorConfigParams.SNOWFLAKE_ICEBERG_CREATE_TABLE_OPTIONS
              + " is only valid when "
              + KafkaConnectorConfigParams.SNOWFLAKE_AUTOCREATE_TABLE_TYPE
              + "=iceberg (got table.type="
              + autocreatedTableType.configValue()
              + ")");
    }

    // client_side validation cannot model the structured RECORD_METADATA column of managed Iceberg
    // tables; client-side schema evolution also relies on ALTER ADD COLUMN, which is a server-side
    // concern for Iceberg. Reject unconditionally rather than only when schematization is enabled.
    if (autocreatedTableType == TableType.ICEBERG
        && validation == SnowflakeValidation.CLIENT_SIDE) {
      throw new IllegalArgumentException(
          KafkaConnectorConfigParams.SNOWFLAKE_VALIDATION
              + "=client_side is not supported for managed Iceberg tables."
              + " RECORD_METADATA is a structured OBJECT handled server-side;"
              + " client-side validation cannot model it."
              + " Set "
              + KafkaConnectorConfigParams.SNOWFLAKE_VALIDATION
              + "=server_side.");
    }

    // Managed Iceberg RECORD_METADATA is cast to a fixed structured OBJECT schema, so every
    // metadata flag that maps to a declared schema field must be enabled. Disabling any of them
    // produces a sparse map that fails the strict typed-OBJECT cast at ingest time.
    if (autocreatedTableType == TableType.ICEBERG
        && !metadataConfig.isFullIcebergMetadataEnabled()) {
      throw new IllegalArgumentException(
          KafkaConnectorConfigParams.SNOWFLAKE_AUTOCREATE_TABLE_TYPE
              + "=iceberg requires all record metadata to be enabled"
              + " (RECORD_METADATA is cast to a fixed structured schema for managed Iceberg)."
              + " Remove any "
              + KafkaConnectorConfigParams.SNOWFLAKE_METADATA_ALL
              + "=false, "
              + KafkaConnectorConfigParams.SNOWFLAKE_METADATA_TOPIC
              + "=false, "
              + KafkaConnectorConfigParams.SNOWFLAKE_METADATA_OFFSET_AND_PARTITION
              + "=false, "
              + KafkaConnectorConfigParams.SNOWFLAKE_METADATA_CREATETIME
              + "=false, or "
              + KafkaConnectorConfigParams.SNOWFLAKE_STREAMING_METADATA_CONNECTOR_PUSH_TIME
              + "=false settings.");
    }

    Builder b = builder();
    b.connectorName(connectorName)
        .taskId(taskId)
        .topicToTableResolver(topicToTableResolver)
        .behaviorOnNullValues(behaviorOnNullValues)
        .jmxEnabled(jmxEnabled)
        .tolerateErrors(tolerateErrors)
        .errorsLogEnable(errorsLogEnable)
        .dlqTopicName(dlqTopicName)
        .enableSanitization(enableSanitization)
        .enableSchematization(enableSchematization)
        .enableColumnIdentifierNormalization(enableColumnIdentifierNormalization)
        .validation(validation)
        .openChannelIoThreads(openChannelIoThreads)
        .streamingClientProviderOverrideMap(streamingClientProviderOverrideMap)
        .cachingConfig(cachingConfig)
        .metadataConfig(metadataConfig)
        .snowflakeUrl(snowflakeUrl)
        .snowflakeUser(snowflakeUser)
        .snowflakeRole(snowflakeRole)
        .authenticator(authenticator)
        .oauthIncludeScope(oauthIncludeScope)
        .snowflakeDatabase(snowflakeDatabase)
        .snowflakeSchema(snowflakeSchema)
        .proxyHost(proxyHost)
        .proxyPort(proxyPort)
        .nonProxyHosts(nonProxyHosts)
        .proxyUsername(proxyUsername)
        .proxyPassword(proxyPassword)
        .jdbcMap(jdbcMap)
        .ssv1MigrationMode(ssv1MigrationMode)
        .ssv1MigrationIncludeConnectorName(ssv1MigrationIncludeConnectorName)
        .assertPartitionAssignmentEnabled(assertPartitionAssignmentEnabled)
        .precommitClientRecoveryEnabled(precommitClientRecoveryEnabled)
        .prometheusMetricsEnabled(prometheusMetricsEnabled)
        .validationErrorTableNameEnabled(validationErrorTableNameEnabled)
        .normalizeTimeEnabled(normalizeTimeEnabled)
        .structuredRecordMetadataEnabled(structuredRecordMetadataEnabled)
        .autocreatedTableType(autocreatedTableType)
        .icebergCreateTableOptions(icebergCreateTableOptions);

    snowflakePrivateKey.ifPresent(b::snowflakePrivateKey);
    snowflakePrivateKeyPassphrase.ifPresent(b::snowflakePrivateKeyPassphrase);

    oauthClientId.ifPresent(b::oauthClientId);
    oauthClientSecret.ifPresent(b::oauthClientSecret);
    oauthRefreshToken.ifPresent(b::oauthRefreshToken);
    oauthTokenEndpoint.ifPresent(b::oauthTokenEndpoint);
    oauthScope.ifPresent(b::oauthScope);
    prometheusMetricsPort.ifPresent(b::prometheusMetricsPort);
    prometheusMetricsHost.ifPresent(b::prometheusMetricsHost);
    return b;
  }

  private static Optional<String> optionalString(String value) {
    return Optional.ofNullable(value).filter(v -> !v.isBlank());
  }

  private static Optional<Password> optionalPassword(String value) {
    return optionalString(value).map(Password::new);
  }

  /**
   * Creates a new builder. Tests must set defaults via {@code SinkTaskConfigTestBuilder} so future
   * changes don't re-add them here.
   */
  public static Builder builder() {
    return new AutoValue_SinkTaskConfig.Builder();
  }

  /**
   * AutoValue-generated builder. When using directly (e.g. in tests), set connectorName and taskId.
   */
  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder connectorName(String connectorName);

    public abstract Builder taskId(String taskId);

    public abstract Builder topicToTableResolver(TopicToTableResolver topicToTableResolver);

    public abstract Builder behaviorOnNullValues(
        ConnectorConfigTools.BehaviorOnNullValues behaviorOnNullValues);

    public abstract Builder jmxEnabled(boolean jmxEnabled);

    public abstract Builder tolerateErrors(boolean tolerateErrors);

    public abstract Builder errorsLogEnable(boolean errorsLogEnable);

    public abstract Builder dlqTopicName(String dlqTopicName);

    public abstract Builder enableSanitization(boolean enableSanitization);

    public abstract Builder enableSchematization(boolean enableSchematization);

    public abstract Builder autocreatedTableType(TableType autocreatedTableType);

    public abstract Builder icebergCreateTableOptions(Optional<String> icebergCreateTableOptions);

    public abstract Builder enableColumnIdentifierNormalization(
        boolean enableColumnIdentifierNormalization);

    public abstract Builder validation(SnowflakeValidation validation);

    public abstract Builder openChannelIoThreads(int openChannelIoThreads);

    public abstract Builder streamingClientProviderOverrideMap(
        String streamingClientProviderOverrideMap);

    public abstract Builder cachingConfig(CachingConfig cachingConfig);

    public abstract Builder metadataConfig(SnowflakeMetadataConfig metadataConfig);

    public abstract Builder snowflakeUrl(String snowflakeUrl);

    public abstract Builder snowflakeUser(String snowflakeUser);

    public abstract Builder snowflakeRole(String snowflakeRole);

    public abstract Builder snowflakePrivateKey(Password snowflakePrivateKey);

    public abstract Builder snowflakePrivateKeyPassphrase(Password snowflakePrivateKeyPassphrase);

    public abstract Builder authenticator(AuthenticatorType authenticator);

    public abstract Builder oauthClientId(String oauthClientId);

    public abstract Builder oauthClientSecret(Password oauthClientSecret);

    public abstract Builder oauthRefreshToken(Password oauthRefreshToken);

    public abstract Builder oauthTokenEndpoint(String oauthTokenEndpoint);

    public abstract Builder oauthIncludeScope(boolean oauthIncludeScope);

    public abstract Builder oauthScope(String oauthScope);

    public abstract Builder snowflakeDatabase(String snowflakeDatabase);

    public abstract Builder snowflakeSchema(String snowflakeSchema);

    public abstract Builder proxyHost(String proxyHost);

    public abstract Builder proxyPort(String proxyPort);

    public abstract Builder nonProxyHosts(String nonProxyHosts);

    public abstract Builder proxyUsername(String proxyUsername);

    public abstract Builder proxyPassword(String proxyPassword);

    public abstract Builder jdbcMap(String jdbcMap);

    public abstract Builder ssv1MigrationMode(Ssv1MigrationMode ssv1MigrationMode);

    public abstract Builder ssv1MigrationIncludeConnectorName(
        boolean ssv1MigrationIncludeConnectorName);

    public abstract Builder assertPartitionAssignmentEnabled(
        boolean assertPartitionAssignmentEnabled);

    public abstract Builder precommitClientRecoveryEnabled(boolean precommitClientRecoveryEnabled);

    public abstract Builder prometheusMetricsEnabled(boolean prometheusMetricsEnabled);

    public abstract Builder prometheusMetricsPort(Integer prometheusMetricsPort);

    public abstract Builder prometheusMetricsHost(String prometheusMetricsHost);

    public abstract Builder validationErrorTableNameEnabled(
        boolean validationErrorTableNameEnabled);

    public abstract Builder normalizeTimeEnabled(boolean normalizeTimeEnabled);

    public abstract Builder structuredRecordMetadataEnabled(
        boolean structuredRecordMetadataEnabled);

    public abstract SinkTaskConfig build();
  }
}
