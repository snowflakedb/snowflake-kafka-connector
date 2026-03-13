package com.snowflake.kafka.connector.config;

import com.google.auto.value.AutoValue;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.snowflake.kafka.connector.ConnectorConfigTools;
import com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams;
import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.internal.CachingConfig;
import com.snowflake.kafka.connector.records.SnowflakeMetadataConfig;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;

/**
 * Parsed, typed configuration for the sink task. Built once from the raw connector config map in
 * {@link com.snowflake.kafka.connector.SnowflakeSinkTask#start(Map)} and passed through the task
 * and streaming layer so call sites use accessors instead of string keys and repeated defaults.
 */
@AutoValue
public abstract class SinkTaskConfig {

  public abstract String getConnectorName();

  public abstract String getTaskId();

  /** Returns an unmodifiable view of the topic-to-table mapping. */
  public abstract Map<String, String> getTopicToTableMap();

  public abstract ConnectorConfigTools.BehaviorOnNullValues getBehaviorOnNullValues();

  public abstract boolean isJmxEnabled();

  public abstract boolean isTolerateErrors();

  public abstract boolean isErrorsLogEnable();

  @Nullable
  public abstract String getDlqTopicName();

  public abstract boolean isEnableSanitization();

  public abstract boolean isEnableSchematization();

  public abstract boolean isClientValidationEnabled();

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

  @Nullable
  public abstract String getSnowflakePrivateKey();

  @Nullable
  public abstract String getSnowflakePrivateKeyPassphrase();

  @Nullable
  public abstract String getSnowflakeDatabase();

  @Nullable
  public abstract String getSnowflakeSchema();

  /**
   * Parses the raw connector config map into an immutable SinkTaskConfig. Applies defaults for
   * missing optional keys. Caller must ensure required fields (connector name, task id) are present
   * or validation will throw.
   *
   * @param raw raw config from the connector (typically after setDefaultValues)
   * @return parsed config
   * @throws IllegalArgumentException if required fields are missing or invalid
   */
  public static SinkTaskConfig from(Map<String, String> raw) {
    return builderFrom(raw).build();
  }

  @VisibleForTesting
  public static Builder builderFrom(Map<String, String> raw) {
    if (raw == null) {
      raw = new HashMap<>();
    }
    Map<String, String> config = new HashMap<>(raw);

    String connectorName = config.get(KafkaConnectorConfigParams.NAME);
    if (connectorName == null || connectorName.trim().isEmpty()) {
      throw new IllegalArgumentException(
          "Connector name ('"
              + KafkaConnectorConfigParams.NAME
              + "') must be set and cannot be empty");
    }

    String taskId = config.get(Utils.TASK_ID);
    if (taskId == null || taskId.trim().isEmpty()) {
      throw new IllegalArgumentException(
          "Task ID ('" + Utils.TASK_ID + "') must be set and cannot be null or empty");
    }

    ImmutableMap<String, String> topicToTableMap = ImmutableMap.of();
    if (config.containsKey(KafkaConnectorConfigParams.SNOWFLAKE_TOPICS2TABLE_MAP)) {
      Map<String, String> parsed =
          Utils.parseTopicToTableMap(
              config.get(KafkaConnectorConfigParams.SNOWFLAKE_TOPICS2TABLE_MAP));
      if (parsed != null) {
        topicToTableMap = ImmutableMap.copyOf(parsed);
      }
    }

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
                KafkaConnectorConfigParams.SNOWFLAKE_ENABLE_AUTOGENERATED_TABLE_NAME_SANITIZATION,
                String.valueOf(
                    KafkaConnectorConfigParams
                        .SNOWFLAKE_ENABLE_AUTOGENERATED_TABLE_NAME_SANITIZATION_DEFAULT)));

    boolean enableSchematization =
        Boolean.parseBoolean(
            config.getOrDefault(
                KafkaConnectorConfigParams.SNOWFLAKE_ENABLE_SCHEMATIZATION,
                String.valueOf(
                    KafkaConnectorConfigParams.SNOWFLAKE_ENABLE_SCHEMATIZATION_DEFAULT)));

    boolean clientValidationEnabled =
        Boolean.parseBoolean(
            config.getOrDefault(
                KafkaConnectorConfigParams.SNOWFLAKE_CLIENT_VALIDATION_ENABLED,
                String.valueOf(
                    KafkaConnectorConfigParams.SNOWFLAKE_CLIENT_VALIDATION_ENABLED_DEFAULT)));

    int openChannelIoThreads =
        Optional.ofNullable(
                config.get(KafkaConnectorConfigParams.SNOWFLAKE_OPEN_CHANNEL_IO_THREADS))
            .map(Integer::parseInt)
            .orElse(KafkaConnectorConfigParams.SNOWFLAKE_OPEN_CHANNEL_IO_THREADS_DEFAULT);

    String streamingClientProviderOverrideMap =
        config.get(KafkaConnectorConfigParams.SNOWFLAKE_STREAMING_CLIENT_PROVIDER_OVERRIDE_MAP);

    CachingConfig cachingConfig = CachingConfig.fromConfig(config);
    SnowflakeMetadataConfig metadataConfig = new SnowflakeMetadataConfig(config);

    String snowflakeUrl = config.get(KafkaConnectorConfigParams.SNOWFLAKE_URL_NAME);
    String snowflakeUser = config.get(KafkaConnectorConfigParams.SNOWFLAKE_USER_NAME);
    String snowflakeRole = config.get(KafkaConnectorConfigParams.SNOWFLAKE_ROLE_NAME);
    String snowflakePrivateKey = config.get(KafkaConnectorConfigParams.SNOWFLAKE_PRIVATE_KEY);
    String snowflakePrivateKeyPassphrase =
        config.get(KafkaConnectorConfigParams.SNOWFLAKE_PRIVATE_KEY_PASSPHRASE);
    String snowflakeDatabase = config.get(KafkaConnectorConfigParams.SNOWFLAKE_DATABASE_NAME);
    String snowflakeSchema = config.get(KafkaConnectorConfigParams.SNOWFLAKE_SCHEMA_NAME);

    return builder()
        .connectorName(connectorName)
        .taskId(taskId)
        .topicToTableMap(topicToTableMap)
        .behaviorOnNullValues(behaviorOnNullValues)
        .jmxEnabled(jmxEnabled)
        .tolerateErrors(tolerateErrors)
        .errorsLogEnable(errorsLogEnable)
        .dlqTopicName(dlqTopicName)
        .enableSanitization(enableSanitization)
        .enableSchematization(enableSchematization)
        .clientValidationEnabled(clientValidationEnabled)
        .openChannelIoThreads(openChannelIoThreads)
        .streamingClientProviderOverrideMap(streamingClientProviderOverrideMap)
        .cachingConfig(cachingConfig)
        .metadataConfig(metadataConfig)
        .snowflakeUrl(snowflakeUrl)
        .snowflakeUser(snowflakeUser)
        .snowflakeRole(snowflakeRole)
        .snowflakePrivateKey(snowflakePrivateKey)
        .snowflakePrivateKeyPassphrase(snowflakePrivateKeyPassphrase)
        .snowflakeDatabase(snowflakeDatabase)
        .snowflakeSchema(snowflakeSchema);
  }

  /** Creates a new builder. Used by {@link #from(Map)} and by tests. */
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

    public abstract Builder topicToTableMap(Map<String, String> topicToTableMap);

    public abstract Builder behaviorOnNullValues(
        ConnectorConfigTools.BehaviorOnNullValues behaviorOnNullValues);

    public abstract Builder jmxEnabled(boolean jmxEnabled);

    public abstract Builder tolerateErrors(boolean tolerateErrors);

    public abstract Builder errorsLogEnable(boolean errorsLogEnable);

    public abstract Builder dlqTopicName(String dlqTopicName);

    public abstract Builder enableSanitization(boolean enableSanitization);

    public abstract Builder enableSchematization(boolean enableSchematization);

    public abstract Builder clientValidationEnabled(boolean clientValidationEnabled);

    public abstract Builder openChannelIoThreads(int openChannelIoThreads);

    public abstract Builder streamingClientProviderOverrideMap(
        String streamingClientProviderOverrideMap);

    public abstract Builder cachingConfig(CachingConfig cachingConfig);

    public abstract Builder metadataConfig(SnowflakeMetadataConfig metadataConfig);

    public abstract Builder snowflakeUrl(String snowflakeUrl);

    public abstract Builder snowflakeUser(String snowflakeUser);

    public abstract Builder snowflakeRole(String snowflakeRole);

    public abstract Builder snowflakePrivateKey(String snowflakePrivateKey);

    public abstract Builder snowflakePrivateKeyPassphrase(String snowflakePrivateKeyPassphrase);

    public abstract Builder snowflakeDatabase(String snowflakeDatabase);

    public abstract Builder snowflakeSchema(String snowflakeSchema);

    public abstract SinkTaskConfig build();
  }
}
