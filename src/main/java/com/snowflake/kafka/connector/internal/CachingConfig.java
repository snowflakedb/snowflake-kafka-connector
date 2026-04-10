package com.snowflake.kafka.connector.internal;

import com.google.common.collect.ImmutableMap;
import com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams;
import com.snowflake.kafka.connector.Utils;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Configuration class for table and pipe existence caching. Contains all cache-related settings
 * with proper types. The values are coming from the connector config map. If you have any cache
 * related configuration parameters add them here.
 */
public final class CachingConfig {
  private final boolean tableExistsCacheEnabled;
  private final long tableExistsCacheExpireMs;
  private final boolean pipeExistsCacheEnabled;
  private final long pipeExistsCacheExpireMs;

  private CachingConfig(
      boolean tableExistsCacheEnabled,
      long tableExistsCacheExpireMs,
      boolean pipeExistsCacheEnabled,
      long pipeExistsCacheExpireMs) {
    this.tableExistsCacheEnabled = tableExistsCacheEnabled;
    this.tableExistsCacheExpireMs = tableExistsCacheExpireMs;
    this.pipeExistsCacheEnabled = pipeExistsCacheEnabled;
    this.pipeExistsCacheExpireMs = pipeExistsCacheExpireMs;
  }

  public boolean isTableExistsCacheEnabled() {
    return tableExistsCacheEnabled;
  }

  public long getTableExistsCacheExpireMs() {
    return tableExistsCacheExpireMs;
  }

  public boolean isPipeExistsCacheEnabled() {
    return pipeExistsCacheEnabled;
  }

  public long getPipeExistsCacheExpireMs() {
    return pipeExistsCacheExpireMs;
  }

  /**
   * Validate raw cache-related config entries, returning a map of config key → error message for
   * invalid entries. Must be called before {@link #fromConfig(Map)} since that method uses lenient
   * parsing (e.g. {@code Boolean.parseBoolean} accepts any string).
   */
  public static ImmutableMap<String, String> validate(Map<String, String> config) {
    Map<String, String> errors = new HashMap<>();

    validateBoolean(config, KafkaConnectorConfigParams.CACHE_TABLE_EXISTS, errors);
    validateBoolean(config, KafkaConnectorConfigParams.CACHE_PIPE_EXISTS, errors);
    validatePositiveLong(config, KafkaConnectorConfigParams.CACHE_TABLE_EXISTS_EXPIRE_MS, errors);
    validatePositiveLong(config, KafkaConnectorConfigParams.CACHE_PIPE_EXISTS_EXPIRE_MS, errors);

    return ImmutableMap.copyOf(errors);
  }

  private static void validateBoolean(
      Map<String, String> config, String key, Map<String, String> errors) {
    if (config.containsKey(key)) {
      String value = config.get(key);
      if (!"true".equalsIgnoreCase(value) && !"false".equalsIgnoreCase(value)) {
        errors.put(
            key, Utils.formatString("{} must be either 'true' or 'false', got: {}", key, value));
      }
    }
  }

  private static void validatePositiveLong(
      Map<String, String> config, String key, Map<String, String> errors) {
    if (config.containsKey(key)) {
      try {
        long value = Long.parseLong(config.get(key));
        if (value <= 0) {
          errors.put(key, Utils.formatString("{} must be a positive number, got: {}", key, value));
        }
      } catch (NumberFormatException e) {
        errors.put(
            key,
            Utils.formatString("{} must be a valid long number, got: {}", key, config.get(key)));
      }
    }
  }

  public static CachingConfig fromConfig(final Map<String, String> config) {

    boolean tableExistsCacheEnabled =
        Optional.ofNullable(config.get(KafkaConnectorConfigParams.CACHE_TABLE_EXISTS))
            .map(Boolean::parseBoolean)
            .orElse(KafkaConnectorConfigParams.CACHE_TABLE_EXISTS_DEFAULT);

    long tableExistsCacheExpireMs =
        parseLongOrDefault(
            config.get(KafkaConnectorConfigParams.CACHE_TABLE_EXISTS_EXPIRE_MS),
            KafkaConnectorConfigParams.CACHE_TABLE_EXISTS_EXPIRE_MS_DEFAULT);

    boolean pipeExistsCacheEnabled =
        Optional.ofNullable(config.get(KafkaConnectorConfigParams.CACHE_PIPE_EXISTS))
            .map(Boolean::parseBoolean)
            .orElse(KafkaConnectorConfigParams.CACHE_PIPE_EXISTS_DEFAULT);

    long pipeExistsCacheExpireMs =
        parseLongOrDefault(
            config.get(KafkaConnectorConfigParams.CACHE_PIPE_EXISTS_EXPIRE_MS),
            KafkaConnectorConfigParams.CACHE_PIPE_EXISTS_EXPIRE_MS_DEFAULT);

    return new CachingConfig(
        tableExistsCacheEnabled,
        tableExistsCacheExpireMs,
        pipeExistsCacheEnabled,
        pipeExistsCacheExpireMs);
  }

  private static long parseLongOrDefault(String value, long defaultValue) {
    if (value == null) {
      return defaultValue;
    }
    try {
      return Long.parseLong(value);
    } catch (NumberFormatException e) {
      // best-effort: keep default; validator will re-check and report properly
      return defaultValue;
    }
  }

  @Override
  public String toString() {
    return "CacheConfig{"
        + "tableExistsCacheEnabled="
        + tableExistsCacheEnabled
        + ", tableExistsCacheExpireMs="
        + tableExistsCacheExpireMs
        + ", pipeExistsCacheEnabled="
        + pipeExistsCacheEnabled
        + ", pipeExistsCacheExpireMs="
        + pipeExistsCacheExpireMs
        + '}';
  }
}
