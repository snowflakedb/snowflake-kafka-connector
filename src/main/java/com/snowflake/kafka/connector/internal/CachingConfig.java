package com.snowflake.kafka.connector.internal;

import com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams;
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

  public static CachingConfig fromConfig(final Map<String, String> config) {

    boolean tableExistsCacheEnabled =
        Optional.ofNullable(config.get(KafkaConnectorConfigParams.CACHE_TABLE_EXISTS))
            .map(Boolean::parseBoolean)
            .orElse(KafkaConnectorConfigParams.CACHE_TABLE_EXISTS_DEFAULT);

    long tableExistsCacheExpireMs =
        Optional.ofNullable(config.get(KafkaConnectorConfigParams.CACHE_TABLE_EXISTS_EXPIRE_MS))
            .map(Long::parseLong)
            .orElse(KafkaConnectorConfigParams.CACHE_TABLE_EXISTS_EXPIRE_MS_DEFAULT);

    boolean pipeExistsCacheEnabled =
        Optional.ofNullable(config.get(KafkaConnectorConfigParams.CACHE_PIPE_EXISTS))
            .map(Boolean::parseBoolean)
            .orElse(KafkaConnectorConfigParams.CACHE_PIPE_EXISTS_DEFAULT);

    long pipeExistsCacheExpireMs =
        Optional.ofNullable(config.get(KafkaConnectorConfigParams.CACHE_PIPE_EXISTS_EXPIRE_MS))
            .map(Long::parseLong)
            .orElse(KafkaConnectorConfigParams.CACHE_PIPE_EXISTS_EXPIRE_MS_DEFAULT);

    // Validate expiration times are positive
    if (tableExistsCacheExpireMs <= 0) {
      throw new IllegalArgumentException(
          "Cache expiration for table existence must be positive, got: "
              + tableExistsCacheExpireMs);
    }
    if (pipeExistsCacheExpireMs <= 0) {
      throw new IllegalArgumentException(
          "Cache expiration for pipe existence must be positive, got: " + pipeExistsCacheExpireMs);
    }

    return new CachingConfig(
        tableExistsCacheEnabled,
        tableExistsCacheExpireMs,
        pipeExistsCacheEnabled,
        pipeExistsCacheExpireMs);
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
