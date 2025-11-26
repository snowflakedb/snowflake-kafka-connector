package com.snowflake.kafka.connector.internal;

import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.CACHE_PIPE_EXISTS;
import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.CACHE_PIPE_EXISTS_EXPIRE_MS;
import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.CACHE_PIPE_EXISTS_EXPIRE_MS_DEFAULT;
import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.CACHE_TABLE_EXISTS;
import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.CACHE_TABLE_EXISTS_EXPIRE_MS;
import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.CACHE_TABLE_EXISTS_EXPIRE_MS_DEFAULT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Tests for CacheConfig class. These tests verify that cache configuration values are properly
 * parsed and validated.
 */
class SnowflakeConnectionServiceCacheTest {

  @Test
  void testCacheConfigDefaults() {
    Map<String, String> configMap = new HashMap<>();
    CachingConfig config = CachingConfig.fromConfig(configMap);

    assertTrue(config.isTableExistsCacheEnabled());
    assertEquals(CACHE_TABLE_EXISTS_EXPIRE_MS_DEFAULT, config.getTableExistsCacheExpireMs());
    assertTrue(config.isPipeExistsCacheEnabled());
    assertEquals(CACHE_PIPE_EXISTS_EXPIRE_MS_DEFAULT, config.getPipeExistsCacheExpireMs());
  }

  @Test
  void testCacheConfigInvalidTableExpiration() {
    Map<String, String> configMap = createConfigWithCache(true, 0L, true, 30000L);

    assertThrows(
        IllegalArgumentException.class,
        () -> CachingConfig.fromConfig(configMap),
        "Should throw exception for non-positive table expiration");
  }

  @Test
  void testCacheConfigInvalidPipeExpiration() {
    Map<String, String> configMap = createConfigWithCache(true, 30000L, true, -100L);

    assertThrows(
        IllegalArgumentException.class,
        () -> CachingConfig.fromConfig(configMap),
        "Should throw exception for negative pipe expiration");
  }

  @Test
  void testCacheConfigInvalidNumberFormat() {
    Map<String, String> configMap = new HashMap<>();
    configMap.put(CACHE_TABLE_EXISTS, "true");
    configMap.put(CACHE_TABLE_EXISTS_EXPIRE_MS, "invalid");
    configMap.put(CACHE_PIPE_EXISTS, "true");
    configMap.put(CACHE_PIPE_EXISTS_EXPIRE_MS, "30000");

    assertThrows(
        IllegalArgumentException.class,
        () -> CachingConfig.fromConfig(configMap),
        "Should throw exception for invalid number format");
  }

  private Map<String, String> createConfigWithCache(
      boolean cacheTableExists,
      long tableExpirationMs,
      boolean cachePipeExists,
      long pipeExpirationMs) {
    Map<String, String> config = new HashMap<>();
    config.put(CACHE_TABLE_EXISTS, String.valueOf(cacheTableExists));
    config.put(CACHE_TABLE_EXISTS_EXPIRE_MS, String.valueOf(tableExpirationMs));
    config.put(CACHE_PIPE_EXISTS, String.valueOf(cachePipeExists));
    config.put(CACHE_PIPE_EXISTS_EXPIRE_MS, String.valueOf(pipeExpirationMs));
    return config;
  }
}
