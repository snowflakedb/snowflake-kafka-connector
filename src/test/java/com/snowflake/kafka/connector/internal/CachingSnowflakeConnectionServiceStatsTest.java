package com.snowflake.kafka.connector.internal;

import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.CACHE_PIPE_EXISTS;
import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.CACHE_PIPE_EXISTS_EXPIRE_MS;
import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.CACHE_TABLE_EXISTS;
import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.CACHE_TABLE_EXISTS_EXPIRE_MS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Tests for cache statistics logging in CachedSnowflakeConnectionService. Verifies that cache stats
 * can be logged without exceptions.
 */
class CachingSnowflakeConnectionServiceStatsTest {

  @Test
  void testCacheStatisticsLogging() {
    // Given: A cached service with both caches enabled
    SnowflakeConnectionService mockDelegate = mock(SnowflakeConnectionService.class);
    when(mockDelegate.tableExist("TABLE1")).thenReturn(true);
    when(mockDelegate.pipeExist("PIPE1")).thenReturn(true);

    CachingConfig config = createCacheConfig(true, 30000L, true, 30000L);
    CachingSnowflakeConnectionService cachedService =
        new CachingSnowflakeConnectionService(mockDelegate, config);

    // When: Perform some operations
    cachedService.tableExist("TABLE1");
    cachedService.tableExist("TABLE1"); // Cache hit
    cachedService.pipeExist("PIPE1");
    cachedService.pipeExist("PIPE1"); // Cache hit

    // Then: Log statistics (should not throw any exceptions)
    cachedService.logCacheStatistics();
  }

  @Test
  void testCacheStatisticsLoggingWithNoCacheEnabled() {
    // Given: A cached service with no caches enabled
    SnowflakeConnectionService mockDelegate = mock(SnowflakeConnectionService.class);

    CachingConfig config = createCacheConfig(false, 30000L, false, 30000L);
    CachingSnowflakeConnectionService cachedService =
        new CachingSnowflakeConnectionService(mockDelegate, config);

    // When: Log statistics with no cache enabled
    cachedService.logCacheStatistics();

    // Then: No exception should be thrown
  }

  private CachingConfig createCacheConfig(
      boolean cacheTableExists,
      long tableExpirationMs,
      boolean cachePipeExists,
      long pipeExpirationMs) {
    Map<String, String> config = new HashMap<>();
    config.put(CACHE_TABLE_EXISTS, String.valueOf(cacheTableExists));
    config.put(CACHE_TABLE_EXISTS_EXPIRE_MS, String.valueOf(tableExpirationMs));
    config.put(CACHE_PIPE_EXISTS, String.valueOf(cachePipeExists));
    config.put(CACHE_PIPE_EXISTS_EXPIRE_MS, String.valueOf(pipeExpirationMs));
    return CachingConfig.fromConfig(config);
  }
}
