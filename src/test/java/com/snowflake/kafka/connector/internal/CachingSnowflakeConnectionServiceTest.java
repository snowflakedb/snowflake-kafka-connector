package com.snowflake.kafka.connector.internal;

import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.CACHE_PIPE_EXISTS;
import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.CACHE_PIPE_EXISTS_EXPIRE_MS;
import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.CACHE_TABLE_EXISTS;
import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.CACHE_TABLE_EXISTS_EXPIRE_MS;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class CachingSnowflakeConnectionServiceTest {

  private static final String TEST_TABLE = "TEST_TABLE";
  private static final String TEST_PIPE = "TEST_PIPE";

  @Test
  void testTableExistCacheEnabled_MultipleCalls_DelegateCalledOnce() {

    SnowflakeConnectionService mockDelegate = mock(SnowflakeConnectionService.class);
    when(mockDelegate.tableExist(TEST_TABLE)).thenReturn(true);

    CachingConfig config = createCacheConfig(true, 30000L, false, 30000L);
    CachingSnowflakeConnectionService cachedService =
        new CachingSnowflakeConnectionService(mockDelegate, config);

    // When: Call tableExist multiple times
    boolean result1 = cachedService.tableExist(TEST_TABLE);
    boolean result2 = cachedService.tableExist(TEST_TABLE);
    boolean result3 = cachedService.tableExist(TEST_TABLE);

    // Then: All calls return true and delegate was called only once
    assertTrue(result1);
    assertTrue(result2);
    assertTrue(result3);
    verify(mockDelegate, times(1)).tableExist(TEST_TABLE);
  }

  @Test
  void testTableExistCacheDisabled_MultipleCalls_DelegateCalledEveryTime() {
    // Given: Cache disabled for table existence
    SnowflakeConnectionService mockDelegate = mock(SnowflakeConnectionService.class);
    when(mockDelegate.tableExist(TEST_TABLE)).thenReturn(true);

    CachingConfig config = createCacheConfig(false, 30000L, false, 30000L);
    CachingSnowflakeConnectionService cachedService =
        new CachingSnowflakeConnectionService(mockDelegate, config);

    // When: Call tableExist multiple times
    boolean result1 = cachedService.tableExist(TEST_TABLE);
    boolean result2 = cachedService.tableExist(TEST_TABLE);
    boolean result3 = cachedService.tableExist(TEST_TABLE);

    // Then: All calls return true and delegate was called every time
    assertTrue(result1);
    assertTrue(result2);
    assertTrue(result3);
    verify(mockDelegate, times(3)).tableExist(TEST_TABLE);
  }

  @Test
  void testTableExistCacheEnabled_DifferentTables_DelegateCalledForEach() {
    // Given: Cache enabled for table existence
    SnowflakeConnectionService mockDelegate = mock(SnowflakeConnectionService.class);
    when(mockDelegate.tableExist("TABLE1")).thenReturn(true);
    when(mockDelegate.tableExist("TABLE2")).thenReturn(false);

    CachingConfig config = createCacheConfig(true, 30000L, false, 30000L);
    CachingSnowflakeConnectionService cachedService =
        new CachingSnowflakeConnectionService(mockDelegate, config);

    // When: Call tableExist for different tables
    boolean result1a = cachedService.tableExist("TABLE1");
    boolean result1b = cachedService.tableExist("TABLE1");
    boolean result2a = cachedService.tableExist("TABLE2");
    boolean result2b = cachedService.tableExist("TABLE2");

    // Then: Delegate called once per unique table
    assertTrue(result1a);
    assertTrue(result1b);
    assertFalse(result2a);
    assertFalse(result2b);
    verify(mockDelegate, times(1)).tableExist("TABLE1");
    verify(mockDelegate, times(1)).tableExist("TABLE2");
  }

  @Test
  void testPipeExistCacheEnabled_MultipleCalls_DelegateCalledOnce() {
    // Given: Cache enabled for pipe existence
    SnowflakeConnectionService mockDelegate = mock(SnowflakeConnectionService.class);
    when(mockDelegate.pipeExist(TEST_PIPE)).thenReturn(true);

    CachingConfig config = createCacheConfig(false, 30000L, true, 30000L);
    CachingSnowflakeConnectionService cachedService =
        new CachingSnowflakeConnectionService(mockDelegate, config);

    // When: Call pipeExist multiple times
    boolean result1 = cachedService.pipeExist(TEST_PIPE);
    boolean result2 = cachedService.pipeExist(TEST_PIPE);
    boolean result3 = cachedService.pipeExist(TEST_PIPE);

    // Then: All calls return true and delegate was called only once
    assertTrue(result1);
    assertTrue(result2);
    assertTrue(result3);
    verify(mockDelegate, times(1)).pipeExist(TEST_PIPE);
  }

  @Test
  void testPipeExistCacheDisabled_MultipleCalls_DelegateCalledEveryTime() {
    // Given: Cache disabled for pipe existence
    SnowflakeConnectionService mockDelegate = mock(SnowflakeConnectionService.class);
    when(mockDelegate.pipeExist(TEST_PIPE)).thenReturn(true);

    CachingConfig config = createCacheConfig(false, 30000L, false, 30000L);
    CachingSnowflakeConnectionService cachedService =
        new CachingSnowflakeConnectionService(mockDelegate, config);

    // When: Call pipeExist multiple times
    boolean result1 = cachedService.pipeExist(TEST_PIPE);
    boolean result2 = cachedService.pipeExist(TEST_PIPE);
    boolean result3 = cachedService.pipeExist(TEST_PIPE);

    // Then: All calls return true and delegate was called every time
    assertTrue(result1);
    assertTrue(result2);
    assertTrue(result3);
    verify(mockDelegate, times(3)).pipeExist(TEST_PIPE);
  }

  @Test
  void testPipeExistCacheEnabled_DifferentPipes_DelegateCalledForEach() {
    // Given: Cache enabled for pipe existence
    SnowflakeConnectionService mockDelegate = mock(SnowflakeConnectionService.class);
    when(mockDelegate.pipeExist("PIPE1")).thenReturn(true);
    when(mockDelegate.pipeExist("PIPE2")).thenReturn(false);

    CachingConfig config = createCacheConfig(false, 30000L, true, 30000L);
    CachingSnowflakeConnectionService cachedService =
        new CachingSnowflakeConnectionService(mockDelegate, config);

    // When: Call pipeExist for different pipes
    boolean result1a = cachedService.pipeExist("PIPE1");
    boolean result1b = cachedService.pipeExist("PIPE1");
    boolean result2a = cachedService.pipeExist("PIPE2");
    boolean result2b = cachedService.pipeExist("PIPE2");

    // Then: Delegate called once per unique pipe
    assertTrue(result1a);
    assertTrue(result1b);
    assertFalse(result2a);
    assertFalse(result2b);
    verify(mockDelegate, times(1)).pipeExist("PIPE1");
    verify(mockDelegate, times(1)).pipeExist("PIPE2");
  }

  @Test
  void testCacheExpiration_TableExists() throws InterruptedException {
    // Given: Very short cache expiration
    SnowflakeConnectionService mockDelegate = mock(SnowflakeConnectionService.class);
    when(mockDelegate.tableExist(TEST_TABLE)).thenReturn(true);

    CachingConfig config = createCacheConfig(true, 100L, false, 30000L);
    CachingSnowflakeConnectionService cachedService =
        new CachingSnowflakeConnectionService(mockDelegate, config);

    // When: Call tableExist, wait for expiration, call again
    cachedService.tableExist(TEST_TABLE);
    Thread.sleep(150); // Wait for cache to expire
    cachedService.tableExist(TEST_TABLE);

    // Then: Delegate was called twice (cache expired)
    verify(mockDelegate, times(2)).tableExist(TEST_TABLE);
  }

  @Test
  void testCacheExpiration_PipeExists() throws InterruptedException {
    // Given: Very short cache expiration
    SnowflakeConnectionService mockDelegate = mock(SnowflakeConnectionService.class);
    when(mockDelegate.pipeExist(TEST_PIPE)).thenReturn(true);

    CachingConfig config = createCacheConfig(false, 30000L, true, 100L);
    CachingSnowflakeConnectionService cachedService =
        new CachingSnowflakeConnectionService(mockDelegate, config);

    // When: Call pipeExist, wait for expiration, call again
    cachedService.pipeExist(TEST_PIPE);
    Thread.sleep(150); // Wait for cache to expire
    cachedService.pipeExist(TEST_PIPE);

    // Then: Delegate was called twice (cache expired)
    verify(mockDelegate, times(2)).pipeExist(TEST_PIPE);
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
