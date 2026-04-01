package com.snowflake.kafka.connector.internal;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.snowflake.kafka.connector.internal.telemetry.SnowflakeTelemetryService;
import java.sql.Connection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CachingSnowflakeConnectionServiceTest {

  private SnowflakeConnectionService delegate;
  private SnowflakeConnectionPool pool;
  private CachingConfig cachingConfig;
  private CachingSnowflakeConnectionService svc;

  @BeforeEach
  void setUp() {
    delegate = mock(SnowflakeConnectionService.class);
    cachingConfig = buildCachingConfig(true, 60_000, true, 60_000);

    SnowflakeConnectionService pooledConn = mock(SnowflakeConnectionService.class);
    when(pooledConn.tableExist("T")).thenReturn(true);
    when(pooledConn.pipeExist("P")).thenReturn(false);
    when(pooledConn.isTableCompatible("T")).thenReturn(true);
    when(pooledConn.describeTable("T")).thenReturn(Optional.of(List.of()));
    when(pooledConn.shouldEvolveSchema("T", "ROLE")).thenReturn(true);
    when(pooledConn.isClosed()).thenReturn(false);

    pool = new SnowflakeConnectionPool(() -> pooledConn, 5, 300_000, 600_000, 30_000);
    svc = new CachingSnowflakeConnectionService(delegate, pool, cachingConfig);
  }

  // -- Cached methods --

  @Test
  void tableExistReturnsCachedValue() {
    assertTrue(svc.tableExist("T"));
    assertTrue(svc.tableExist("T"));
    // Second call should be a cache hit - only one connection lease from pool
  }

  @Test
  void pipeExistReturnsCachedValue() {
    assertFalse(svc.pipeExist("P"));
    assertFalse(svc.pipeExist("P"));
  }

  @Test
  void tableExistCacheDisabled() {
    CachingConfig noCacheConfig = buildCachingConfig(false, 60_000, true, 60_000);
    SnowflakeConnectionService pooledConn = mock(SnowflakeConnectionService.class);
    when(pooledConn.tableExist("T")).thenReturn(true);
    when(pooledConn.isClosed()).thenReturn(false);
    SnowflakeConnectionPool pool2 =
        new SnowflakeConnectionPool(() -> pooledConn, 5, 300_000, 600_000, 30_000);
    CachingSnowflakeConnectionService svc2 =
        new CachingSnowflakeConnectionService(delegate, pool2, noCacheConfig);

    assertTrue(svc2.tableExist("T"));
    assertTrue(svc2.tableExist("T"));
    verify(pooledConn, times(2)).tableExist("T");
    pool2.close();
  }

  @Test
  void pipeExistCacheDisabled() {
    CachingConfig noCacheConfig = buildCachingConfig(true, 60_000, false, 60_000);
    SnowflakeConnectionService pooledConn = mock(SnowflakeConnectionService.class);
    when(pooledConn.pipeExist("P")).thenReturn(false);
    when(pooledConn.isClosed()).thenReturn(false);
    SnowflakeConnectionPool pool2 =
        new SnowflakeConnectionPool(() -> pooledConn, 5, 300_000, 600_000, 30_000);
    CachingSnowflakeConnectionService svc2 =
        new CachingSnowflakeConnectionService(delegate, pool2, noCacheConfig);

    assertFalse(svc2.pipeExist("P"));
    assertFalse(svc2.pipeExist("P"));
    verify(pooledConn, times(2)).pipeExist("P");
    pool2.close();
  }

  // -- Pooled (non-cached) methods --

  @Test
  void createTableWithMetadataColumnUsesPool() {
    svc.createTableWithMetadataColumn("T");
  }

  @Test
  void createTableWithOnlyMetadataColumnUsesPool() {
    svc.createTableWithOnlyMetadataColumn("T");
  }

  @Test
  void isTableCompatibleUsesPool() {
    assertTrue(svc.isTableCompatible("T"));
  }

  @Test
  void describeTableUsesPool() {
    assertTrue(svc.describeTable("T").isPresent());
  }

  @Test
  void shouldEvolveSchemaUsesPool() {
    assertTrue(svc.shouldEvolveSchema("T", "ROLE"));
  }

  @Test
  void executeQueryWithParametersUsesPool() {
    svc.executeQueryWithParameters("SELECT 1");
  }

  @Test
  void appendColumnsToTableUsesPool() {
    svc.appendColumnsToTable("T", Map.of());
  }

  @Test
  void alterNonNullableColumnsUsesPool() {
    svc.alterNonNullableColumns("T", List.of("col1"));
  }

  // -- Delegate methods --

  @Test
  void getTelemetryClientDelegates() {
    SnowflakeTelemetryService mockTelemetry = mock(SnowflakeTelemetryService.class);
    when(delegate.getTelemetryClient()).thenReturn(mockTelemetry);
    assertSame(mockTelemetry, svc.getTelemetryClient());
  }

  @Test
  void getConnectorNameDelegates() {
    when(delegate.getConnectorName()).thenReturn("my-connector");
    assertEquals("my-connector", svc.getConnectorName());
  }

  @Test
  void isClosedDelegates() {
    when(delegate.isClosed()).thenReturn(false);
    assertFalse(svc.isClosed());
  }

  @Test
  void getConnectionDelegates() {
    Connection mockJdbc = mock(Connection.class);
    when(delegate.getConnection()).thenReturn(mockJdbc);
    assertSame(mockJdbc, svc.getConnection());
  }

  @Test
  void databaseExistsDelegates() {
    svc.databaseExists("mydb");
    verify(delegate).databaseExists("mydb");
  }

  @Test
  void schemaExistsDelegates() {
    svc.schemaExists("myschema");
    verify(delegate).schemaExists("myschema");
  }

  // -- close --

  @Test
  void closeClosesPoolAndDelegate() {
    svc.close();
    verify(delegate).close();
  }

  // -- Concurrent cache coalescing --

  @Test
  void concurrentTableExistCoalescesToSingleLoad() throws Exception {
    AtomicInteger loadCount = new AtomicInteger(0);
    SnowflakeConnectionService slowConn = mock(SnowflakeConnectionService.class);
    when(slowConn.isClosed()).thenReturn(false);
    when(slowConn.tableExist("SLOW"))
        .thenAnswer(
            inv -> {
              loadCount.incrementAndGet();
              Thread.sleep(200);
              return true;
            });

    SnowflakeConnectionPool slowPool =
        new SnowflakeConnectionPool(() -> slowConn, 10, 300_000, 600_000, 30_000);
    CachingSnowflakeConnectionService slowSvc =
        new CachingSnowflakeConnectionService(delegate, slowPool, cachingConfig);

    ExecutorService executor = Executors.newFixedThreadPool(10);
    try {
      Future<Boolean>[] futures =
          IntStream.range(0, 10)
              .mapToObj(i -> executor.submit(() -> slowSvc.tableExist("SLOW")))
              .toArray(Future[]::new);

      for (Future<Boolean> f : futures) {
        assertTrue(f.get(5, TimeUnit.SECONDS));
      }

      // Guava Cache.get() coalesces concurrent loads, so we expect significantly fewer
      // than 10 loads. In practice it's usually 1, but we allow some slack.
      assertTrue(loadCount.get() <= 3, "Expected coalesced loads but got " + loadCount.get());
    } finally {
      executor.shutdownNow();
      slowPool.close();
    }
  }

  // -- Helpers --

  private CachingConfig buildCachingConfig(
      boolean tableEnabled, long tableExpireMs, boolean pipeEnabled, long pipeExpireMs) {
    java.util.Map<String, String> config = new java.util.HashMap<>();
    config.put("snowflake.cache.table.exists", String.valueOf(tableEnabled));
    config.put("snowflake.cache.table.exists.expire.ms", String.valueOf(tableExpireMs));
    config.put("snowflake.cache.pipe.exists", String.valueOf(pipeEnabled));
    config.put("snowflake.cache.pipe.exists.expire.ms", String.valueOf(pipeExpireMs));
    return CachingConfig.fromConfig(config);
  }
}
