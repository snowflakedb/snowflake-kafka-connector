package com.snowflake.kafka.connector.internal;

import static java.util.concurrent.TimeUnit.MINUTES;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheStats;
import com.snowflake.kafka.connector.internal.schemaevolution.ColumnInfos;
import com.snowflake.kafka.connector.internal.telemetry.SnowflakeTelemetryService;
import java.sql.Connection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Single JDBC access point that routes operations through a delegate connection or a connection
 * pool.
 *
 * <p>Lightweight / single-threaded operations ({@link #getTelemetryClient}, {@link
 * #getConnectorName}, {@link #isClosed}, etc.) go straight to the delegate.
 *
 * <p>JDBC queries that may be called concurrently ({@link #tableExist}, {@link #pipeExist}, {@link
 * #createTableWithOnlyMetadataColumn}, {@link #describeTable}, etc.) lease a connection from the
 * pool for the duration of the call. {@code tableExist} and {@code pipeExist} are additionally
 * cached: Guava {@link Cache#get(Object, java.util.concurrent.Callable)} coalesces concurrent loads
 * for the same key, so N threads checking the same table result in at most 1 JDBC round-trip.
 */
public class CachingSnowflakeConnectionService implements SnowflakeConnectionService {

  private static final KCLogger LOGGER =
      new KCLogger(CachingSnowflakeConnectionService.class.getName());

  private static final long CACHE_STATS_LOG_INTERVAL_MS = MINUTES.toMillis(5);
  private static final int CACHE_SIZE = 100;

  private final SnowflakeConnectionService delegate;
  private final SnowflakeConnectionPool pool;
  private final Cache<String, Boolean> tableExistsCache;
  private final Cache<String, Boolean> pipeExistsCache;
  private final boolean tableExistsCacheEnabled;
  private final boolean pipeExistsCacheEnabled;

  private final AtomicLong lastStatsLogTimestamp = new AtomicLong(System.currentTimeMillis());

  public CachingSnowflakeConnectionService(
      SnowflakeConnectionService delegate,
      SnowflakeConnectionPool pool,
      CachingConfig cachingConfig) {
    this.delegate = delegate;
    this.pool = pool;
    this.tableExistsCacheEnabled = cachingConfig.isTableExistsCacheEnabled();
    this.pipeExistsCacheEnabled = cachingConfig.isPipeExistsCacheEnabled();
    this.tableExistsCache =
        CacheBuilder.newBuilder()
            .expireAfterWrite(cachingConfig.getTableExistsCacheExpireMs(), TimeUnit.MILLISECONDS)
            .recordStats()
            .maximumSize(CACHE_SIZE)
            .build();
    this.pipeExistsCache =
        CacheBuilder.newBuilder()
            .expireAfterWrite(cachingConfig.getPipeExistsCacheExpireMs(), TimeUnit.MILLISECONDS)
            .maximumSize(CACHE_SIZE)
            .recordStats()
            .build();

    LOGGER.info(
        "Initialized CachingSnowflakeConnectionService - tableExists: {} ({}ms), pipeExists:"
            + " {} ({}ms), pool: {}",
        tableExistsCacheEnabled,
        cachingConfig.getTableExistsCacheExpireMs(),
        pipeExistsCacheEnabled,
        cachingConfig.getPipeExistsCacheExpireMs(),
        pool != null);
  }

  // -- Cached + pooled methods --

  @Override
  public boolean tableExist(final String tableName) {
    if (!tableExistsCacheEnabled) {
      return pooledTableExist(tableName);
    }

    try {
      boolean result = tableExistsCache.get(tableName, () -> pooledTableExist(tableName));
      logStatsIfNeeded();
      return result;
    } catch (Exception e) {
      throw new RuntimeException("Error checking table existence for: " + tableName, e);
    }
  }

  @Override
  public boolean pipeExist(final String pipeName) {
    if (!pipeExistsCacheEnabled) {
      return pooledPipeExist(pipeName);
    }

    try {
      boolean result = pipeExistsCache.get(pipeName, () -> pooledPipeExist(pipeName));
      logStatsIfNeeded();
      return result;
    } catch (Exception e) {
      throw new RuntimeException("Error checking pipe existence for: " + pipeName, e);
    }
  }

  // -- Pooled methods (concurrent-safe, no caching) --

  @Override
  public void createTableWithMetadataColumn(String tableName, boolean overwrite) {
    try (SnowflakeConnectionPool.Lease lease = pool.lease()) {
      lease.conn().createTableWithMetadataColumn(tableName, overwrite);
    }
    tableExistsCache.invalidate(tableName);
  }

  @Override
  public void createTableWithMetadataColumn(String tableName) {
    try (SnowflakeConnectionPool.Lease lease = pool.lease()) {
      lease.conn().createTableWithMetadataColumn(tableName);
    }
    tableExistsCache.invalidate(tableName);
  }

  @Override
  public void createTableWithOnlyMetadataColumn(String tableName) {
    try (SnowflakeConnectionPool.Lease lease = pool.lease()) {
      lease.conn().createTableWithOnlyMetadataColumn(tableName);
    }
    tableExistsCache.invalidate(tableName);
  }

  @Override
  public boolean isTableCompatible(String tableName) {
    try (SnowflakeConnectionPool.Lease lease = pool.lease()) {
      return lease.conn().isTableCompatible(tableName);
    }
  }

  @Override
  public Optional<List<DescribeTableRow>> describeTable(String tableName) {
    try (SnowflakeConnectionPool.Lease lease = pool.lease()) {
      return lease.conn().describeTable(tableName);
    }
  }

  @Override
  public void executeQueryWithParameters(String query, String... parameters) {
    try (SnowflakeConnectionPool.Lease lease = pool.lease()) {
      lease.conn().executeQueryWithParameters(query, parameters);
    }
    pipeExistsCache.invalidateAll();
    tableExistsCache.invalidateAll();
  }

  @Override
  public void appendColumnsToTable(String tableName, Map<String, ColumnInfos> columnInfosMap) {
    try (SnowflakeConnectionPool.Lease lease = pool.lease()) {
      lease.conn().appendColumnsToTable(tableName, columnInfosMap);
    }
  }

  @Override
  public void alterNonNullableColumns(String tableName, List<String> columnNames) {
    try (SnowflakeConnectionPool.Lease lease = pool.lease()) {
      lease.conn().alterNonNullableColumns(tableName, columnNames);
    }
  }

  @Override
  public boolean shouldEvolveSchema(String tableName, String role) {
    try (SnowflakeConnectionPool.Lease lease = pool.lease()) {
      return lease.conn().shouldEvolveSchema(tableName, role);
    }
  }

  // -- Delegate methods (single-threaded, no JDBC or lightweight) --

  @Override
  public void databaseExists(String databaseName) {
    delegate.databaseExists(databaseName);
  }

  @Override
  public void schemaExists(String schemaName) {
    delegate.schemaExists(schemaName);
  }

  @Override
  public SnowflakeTelemetryService getTelemetryClient() {
    return delegate.getTelemetryClient();
  }

  @Override
  public boolean isClosed() {
    return delegate.isClosed();
  }

  @Override
  public String getConnectorName() {
    return delegate.getConnectorName();
  }

  @Override
  public Connection getConnection() {
    return delegate.getConnection();
  }

  @Override
  public void close() {
    LOGGER.info("Closing CachingSnowflakeConnectionService, final cache statistics:");
    logCacheStatistics();
    if (pool != null) {
      pool.close();
    }
    delegate.close();
  }

  // -- Cache stats --

  /** Logs detailed cache statistics for both table and pipe caches. */
  public void logCacheStatistics() {
    if (tableExistsCacheEnabled) {
      CacheStats tableStats = tableExistsCache.stats();
      LOGGER.info(
          "Table cache stats - Requests: {}, Hits: {}, Misses: {}, Hit Rate: {}%, "
              + "Evictions: {}, Load Success: {}, Load Failures: {}, Avg Load Time: {}ms, Size: {}",
          tableStats.requestCount(),
          tableStats.hitCount(),
          tableStats.missCount(),
          String.format("%.2f", tableStats.hitRate() * 100),
          tableStats.evictionCount(),
          tableStats.loadSuccessCount(),
          tableStats.loadExceptionCount(),
          String.format("%.2f", tableStats.averageLoadPenalty() / 1_000_000.0),
          tableExistsCache.size());
    }

    if (pipeExistsCacheEnabled) {
      CacheStats pipeStats = pipeExistsCache.stats();
      LOGGER.info(
          "Pipe cache stats - Requests: {}, Hits: {}, Misses: {}, Hit Rate: {}%, "
              + "Evictions: {}, Load Success: {}, Load Failures: {}, Avg Load Time: {}ms, Size: {}",
          pipeStats.requestCount(),
          pipeStats.hitCount(),
          pipeStats.missCount(),
          String.format("%.2f", pipeStats.hitRate() * 100),
          pipeStats.evictionCount(),
          pipeStats.loadSuccessCount(),
          pipeStats.loadExceptionCount(),
          String.format("%.2f", pipeStats.averageLoadPenalty() / 1_000_000.0),
          pipeExistsCache.size());
    }
  }

  // -- Internals --

  private boolean pooledTableExist(String tableName) {
    try (SnowflakeConnectionPool.Lease lease = pool.lease()) {
      return lease.conn().tableExist(tableName);
    }
  }

  private boolean pooledPipeExist(String pipeName) {
    try (SnowflakeConnectionPool.Lease lease = pool.lease()) {
      return lease.conn().pipeExist(pipeName);
    }
  }

  private void logStatsIfNeeded() {
    final long now = System.currentTimeMillis();
    final long lastLogged = lastStatsLogTimestamp.get();
    if (now - lastLogged >= CACHE_STATS_LOG_INTERVAL_MS
        && lastStatsLogTimestamp.compareAndSet(lastLogged, now)) {
      logCacheStatistics();
    }
  }
}
