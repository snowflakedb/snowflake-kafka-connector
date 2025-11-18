package com.snowflake.kafka.connector.internal;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheStats;
import com.snowflake.kafka.connector.internal.streaming.schemaevolution.ColumnInfos;
import com.snowflake.kafka.connector.internal.telemetry.SnowflakeTelemetryService;
import java.sql.Connection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Decorator implementation of SnowflakeConnectionService that adds caching for table and pipe
 * existence checks. This class wraps an existing SnowflakeConnectionService and intercepts calls to
 * tableExist() and pipeExist() to provide caching.
 */
public class CachingSnowflakeConnectionService implements SnowflakeConnectionService {

  private static final KCLogger LOGGER =
      new KCLogger(CachingSnowflakeConnectionService.class.getName());

  private static final long STATS_LOG_INTERVAL = 1000; // Log stats every 1000 operations
  private final SnowflakeConnectionService delegate;
  private final Cache<String, Boolean> tableExistsCache;
  private final Cache<String, Boolean> pipeExistsCache;
  private final boolean tableExistsCacheEnabled;
  private final boolean pipeExistsCacheEnabled;

  private final AtomicLong operationCount = new AtomicLong(0);

  /**
   * Creates a cached wrapper around an existing SnowflakeConnectionService.
   *
   * @param delegate the underlying connection service to wrap
   * @param cachingConfig cache configuration settings
   */
  public CachingSnowflakeConnectionService(
      SnowflakeConnectionService delegate, CachingConfig cachingConfig) {
    final int cacheSize = 100;
    this.delegate = delegate;
    this.tableExistsCacheEnabled = cachingConfig.isTableExistsCacheEnabled();
    this.pipeExistsCacheEnabled = cachingConfig.isPipeExistsCacheEnabled();
    this.tableExistsCache =
        CacheBuilder.newBuilder()
            .expireAfterWrite(cachingConfig.getTableExistsCacheExpireMs(), TimeUnit.MILLISECONDS)
            .recordStats()
            .maximumSize(cacheSize)
            .build();
    this.pipeExistsCache =
        CacheBuilder.newBuilder()
            .expireAfterWrite(cachingConfig.getPipeExistsCacheExpireMs(), TimeUnit.MILLISECONDS)
            .maximumSize(cacheSize)
            .recordStats()
            .build();

    LOGGER.info(
        "Initialized cached connection service - tableExists: {} ({}ms), pipeExists: {} ({}ms)",
        tableExistsCacheEnabled,
        cachingConfig.getTableExistsCacheExpireMs(),
        pipeExistsCacheEnabled,
        cachingConfig.getPipeExistsCacheExpireMs());
  }

  @Override
  public boolean tableExist(final String tableName) {
    if (!tableExistsCacheEnabled) {
      return delegate.tableExist(tableName);
    }

    try {
      boolean result = tableExistsCache.get(tableName, () -> delegate.tableExist(tableName));
      logStatsIfNeeded();
      return result;
    } catch (Exception e) {
      throw new RuntimeException("Error accessing table exists cache for table: " + tableName, e);
    }
  }

  @Override
  public boolean pipeExist(final String pipeName) {
    if (!pipeExistsCacheEnabled) {
      return delegate.pipeExist(pipeName);
    }

    try {
      boolean result = pipeExistsCache.get(pipeName, () -> delegate.pipeExist(pipeName));
      logStatsIfNeeded();
      return result;
    } catch (Exception e) {
      throw new RuntimeException("Error accessing pipe exists cache for pipe: {}" + pipeName, e);
    }
  }

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
          String.format(
              "%.2f",
              tableStats.averageLoadPenalty() / 1_000_000.0), // Convert nanoseconds to milliseconds
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
          String.format(
              "%.2f",
              pipeStats.averageLoadPenalty() / 1_000_000.0), // Convert nanoseconds to milliseconds
          pipeExistsCache.size());
    }
  }

  // All other methods delegate directly without caching

  @Override
  public void createTable(String tableName, boolean overwrite) {
    delegate.createTable(tableName, overwrite);
    tableExistsCache.invalidate(tableName);
  }

  @Override
  public void createTable(String tableName) {
      delegate.createTable(tableName);
      tableExistsCache.invalidate(tableName);
  }

  @Override
  public void createTableWithOnlyMetadataColumn(String tableName) {
    delegate.createTableWithOnlyMetadataColumn(tableName);
    tableExistsCache.invalidate(tableName);
  }

  @Override
  public void addMetadataColumnForIcebergIfNotExists(String tableName) {
    delegate.addMetadataColumnForIcebergIfNotExists(tableName);
  }

  @Override
  public void initializeMetadataColumnTypeForIceberg(String tableName) {
    delegate.initializeMetadataColumnTypeForIceberg(tableName);
  }

  @Override
  public boolean isTableCompatible(String tableName) {
    return delegate.isTableCompatible(tableName);
  }

  @Override
  public boolean hasSchemaEvolutionPermission(String tableName, String role) {
    return delegate.hasSchemaEvolutionPermission(tableName, role);
  }

  @Override
  public void appendColumnsToTable(String tableName, Map<String, ColumnInfos> columnInfosMap) {
    delegate.appendColumnsToTable(tableName, columnInfosMap);
  }

  @Override
  public void alterColumnsDataTypeIcebergTable(
      String tableName, Map<String, ColumnInfos> columnInfosMap) {
    delegate.alterColumnsDataTypeIcebergTable(tableName, columnInfosMap);
  }

  @Override
  public void appendColumnsToIcebergTable(
      String tableName, Map<String, ColumnInfos> columnInfosMap) {
    delegate.appendColumnsToIcebergTable(tableName, columnInfosMap);
  }

  @Override
  public void alterNonNullableColumns(String tableName, List<String> columnNames) {
    delegate.alterNonNullableColumns(tableName, columnNames);
  }

  @Override
  public void databaseExists(String databaseName) {
    delegate.databaseExists(databaseName);
  }

  @Override
  public void schemaExists(String schemaName) {
    delegate.schemaExists(schemaName);
  }

  @Override
  public void dropPipe(String pipeName) {
    delegate.dropPipe(pipeName);
    pipeExistsCache.invalidate(pipeName);
  }

  @Override
  public SnowflakeTelemetryService getTelemetryClient() {
    return delegate.getTelemetryClient();
  }

  @Override
  public void close() {
    LOGGER.info("Closing CachedSnowflakeConnectionService, final cache statistics:");
    logCacheStatistics();
    delegate.close();
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
  public Optional<List<DescribeTableRow>> describeTable(String tableName) {
    return delegate.describeTable(tableName);
  }

  @Override
  public void executeQueryWithParameters(String query, String... parameters) {
    delegate.executeQueryWithParameters(query, parameters);
    pipeExistsCache.invalidateAll();
    tableExistsCache.invalidateAll();
  }

  @Override
  public void appendMetaColIfNotExist(String tableName) {
    delegate.appendMetaColIfNotExist(tableName);
  }

    /** Logs cache statistics periodically based on operation count. */
    private void logStatsIfNeeded() {
        long count = operationCount.incrementAndGet();
        if (count % STATS_LOG_INTERVAL == 0) {
            logCacheStatistics();
        }
    }

}
