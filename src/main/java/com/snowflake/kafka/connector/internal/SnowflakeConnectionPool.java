package com.snowflake.kafka.connector.internal;

import java.time.Clock;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import org.apache.kafka.connect.errors.ConnectException;

/**
 * A bounded, lease-based pool of {@link SnowflakeConnectionService} instances.
 *
 * <p>Connections are created lazily (on demand) up to {@code maxSize}. A {@link Semaphore} bounds
 * the total number of connections that can exist at once (leased + idle). A {@link
 * ConcurrentLinkedDeque} holds only idle connections; leased connections are exclusively owned by
 * the caller with no pool reference, so there is no risk of evicting a connection that is in use.
 *
 * <p>Connections are health-checked on checkout ({@code isClosed()}), discarded when they exceed
 * {@code idleTimeoutMs} (time since last return) or {@code maxLifetimeMs} (time since creation),
 * and replaced transparently.
 */
public class SnowflakeConnectionPool implements AutoCloseable {
  private static final KCLogger LOGGER = new KCLogger(SnowflakeConnectionPool.class.getName());

  private final Supplier<SnowflakeConnectionService> factory;
  private final Semaphore permits;
  private final ConcurrentLinkedDeque<PooledConnection> idle = new ConcurrentLinkedDeque<>();
  private final int maxSize;
  private final long idleTimeoutMs;
  private final long maxLifetimeMs;
  private final long acquireTimeoutMs;
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private final Clock clock;

  /**
   * @param factory creates a new {@link SnowflakeConnectionService} on demand
   * @param maxSize maximum number of connections (leased + idle)
   * @param idleTimeoutMs connections idle longer than this are discarded on checkout
   * @param maxLifetimeMs absolute max age of a connection from creation; retired on checkout and
   *     return
   * @param acquireTimeoutMs how long {@link #acquire()} blocks waiting for a permit before throwing
   */
  public SnowflakeConnectionPool(
      Supplier<SnowflakeConnectionService> factory,
      int maxSize,
      long idleTimeoutMs,
      long maxLifetimeMs,
      long acquireTimeoutMs) {
    this(factory, maxSize, idleTimeoutMs, maxLifetimeMs, acquireTimeoutMs, Clock.systemUTC());
  }

  SnowflakeConnectionPool(
      Supplier<SnowflakeConnectionService> factory,
      int maxSize,
      long idleTimeoutMs,
      long maxLifetimeMs,
      long acquireTimeoutMs,
      Clock clock) {
    if (maxSize < 1) {
      throw new IllegalArgumentException("maxSize must be >= 1, got " + maxSize);
    }
    this.factory = factory;
    this.maxSize = maxSize;
    this.idleTimeoutMs = idleTimeoutMs;
    this.maxLifetimeMs = maxLifetimeMs;
    this.acquireTimeoutMs = acquireTimeoutMs;
    this.permits = new Semaphore(maxSize);
    this.clock = clock;
    LOGGER.info(
        "Connection pool created: maxSize={}, idleTimeoutMs={}, maxLifetimeMs={},"
            + " acquireTimeoutMs={}",
        maxSize,
        idleTimeoutMs,
        maxLifetimeMs,
        acquireTimeoutMs);
  }

  /**
   * Acquires a connection from the pool. Blocks up to {@code acquireTimeoutMs} if all connections
   * are currently leased.
   *
   * @return a healthy, non-expired connection
   * @throws ConnectException if the pool is closed, the timeout elapses, or the thread is
   *     interrupted
   */
  private static final long SLOW_ACQUIRE_THRESHOLD_MS = 1000;

  public SnowflakeConnectionService acquire() {
    if (closed.get()) {
      throw new ConnectException("Connection pool is closed");
    }

    long startMs = clock.millis();
    boolean permitAcquired = false;
    try {
      permitAcquired = permits.tryAcquire(acquireTimeoutMs, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ConnectException("Interrupted while waiting for a pooled connection", e);
    }

    if (!permitAcquired) {
      throw new ConnectException(
          "Timed out after "
              + acquireTimeoutMs
              + "ms waiting for a pooled connection (pool size: "
              + maxSize
              + ")");
    }

    try {
      SnowflakeConnectionService conn = pollOrCreate();
      long elapsedMs = clock.millis() - startMs;
      if (elapsedMs >= SLOW_ACQUIRE_THRESHOLD_MS) {
        LOGGER.warn(
            "Slow connection pool acquire: {}ms (pool size: {}, available permits: {})",
            elapsedMs,
            maxSize,
            permits.availablePermits());
      }
      return conn;
    } catch (RuntimeException e) {
      permits.release();
      throw e;
    }
  }

  /**
   * Returns a connection to the pool. If the connection has exceeded its max lifetime or is closed,
   * it is discarded instead.
   */
  public void release(SnowflakeConnectionService conn) {
    if (conn == null) {
      return;
    }

    PooledConnection pooled = findPooledConnection(conn);
    if (pooled == null) {
      // Not a pooled connection (e.g. created outside the pool) -- just close it.
      closeQuietly(conn);
      permits.release();
      return;
    }

    long now = clock.millis();

    if (closed.get() || isExpiredByLifetime(pooled, now) || conn.isClosed()) {
      closeQuietly(conn);
      permits.release();
      return;
    }

    pooled.lastReturnedAtMs = now;
    idle.offerFirst(pooled);
    permits.release();
  }

  /**
   * Returns an {@link AutoCloseable} lease that releases the connection on close. Intended for
   * try-with-resources usage.
   */
  public Lease lease() {
    return new Lease(acquire());
  }

  /** Shuts down the pool and closes all idle connections. */
  @Override
  public void close() {
    if (!closed.compareAndSet(false, true)) {
      return;
    }

    LOGGER.info("Closing connection pool (maxSize={})", maxSize);

    PooledConnection entry;
    int closedCount = 0;
    while ((entry = idle.pollFirst()) != null) {
      closeQuietly(entry.conn);
      closedCount++;
    }
    LOGGER.info("Connection pool closed, {} idle connections released", closedCount);
  }

  // -- internals --

  private SnowflakeConnectionService pollOrCreate() {
    long now = clock.millis();

    PooledConnection entry;
    while ((entry = idle.pollFirst()) != null) {
      if (isExpiredByLifetime(entry, now) || isIdleExpired(entry, now) || entry.conn.isClosed()) {
        closeQuietly(entry.conn);
        // The permit we're holding covers this slot; no need to release/reacquire.
        continue;
      }
      return entry.conn;
    }

    // Deque empty (or all entries were stale) -- create a new connection.
    SnowflakeConnectionService conn = factory.get();
    PooledConnection pooled = new PooledConnection(conn, now);
    pooled.lastReturnedAtMs = now;
    // We stash the PooledConnection on the connection so release() can find it.
    CONN_METADATA.put(conn, pooled);
    return conn;
  }

  private boolean isExpiredByLifetime(PooledConnection entry, long now) {
    return (now - entry.createdAtMs) >= maxLifetimeMs;
  }

  private boolean isIdleExpired(PooledConnection entry, long now) {
    return (now - entry.lastReturnedAtMs) >= idleTimeoutMs;
  }

  private PooledConnection findPooledConnection(SnowflakeConnectionService conn) {
    return CONN_METADATA.get(conn);
  }

  private static void closeQuietly(SnowflakeConnectionService conn) {
    try {
      if (!conn.isClosed()) {
        conn.close();
      }
    } catch (Exception e) {
      LOGGER.warn("Error closing pooled connection: {}", e.getMessage());
    } finally {
      CONN_METADATA.remove(conn);
    }
  }

  /**
   * Identity-based map from connection to its pool metadata. Uses a WeakHashMap-like approach via
   * ConcurrentHashMap (connections are strongly referenced by the pool or the caller, so they won't
   * be GC'd while in use).
   */
  private static final java.util.concurrent.ConcurrentHashMap<
          SnowflakeConnectionService, PooledConnection>
      CONN_METADATA = new java.util.concurrent.ConcurrentHashMap<>();

  static class PooledConnection {
    final SnowflakeConnectionService conn;
    final long createdAtMs;
    volatile long lastReturnedAtMs;

    PooledConnection(SnowflakeConnectionService conn, long createdAtMs) {
      this.conn = conn;
      this.createdAtMs = createdAtMs;
    }
  }

  /** An {@link AutoCloseable} wrapper that releases the connection back to the pool on close. */
  public class Lease implements AutoCloseable {
    private final SnowflakeConnectionService conn;

    Lease(SnowflakeConnectionService conn) {
      this.conn = conn;
    }

    public SnowflakeConnectionService conn() {
      return conn;
    }

    @Override
    public void close() {
      release(conn);
    }
  }
}
