package com.snowflake.kafka.connector.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SnowflakeConnectionPoolTest {

  private static final long IDLE_TIMEOUT_MS = 5_000;
  private static final long MAX_LIFETIME_MS = 30_000;
  private static final long ACQUIRE_TIMEOUT_MS = 1_000;

  private final AtomicInteger createCount = new AtomicInteger(0);
  private final List<SnowflakeConnectionService> createdConnections = new ArrayList<>();
  private Clock clock;
  private long currentTimeMs;

  @BeforeEach
  void setUp() {
    currentTimeMs = 1_000_000L;
    clock = mock(Clock.class);
    when(clock.millis()).thenAnswer(inv -> currentTimeMs);
    createCount.set(0);
    createdConnections.clear();
  }

  @AfterEach
  void tearDown() {
    createdConnections.clear();
  }

  private Supplier<SnowflakeConnectionService> mockFactory() {
    return () -> {
      createCount.incrementAndGet();
      SnowflakeConnectionService conn = mock(SnowflakeConnectionService.class);
      when(conn.isClosed()).thenReturn(false);
      createdConnections.add(conn);
      return conn;
    };
  }

  private SnowflakeConnectionPool createPool(int maxSize) {
    return new SnowflakeConnectionPool(
        mockFactory(), maxSize, IDLE_TIMEOUT_MS, MAX_LIFETIME_MS, ACQUIRE_TIMEOUT_MS, clock);
  }

  // --- basic acquire/release ---

  @Test
  void acquireCreatesNewConnectionWhenPoolIsEmpty() {
    try (SnowflakeConnectionPool pool = createPool(5)) {
      SnowflakeConnectionService conn = pool.acquire();
      assertEquals(1, createCount.get());
      pool.release(conn);
    }
  }

  @Test
  void releaseReturnsConnectionToPoolForReuse() {
    try (SnowflakeConnectionPool pool = createPool(5)) {
      SnowflakeConnectionService conn1 = pool.acquire();
      pool.release(conn1);

      SnowflakeConnectionService conn2 = pool.acquire();
      assertSame(conn1, conn2, "Should reuse the released connection");
      assertEquals(1, createCount.get(), "Should not create a new connection");
      pool.release(conn2);
    }
  }

  // --- bounding ---

  @Test
  void acquireBlocksWhenPoolIsExhaustedAndTimesOut() {
    try (SnowflakeConnectionPool pool = createPool(1)) {
      SnowflakeConnectionService conn = pool.acquire();

      ConnectException ex = assertThrows(ConnectException.class, pool::acquire);
      assertTrue(ex.getMessage().contains("Timed out"));

      pool.release(conn);
    }
  }

  @Test
  void releaseUnblocksWaitingAcquire() throws Exception {
    try (SnowflakeConnectionPool pool = createPool(1)) {
      SnowflakeConnectionService conn1 = pool.acquire();

      CompletableFuture<SnowflakeConnectionService> future =
          CompletableFuture.supplyAsync(pool::acquire);

      Thread.sleep(50);
      pool.release(conn1);

      SnowflakeConnectionService conn2 = future.get(5, TimeUnit.SECONDS);
      assertSame(conn1, conn2, "Blocked acquire should get the released connection");
      pool.release(conn2);
    }
  }

  @Test
  void multipleConnectionsUpToMaxSize() {
    try (SnowflakeConnectionPool pool = createPool(3)) {
      SnowflakeConnectionService c1 = pool.acquire();
      SnowflakeConnectionService c2 = pool.acquire();
      SnowflakeConnectionService c3 = pool.acquire();

      assertEquals(3, createCount.get());
      assertNotSame(c1, c2);
      assertNotSame(c2, c3);

      pool.release(c1);
      pool.release(c2);
      pool.release(c3);
    }
  }

  // --- idle expiry ---

  @Test
  void idleConnectionIsEvictedOnCheckout() {
    try (SnowflakeConnectionPool pool = createPool(5)) {
      SnowflakeConnectionService conn1 = pool.acquire();
      pool.release(conn1);

      currentTimeMs += IDLE_TIMEOUT_MS + 1;

      SnowflakeConnectionService conn2 = pool.acquire();
      assertNotSame(conn1, conn2, "Idle-expired connection should be discarded");
      assertEquals(2, createCount.get());
      verify(conn1).close();
      pool.release(conn2);
    }
  }

  @Test
  void nonIdleConnectionIsReused() {
    try (SnowflakeConnectionPool pool = createPool(5)) {
      SnowflakeConnectionService conn1 = pool.acquire();
      pool.release(conn1);

      currentTimeMs += IDLE_TIMEOUT_MS - 1;

      SnowflakeConnectionService conn2 = pool.acquire();
      assertSame(conn1, conn2, "Non-idle connection should be reused");
      assertEquals(1, createCount.get());
      pool.release(conn2);
    }
  }

  // --- max lifetime ---

  @Test
  void maxLifetimeExpiredConnectionIsEvictedOnCheckout() {
    try (SnowflakeConnectionPool pool = createPool(5)) {
      SnowflakeConnectionService conn1 = pool.acquire();
      pool.release(conn1);

      currentTimeMs += MAX_LIFETIME_MS + 1;

      SnowflakeConnectionService conn2 = pool.acquire();
      assertNotSame(conn1, conn2, "Lifetime-expired connection should be discarded");
      assertEquals(2, createCount.get());
      verify(conn1).close();
      pool.release(conn2);
    }
  }

  @Test
  void maxLifetimeExpiredConnectionIsDiscardedOnReturn() {
    try (SnowflakeConnectionPool pool = createPool(5)) {
      SnowflakeConnectionService conn1 = pool.acquire();

      currentTimeMs += MAX_LIFETIME_MS + 1;
      pool.release(conn1);

      verify(conn1).close();

      SnowflakeConnectionService conn2 = pool.acquire();
      assertNotSame(conn1, conn2, "Should create a new connection after lifetime expiry");
      assertEquals(2, createCount.get());
      pool.release(conn2);
    }
  }

  // --- health check ---

  @Test
  void closedConnectionIsDiscardedOnCheckout() {
    try (SnowflakeConnectionPool pool = createPool(5)) {
      SnowflakeConnectionService conn1 = pool.acquire();
      pool.release(conn1);

      when(conn1.isClosed()).thenReturn(true);

      SnowflakeConnectionService conn2 = pool.acquire();
      assertNotSame(conn1, conn2, "Closed connection should be discarded");
      assertEquals(2, createCount.get());
      pool.release(conn2);
    }
  }

  @Test
  void closedConnectionIsDiscardedOnReturn() {
    try (SnowflakeConnectionPool pool = createPool(5)) {
      SnowflakeConnectionService conn1 = pool.acquire();
      when(conn1.isClosed()).thenReturn(true);
      pool.release(conn1);

      SnowflakeConnectionService conn2 = pool.acquire();
      assertNotSame(conn1, conn2, "Should create a new connection");
      assertEquals(2, createCount.get());
      pool.release(conn2);
    }
  }

  // --- lease ---

  @Test
  void leaseAutoReleasesOnClose() {
    try (SnowflakeConnectionPool pool = createPool(1)) {
      SnowflakeConnectionService conn;

      try (SnowflakeConnectionPool.Lease lease = pool.lease()) {
        conn = lease.conn();
      }

      SnowflakeConnectionService conn2 = pool.acquire();
      assertSame(conn, conn2, "Lease should have returned the connection to the pool");
      pool.release(conn2);
    }
  }

  // --- close pool ---

  @Test
  void closePoolClosesAllIdleConnections() {
    SnowflakeConnectionPool pool = createPool(5);

    SnowflakeConnectionService c1 = pool.acquire();
    SnowflakeConnectionService c2 = pool.acquire();
    pool.release(c1);
    pool.release(c2);

    pool.close();

    verify(c1).close();
    verify(c2).close();
  }

  @Test
  void acquireAfterCloseThrows() {
    SnowflakeConnectionPool pool = createPool(5);
    pool.close();

    assertThrows(ConnectException.class, pool::acquire);
  }

  @Test
  void releaseAfterCloseDiscardsConnection() {
    SnowflakeConnectionPool pool = createPool(5);
    SnowflakeConnectionService conn = pool.acquire();
    pool.close();
    pool.release(conn);

    verify(conn).close();
  }

  @Test
  void doubleCloseIsIdempotent() {
    SnowflakeConnectionPool pool = createPool(5);
    pool.close();
    pool.close();
  }

  // --- concurrency ---

  @Test
  void concurrentAcquireReleaseDoesNotExceedMaxSize() throws Exception {
    int maxSize = 3;
    int threadCount = 10;
    int iterationsPerThread = 50;

    Clock realClock = Clock.systemUTC();
    AtomicInteger concurrentLeases = new AtomicInteger(0);
    AtomicInteger maxConcurrentLeases = new AtomicInteger(0);

    try (SnowflakeConnectionPool pool =
        new SnowflakeConnectionPool(
            mockFactory(), maxSize, IDLE_TIMEOUT_MS, MAX_LIFETIME_MS, 5_000, realClock)) {

      ExecutorService executor = Executors.newFixedThreadPool(threadCount);
      CountDownLatch startLatch = new CountDownLatch(1);
      CountDownLatch doneLatch = new CountDownLatch(threadCount);

      for (int t = 0; t < threadCount; t++) {
        executor.submit(
            () -> {
              try {
                startLatch.await();
                for (int i = 0; i < iterationsPerThread; i++) {
                  SnowflakeConnectionService conn = pool.acquire();
                  int current = concurrentLeases.incrementAndGet();
                  maxConcurrentLeases.updateAndGet(max -> Math.max(max, current));
                  Thread.yield();
                  concurrentLeases.decrementAndGet();
                  pool.release(conn);
                }
              } catch (Exception e) {
                throw new RuntimeException(e);
              } finally {
                doneLatch.countDown();
              }
            });
      }

      startLatch.countDown();
      assertTrue(doneLatch.await(30, TimeUnit.SECONDS), "Threads should complete");
      executor.shutdownNow();

      assertTrue(
          maxConcurrentLeases.get() <= maxSize,
          "Max concurrent leases ("
              + maxConcurrentLeases.get()
              + ") should not exceed maxSize ("
              + maxSize
              + ")");
    }
  }

  // --- null release ---

  @Test
  void releaseNullIsNoOp() {
    try (SnowflakeConnectionPool pool = createPool(5)) {
      pool.release(null);
    }
  }

  // --- factory failure ---

  @Test
  void factoryFailureReleasesPermit() {
    Supplier<SnowflakeConnectionService> failingFactory =
        () -> {
          throw new RuntimeException("connection failed");
        };

    try (SnowflakeConnectionPool pool =
        new SnowflakeConnectionPool(
            failingFactory, 1, IDLE_TIMEOUT_MS, MAX_LIFETIME_MS, ACQUIRE_TIMEOUT_MS, clock)) {
      assertThrows(RuntimeException.class, pool::acquire);

      // The permit should have been released, so a subsequent acquire (with a working factory)
      // should not time out. We can't change the factory, but we can verify the permit was released
      // by checking that a second call also throws from the factory (not from timeout).
      RuntimeException ex = assertThrows(RuntimeException.class, pool::acquire);
      assertEquals("connection failed", ex.getMessage());
    }
  }

  // --- maxSize validation ---

  @Test
  void maxSizeZeroThrows() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new SnowflakeConnectionPool(
                mockFactory(), 0, IDLE_TIMEOUT_MS, MAX_LIFETIME_MS, ACQUIRE_TIMEOUT_MS));
  }

  // --- stale entries are drained in sequence ---

  @Test
  void multipleStaleIdleConnectionsAreDrainedBeforeCreatingNew() {
    try (SnowflakeConnectionPool pool = createPool(5)) {
      SnowflakeConnectionService c1 = pool.acquire();
      SnowflakeConnectionService c2 = pool.acquire();
      SnowflakeConnectionService c3 = pool.acquire();
      pool.release(c1);
      pool.release(c2);
      pool.release(c3);

      currentTimeMs += IDLE_TIMEOUT_MS + 1;

      SnowflakeConnectionService fresh = pool.acquire();
      assertNotSame(c1, fresh);
      assertNotSame(c2, fresh);
      assertNotSame(c3, fresh);

      verify(c1).close();
      verify(c2).close();
      verify(c3).close();
      assertEquals(4, createCount.get());

      pool.release(fresh);
    }
  }

  // --- idle timeout updates on return ---

  @Test
  void idleTimeoutResetsOnReturn() {
    try (SnowflakeConnectionPool pool = createPool(5)) {
      SnowflakeConnectionService conn1 = pool.acquire();

      currentTimeMs += IDLE_TIMEOUT_MS - 100;
      pool.release(conn1);

      currentTimeMs += IDLE_TIMEOUT_MS - 100;
      SnowflakeConnectionService conn2 = pool.acquire();
      assertSame(conn1, conn2, "Connection should still be valid after re-return reset idle timer");
      verify(conn1, never()).close();
      pool.release(conn2);
    }
  }
}
