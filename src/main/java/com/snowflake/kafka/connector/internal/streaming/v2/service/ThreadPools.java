package com.snowflake.kafka.connector.internal.streaming.v2.service;

import com.snowflake.kafka.connector.internal.KCLogger;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * JVM-global registry of per-connector thread pools.
 *
 * <p>Provides an unbounded cached thread pool for bursty blocking I/O: SDK client creation and
 * batch offset fetching ({@code getChannelStatus} HTTP calls). Threads are created on demand and
 * reclaimed after 60 s of idleness.
 *
 * <p>Like {@link com.snowflake.kafka.connector.internal.streaming.v2.client.StreamingClientPools},
 * this class uses a static {@link ConcurrentHashMap} keyed by connector name. Each connector gets
 * its own pool, and the pool is shut down when the last task for a connector calls {@link
 * #closeForTask(String, String)}.
 */
public class ThreadPools {
  private static final KCLogger LOGGER = new KCLogger(ThreadPools.class.getName());

  private static final Map<String, ConnectorThreadPool> connectorPools = new ConcurrentHashMap<>();

  private ThreadPools() {}

  /** Holds the executor and the set of task IDs currently using it. */
  private static class ConnectorThreadPool {
    final ExecutorService ioExecutor;
    final Set<String> taskIds = ConcurrentHashMap.newKeySet();

    ConnectorThreadPool(String connectorName) {
      LOGGER.info("Creating I/O thread pool for connector: {}", connectorName);
      this.ioExecutor =
          Executors.newCachedThreadPool(new DaemonThreadFactory(connectorName + "-io"));
    }
  }

  /**
   * Returns the I/O executor (cached thread pool) for the given connector, creating it if it does
   * not yet exist.
   */
  public static ExecutorService getIoExecutor(final String connectorName) {
    return connectorPools.computeIfAbsent(
            connectorName,
            k -> {
              // This is a logical error but we can recover.
              LOGGER.warn("Connector thread pool not found for connector: {}", connectorName);
              return new ConnectorThreadPool(connectorName);
            })
        .ioExecutor;
  }

  /**
   * Registers a task as a user of the connector's thread pool. Must be paired with a later call to
   * {@link #closeForTask(String, String)} to ensure the pool is shut down when no tasks remain.
   */
  public static void registerTask(final String connectorName, final String taskId) {
    connectorPools.compute(
        connectorName,
        (key, pool) -> {
          if (pool == null) {
            pool = new ConnectorThreadPool(connectorName);
          }
          pool.taskIds.add(taskId);
          return pool;
        });
  }

  /**
   * Unregisters a task from the connector's thread pool. When the last task unregisters, the
   * executor is shut down and removed from the registry.
   */
  public static void closeForTask(final String connectorName, final String taskId) {
    connectorPools.computeIfPresent(
        connectorName,
        (key, pool) -> {
          pool.taskIds.remove(taskId);
          if (pool.taskIds.isEmpty()) {
            LOGGER.info("Shutting down I/O thread pool for connector: {}", connectorName);
            pool.ioExecutor.shutdownNow();
            return null;
          }
          return pool;
        });
  }

  /**
   * The context class loader is captured at factory creation time because Kafka Connect uses a
   * PluginClassLoader that must be on the thread context for the SDK's native library loading
   * (FFIBootstrap) to find resources inside plugin JARs.
   */
  private static final class DaemonThreadFactory implements ThreadFactory {
    private final AtomicInteger counter = new AtomicInteger(0);
    private final String prefix;
    private final ClassLoader contextClassLoader;

    DaemonThreadFactory(String prefix) {
      this.prefix = prefix;
      this.contextClassLoader = Thread.currentThread().getContextClassLoader();
    }

    @Override
    public Thread newThread(Runnable r) {
      Thread t = new Thread(r, prefix + "-" + counter.getAndIncrement());
      t.setDaemon(true);
      t.setContextClassLoader(contextClassLoader);
      return t;
    }
  }
}
