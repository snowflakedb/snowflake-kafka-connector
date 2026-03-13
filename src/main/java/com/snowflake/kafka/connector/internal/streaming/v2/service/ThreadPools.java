package com.snowflake.kafka.connector.internal.streaming.v2.service;

import com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams;
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
 * <ul>
 *   <li><b>ioExecutor</b> — an unbounded cached thread pool for bursty blocking I/O: SDK client
 *       creation and batch offset fetching ({@code getChannelStatus} HTTP calls). Threads are
 *       created on demand and reclaimed after 60 s of idleness.
 *   <li><b>openChannelIoExecutor</b> — a fixed-size thread pool that rate-limits channel open
 *       operations. The size is controlled by {@code snowflake.open.channel.io.threads}.
 * </ul>
 *
 * <p>Like {@link com.snowflake.kafka.connector.internal.streaming.v2.client.StreamingClientPools},
 * this class uses a static {@link ConcurrentHashMap} keyed by connector name. Each connector gets
 * its own pools, and the pools are shut down when the last task for a connector calls {@link
 * #closeForTask(String, String)}.
 */
public class ThreadPools {
  private static final KCLogger LOGGER = new KCLogger(ThreadPools.class.getName());

  private static final Map<String, ConnectorThreadPool> connectorPools = new ConcurrentHashMap<>();

  private ThreadPools() {}

  /** Holds the executors and the set of task IDs currently using them. */
  private static class ConnectorThreadPool {
    final ExecutorService ioExecutor;
    final ExecutorService openChannelIoExecutor;
    final Set<String> taskIds = ConcurrentHashMap.newKeySet();

    ConnectorThreadPool(String connectorName, Map<String, String> connectorConfig) {
      LOGGER.info("Creating I/O thread pool for connector: {}", connectorName);
      this.ioExecutor =
          Executors.newCachedThreadPool(new DaemonThreadFactory(connectorName + "-io"));

      int maxThreads = KafkaConnectorConfigParams.SNOWFLAKE_OPEN_CHANNEL_IO_THREADS_DEFAULT;
      String configured =
          connectorConfig.get(KafkaConnectorConfigParams.SNOWFLAKE_OPEN_CHANNEL_IO_THREADS);
      if (configured != null) {
        maxThreads = Math.max(1, Integer.parseInt(configured.trim()));
      }
      LOGGER.info(
          "Creating channel thread pool for connector: {}, threads: {}", connectorName, maxThreads);
      this.openChannelIoExecutor =
          Executors.newFixedThreadPool(
              maxThreads, new DaemonThreadFactory(connectorName + "-channel"));
    }
  }

  /**
   * Returns the I/O executor (cached thread pool) for the given connector. The pool must have been
   * created by a prior call to {@link #registerTask(String, String, Map)}.
   */
  public static ExecutorService getIoExecutor(final String connectorName) {
    ConnectorThreadPool pool = connectorPools.get(connectorName);
    if (pool == null) {
      throw new IllegalStateException("No thread pool registered for connector: " + connectorName);
    }
    return pool.ioExecutor;
  }

  /**
   * Returns the open-channel executor (fixed-size thread pool) for the given connector. The pool
   * must have been created by a prior call to {@link #registerTask(String, String, Map)}.
   */
  public static ExecutorService getOpenChannelIoExecutor(final String connectorName) {
    ConnectorThreadPool pool = connectorPools.get(connectorName);
    if (pool == null) {
      throw new IllegalStateException("No thread pool registered for connector: " + connectorName);
    }
    return pool.openChannelIoExecutor;
  }

  /**
   * Registers a task as a user of the connector's thread pools, creating the pools if this is the
   * first task for the connector. Must be paired with a later call to {@link #closeForTask(String,
   * String)} to ensure the pools are shut down when no tasks remain.
   */
  public static void registerTask(
      final String connectorName, final String taskId, final Map<String, String> connectorConfig) {
    connectorPools.compute(
        connectorName,
        (key, pool) -> {
          if (pool == null) {
            pool = new ConnectorThreadPool(connectorName, connectorConfig);
          }
          pool.taskIds.add(taskId);
          return pool;
        });
  }

  /**
   * Unregisters a task from the connector's thread pools. When the last task unregisters, the
   * executors are shut down and removed from the registry.
   */
  public static void closeForTask(final String connectorName, final String taskId) {
    connectorPools.computeIfPresent(
        connectorName,
        (key, pool) -> {
          pool.taskIds.remove(taskId);
          if (pool.taskIds.isEmpty()) {
            LOGGER.info("Shutting down thread pools for connector: {}", connectorName);
            pool.ioExecutor.shutdownNow();
            pool.openChannelIoExecutor.shutdownNow();
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
