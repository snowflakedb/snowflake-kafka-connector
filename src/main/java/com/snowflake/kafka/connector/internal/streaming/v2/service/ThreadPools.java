package com.snowflake.kafka.connector.internal.streaming.v2.service;

import com.snowflake.kafka.connector.internal.KCLogger;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * JVM-global registry of per-connector thread pools.
 *
 * <p>Provides an unbounded cached thread pool for bursty blocking I/O: SDK client creation. Threads
 * are created on demand and reclaimed after 60 s of idleness.
 *
 * <p>Like {@link com.snowflake.kafka.connector.internal.streaming.v2.client.StreamingClientPools},
 * this class uses a static {@link ConcurrentHashMap} keyed by connector name. Each connector gets
 * its own pool, and the pool is shut down when the last task for a connector calls {@link
 * #closeForConnector(String)}.
 */
public class ThreadPools {
  private static final KCLogger LOGGER = new KCLogger(ThreadPools.class.getName());

  private static final Map<String, ExecutorService> connectors = new ConcurrentHashMap<>();

  private ThreadPools() {}

  /**
   * Returns the I/O executor (cached thread pool) for the given connector, creating it if it does
   * not yet exist.
   */
  public static ExecutorService getIoExecutor(final String connectorName) {
    return connectors.computeIfAbsent(
        connectorName,
        k -> {
          LOGGER.info("Creating I/O thread pool for connector: {}", connectorName);
          return Executors.newCachedThreadPool(new DaemonThreadFactory(connectorName + "-io"));
        });
  }

  /**
   * Shuts down the thread pool for the given connector and removes it from the registry. Safe to
   * call even if no pool exists for the connector.
   */
  public static void closeForConnector(final String connectorName) {
    connectors.computeIfPresent(
        connectorName,
        (key, executor) -> {
          LOGGER.info("Shutting down I/O thread pool for connector: {}", connectorName);
          executor.shutdownNow();
          return null;
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
