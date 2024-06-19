/*
 * Copyright (c) 2023 Snowflake Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.snowflake.kafka.connector.internal.streaming;

import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.ENABLE_STREAMING_CLIENT_OPTIMIZATION_DEFAULT;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig;
import com.snowflake.kafka.connector.internal.KCLogger;
import java.util.Map;
import java.util.function.Supplier;
import net.snowflake.ingest.internal.com.github.benmanes.caffeine.cache.Caffeine;
import net.snowflake.ingest.internal.com.github.benmanes.caffeine.cache.LoadingCache;
import net.snowflake.ingest.internal.com.github.benmanes.caffeine.cache.RemovalCause;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;

/**
 * Static factory that provides streaming client(s). If {@link
 * SnowflakeSinkConnectorConfig#ENABLE_STREAMING_CLIENT_OPTIMIZATION_CONFIG} is disabled then the
 * provider will always create a new client. If the optimization is enabled, then the provider will
 * reuse clients when possible by registering clients internally. Since this is a static factory,
 * clients will be reused on a per Kafka worker node and based on it's {@link
 * StreamingClientProperties}. This means that multiple connectors/tasks on the same Kafka worker
 * node with equal {@link StreamingClientProperties} will use the same client
 */
public class StreamingClientProvider {
  private static volatile StreamingClientProvider streamingClientProvider = null;

  private static Supplier<StreamingClientHandler> clientHandlerSupplier =
      DirectStreamingClientHandler::new;

  /**
   * Gets the current streaming provider
   *
   * @return The streaming client provider
   */
  public static StreamingClientProvider getStreamingClientProviderInstance() {
    if (streamingClientProvider == null) {
      synchronized (StreamingClientProvider.class) {
        if (streamingClientProvider == null) {
          streamingClientProvider = new StreamingClientProvider(clientHandlerSupplier.get());
        }
      }
    }

    return streamingClientProvider;
  }

  /**
   * Gets the provider state to pre-initialization state. This method is currently used by the test
   * code only.
   */
  @VisibleForTesting
  public static void reset() {
    synchronized (StreamingClientProvider.class) {
      streamingClientProvider = null;
      clientHandlerSupplier = DirectStreamingClientHandler::new;
    }
  }

  /***
   * The method allows for providing custom {@link StreamingClientHandler} to be used by the connector
   * instead of the default that is {@link DirectStreamingClientHandler}
   *
   * This method is currently used by the test code only.
   *
   * @param streamingClientHandler The handler that will be used by the connector.
   */
  @VisibleForTesting
  public static void overrideStreamingClientHandler(StreamingClientHandler streamingClientHandler) {
    Preconditions.checkState(
        streamingClientProvider == null,
        "StreamingClientProvider is already initialized and cannot be overridden.");
    synchronized (StreamingClientProvider.class) {
      clientHandlerSupplier = () -> streamingClientHandler;
    }
  }

  /**
   * Builds a threadsafe loading cache to register at max 10,000 streaming clients. It maps each
   * {@link StreamingClientProperties} to it's corresponding {@link SnowflakeStreamingIngestClient}
   *
   * @param streamingClientHandler The handler to create clients with
   * @return A loading cache to register clients
   */
  public static LoadingCache<StreamingClientProperties, SnowflakeStreamingIngestClient>
      buildLoadingCache(StreamingClientHandler streamingClientHandler) {
    return Caffeine.newBuilder()
        .maximumSize(10000) // limit 10,000 clients
        .evictionListener(
            (StreamingClientProperties key,
                SnowflakeStreamingIngestClient client,
                RemovalCause removalCause) -> {
              streamingClientHandler.closeClient(client);
              LOGGER.info(
                  "Removed registered client {} due to {}",
                  client.getName(),
                  removalCause.toString());
            })
        .build(streamingClientHandler::createClient);
  }

  /***************************** BEGIN SINGLETON CODE *****************************/
  private static final KCLogger LOGGER = new KCLogger(StreamingClientProvider.class.getName());

  private final StreamingClientHandler streamingClientHandler;
  private LoadingCache<StreamingClientProperties, SnowflakeStreamingIngestClient> registeredClients;

  /**
   * Private constructor to retain singleton
   *
   * <p>If the one client optimization is enabled, this creates a threadsafe {@link LoadingCache} to
   * register created clients based on the corresponding {@link StreamingClientProperties} built
   * from the given connector configuration. The cache calls streamingClientHandler to create the
   * client if the requested streaming client properties has not already been loaded into the cache.
   * When a client is evicted, the cache will try closing the client, however it is best to still
   * call close client manually as eviction is executed lazily
   */
  private StreamingClientProvider(StreamingClientHandler streamingClientHandler) {
    this.streamingClientHandler = streamingClientHandler;
    this.registeredClients = buildLoadingCache(this.streamingClientHandler);
  }

  /**
   * Gets the current client or creates a new one from the given connector config. If client
   * optimization is not enabled, it will create a new streaming client and the caller is
   * responsible for closing it. If the optimization is enabled and the registered client is
   * invalid, we will try recreating and reregistering the client
   *
   * @param connectorConfig The connector config
   * @return A streaming client
   */
  public SnowflakeStreamingIngestClient getClient(Map<String, String> connectorConfig) {
    SnowflakeStreamingIngestClient resultClient;
    StreamingClientProperties clientProperties = new StreamingClientProperties(connectorConfig);
    final boolean isOptimizationEnabled =
        Boolean.parseBoolean(
            connectorConfig.getOrDefault(
                SnowflakeSinkConnectorConfig.ENABLE_STREAMING_CLIENT_OPTIMIZATION_CONFIG,
                Boolean.toString(ENABLE_STREAMING_CLIENT_OPTIMIZATION_DEFAULT)));

    if (isOptimizationEnabled) {
      resultClient = this.registeredClients.get(clientProperties);

      // refresh if registered client is invalid
      if (!StreamingClientHandler.isClientValid(resultClient)) {
        LOGGER.warn(
            "Registered streaming client is not valid, recreating and registering new client");
        resultClient = this.streamingClientHandler.createClient(clientProperties);
        this.registeredClients.put(clientProperties, resultClient);
      }
    } else {
      resultClient = this.streamingClientHandler.createClient(clientProperties);
    }

    LOGGER.info(
        "Streaming client optimization is {}. Returning client with name: {}",
        isOptimizationEnabled
            ? "enabled per worker node, KC will reuse valid clients when possible"
            : "disabled, KC will create new clients",
        resultClient.getName());

    return resultClient;
  }

  /**
   * Closes the given client and deregisters it from the cache if necessary. It will also call close
   * on the registered client if exists, which should be the same as the given client so the call
   * will no-op.
   *
   * @param connectorConfig The configuration to deregister from the cache
   * @param client The client to be closed
   */
  public void closeClient(
      Map<String, String> connectorConfig, SnowflakeStreamingIngestClient client) {
    StreamingClientProperties clientProperties = new StreamingClientProperties(connectorConfig);

    // invalidate cache
    SnowflakeStreamingIngestClient registeredClient =
        this.registeredClients.getIfPresent(clientProperties);
    if (registeredClient != null) {
      // invalidations are processed on the next get or in the background, so we still need to close
      // the client here
      this.registeredClients.invalidate(clientProperties);
      this.streamingClientHandler.closeClient(registeredClient);
    }

    // also close given client in case it is different from registered client. this should no-op if
    // it is already closed
    this.streamingClientHandler.closeClient(client);
  }

  // TEST ONLY - to get a provider with injected properties
  @VisibleForTesting
  public static StreamingClientProvider getStreamingClientProviderForTests(
      StreamingClientHandler streamingClientHandler,
      LoadingCache<StreamingClientProperties, SnowflakeStreamingIngestClient> registeredClients) {
    return new StreamingClientProvider(streamingClientHandler, registeredClients);
  }

  // TEST ONLY - private constructor to inject properties for testing
  @VisibleForTesting
  private StreamingClientProvider(
      StreamingClientHandler streamingClientHandler,
      LoadingCache<StreamingClientProperties, SnowflakeStreamingIngestClient> registeredClients) {
    this.streamingClientHandler = streamingClientHandler;
    this.registeredClients = registeredClients;
  }

  // TEST ONLY - return the current state of the registered clients
  @VisibleForTesting
  public Map<StreamingClientProperties, SnowflakeStreamingIngestClient> getRegisteredClients() {
    return this.registeredClients.asMap();
  }
}
