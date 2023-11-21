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
import com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig;
import com.snowflake.kafka.connector.internal.KCLogger;
import java.util.Map;
import net.snowflake.ingest.internal.com.github.benmanes.caffeine.cache.Caffeine;
import net.snowflake.ingest.internal.com.github.benmanes.caffeine.cache.LoadingCache;
import net.snowflake.ingest.internal.com.github.benmanes.caffeine.cache.RemovalCause;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;

/**
 * Static factory that provides streaming client(s). If {@link
 * SnowflakeSinkConnectorConfig#ENABLE_STREAMING_CLIENT_OPTIMIZATION_CONFIG} is disabled then the
 * provider will always create a new client. If the optimization is enabled, then the provider will
 * reuse clients when possible. Clients will be reused on a per Kafka worker node and then per
 * connector level.
 */
public class StreamingClientProvider {
  private static class StreamingClientProviderSingleton {
    private static final StreamingClientProvider streamingClientProvider =
        new StreamingClientProvider();
  }

  /**
   * Gets the current streaming provider
   *
   * @return The streaming client provider
   */
  public static StreamingClientProvider getStreamingClientProviderInstance() {
    return StreamingClientProviderSingleton.streamingClientProvider;
  }

  /**
   * Builds the loading cache to register streaming clients
   * @param streamingClientHandler The handler to create clients with
   * @return A loading cache to register clients
   */
  public static LoadingCache<Map<String, String>, SnowflakeStreamingIngestClient> buildLoadingCache(StreamingClientHandler streamingClientHandler) {
    return Caffeine.newBuilder()
        .maximumSize(Runtime.getRuntime().maxMemory())
        .evictionListener(
            (Map<String, String> key,
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

  /** ONLY FOR TESTING - to get a provider with injected properties */
  @VisibleForTesting
  public static StreamingClientProvider getStreamingClientProviderForTests(
      StreamingClientHandler streamingClientHandler, LoadingCache<Map<String, String >, SnowflakeStreamingIngestClient> registeredClients) {
    return new StreamingClientProvider(streamingClientHandler, registeredClients);
  }

  /** ONLY FOR TESTING - private constructor to inject properties for testing */
  private StreamingClientProvider(StreamingClientHandler streamingClientHandler, LoadingCache<Map<String, String >, SnowflakeStreamingIngestClient> registeredClients) {
    this.streamingClientHandler = streamingClientHandler;
    this.registeredClients = registeredClients;
  }

  private static final KCLogger LOGGER = new KCLogger(StreamingClientProvider.class.getName());
  private StreamingClientHandler streamingClientHandler;
  private LoadingCache<Map<String, String>, SnowflakeStreamingIngestClient> registeredClients;

  // private constructor for singleton
  private StreamingClientProvider() {
    this.streamingClientHandler = new StreamingClientHandler();

    // if the one client optimization is enabled, we use this to cache the created clients based on
    // corresponding connector config. The cache calls streamingClientHandler to create the client
    // if the requested connector config has not already been loaded into the cache. When a client
    // is evicted, it will try closing the client, however it is best to still call close client manually as eviction is executed lazily
    this.registeredClients = buildLoadingCache(this.streamingClientHandler);
  }

  /**
   * Gets the current client or creates a new one from the given connector config. If client
   * optimization is not enabled, it will create a new streaming client and the caller is
   * responsible for closing it. If the optimization is enabled and the registered client is invalid, we will try recreating and reregistering the client
   *
   * @param connectorConfig The connector config
   * @return A streaming client
   */
  public SnowflakeStreamingIngestClient getClient(Map<String, String> connectorConfig) {
    SnowflakeStreamingIngestClient resultClient;

    if (Boolean.parseBoolean(
        connectorConfig.getOrDefault(
            SnowflakeSinkConnectorConfig.ENABLE_STREAMING_CLIENT_OPTIMIZATION_CONFIG,
            Boolean.toString(ENABLE_STREAMING_CLIENT_OPTIMIZATION_DEFAULT)))) {
      LOGGER.info(
          "Streaming client optimization is enabled per worker node. Reusing valid clients when"
              + " possible");
      resultClient = this.registeredClients.get(connectorConfig);

      // refresh if registered client is invalid
      if (!StreamingClientHandler.isClientValid(resultClient)) {
        resultClient = this.streamingClientHandler.createClient(connectorConfig);
        this.registeredClients.put(connectorConfig, resultClient);
      }
    } else {
      resultClient = this.streamingClientHandler.createClient(connectorConfig);
      LOGGER.info(
          "Streaming client optimization is disabled, creating a new streaming client with name:"
              + " {}",
          resultClient.getName());
    }

    return resultClient;
  }

  /**
   * Closes the given client and deregisters it from the cache if necessary
   *
   * @param client The client to be closed
   */
  public void closeClient(
      Map<String, String> connectorConfig, SnowflakeStreamingIngestClient client) {
    // invalidate cache
    SnowflakeStreamingIngestClient registeredClient =
        this.registeredClients.getIfPresent(connectorConfig);
    if (registeredClient != null) {
      // invalidations are processed on the next get or in the background, so we still need to close
      // the client here
      this.registeredClients.invalidate(connectorConfig);
      this.streamingClientHandler.closeClient(registeredClient);
    }

    // also close given client in case it is different from registered client. this should no-op if it is already closed
    this.streamingClientHandler.closeClient(client);
  }

  // TEST ONLY - return the current state of the registered clients
  @VisibleForTesting
  public Map<Map<String, String>, SnowflakeStreamingIngestClient> getRegisteredClients() {
    return this.registeredClients.asMap();
  }
}
