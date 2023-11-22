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
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.SNOWPIPE_STREAMING_FILE_VERSION;
import static net.snowflake.ingest.utils.ParameterProvider.BLOB_FORMAT_VERSION;

import com.google.common.annotations.VisibleForTesting;
import com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig;
import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.internal.KCLogger;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
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

  /** ONLY FOR TESTING - to get a provider with injected properties */
  @VisibleForTesting
  public static StreamingClientProvider getStreamingClientProviderForTests(
      StreamingClientHandler streamingClientHandler,
      LoadingCache<StreamingClientProperties, SnowflakeStreamingIngestClient> registeredClients) {
    return new StreamingClientProvider(streamingClientHandler, registeredClients);
  }

  /** ONLY FOR TESTING - private constructor to inject properties for testing */
  private StreamingClientProvider(
      StreamingClientHandler streamingClientHandler,
      LoadingCache<StreamingClientProperties, SnowflakeStreamingIngestClient> registeredClients) {
    this();
    this.streamingClientHandler = streamingClientHandler;
    this.registeredClients = registeredClients;
  }

  private static final KCLogger LOGGER = new KCLogger(StreamingClientProvider.class.getName());
  private StreamingClientHandler streamingClientHandler;
  private LoadingCache<StreamingClientProperties, SnowflakeStreamingIngestClient> registeredClients;
  private static final String STREAMING_CLIENT_PREFIX_NAME = "KC_CLIENT_";
  private static final String TEST_CLIENT_NAME = "TEST_CLIENT";

  // private constructor for singleton
  private StreamingClientProvider() {
    this.streamingClientHandler = new StreamingClientHandler();

    // if the one client optimization is enabled, we use this to cache the created clients based on
    // corresponding connector config. The cache calls streamingClientHandler to create the client
    // if the requested connector config has not already been loaded into the cache. When a client
    // is evicted, it will try closing the client, however it is best to still call close client
    // manually as eviction is executed lazily
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

    if (Boolean.parseBoolean(
        connectorConfig.getOrDefault(
            SnowflakeSinkConnectorConfig.ENABLE_STREAMING_CLIENT_OPTIMIZATION_CONFIG,
            Boolean.toString(ENABLE_STREAMING_CLIENT_OPTIMIZATION_DEFAULT)))) {
      LOGGER.info(
          "Streaming client optimization is enabled per worker node. Reusing valid clients when"
              + " possible");
      resultClient = this.registeredClients.get(clientProperties);

      // refresh if registered client is invalid
      if (!StreamingClientHandler.isClientValid(resultClient)) {
        resultClient = this.streamingClientHandler.createClient(clientProperties);
        this.registeredClients.put(clientProperties, resultClient);
      }
    } else {
      resultClient = this.streamingClientHandler.createClient(clientProperties);
      LOGGER.info(
          "Streaming client optimization is disabled, creating a new streaming client with name:"
              + " {}",
          resultClient.getName());
    }

    return resultClient;
  }

  /**
   * Closes the given client and deregisters it from the cache if necessary. It will also call close
   * on the registered client, which should be the same as the given client so the call will no-op.
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

  // TEST ONLY - return the current state of the registered clients
  @VisibleForTesting
  public Map<StreamingClientProperties, SnowflakeStreamingIngestClient> getRegisteredClients() {
    return this.registeredClients.asMap();
  }

  public static class StreamingClientProperties {
    public final Properties clientProperties;
    public final String clientName;
    public final Map<String, Object> parameterOverrides;

    public StreamingClientProperties(Map<String, String> connectorConfig) {
      this.clientProperties = StreamingUtils.convertConfigForStreamingClient(connectorConfig);

      this.clientName =
          STREAMING_CLIENT_PREFIX_NAME
              + connectorConfig.getOrDefault(Utils.NAME, TEST_CLIENT_NAME);

      // Override only if bdec version is explicitly set in config, default to the version set
      // inside Ingest SDK
      this.parameterOverrides = new HashMap<>();
      Optional<String> snowpipeStreamingBdecVersion =
          Optional.ofNullable(connectorConfig.get(SNOWPIPE_STREAMING_FILE_VERSION));
      snowpipeStreamingBdecVersion.ifPresent(
          overriddenValue -> {
            LOGGER.info("Config is overridden for {} ", SNOWPIPE_STREAMING_FILE_VERSION);
            parameterOverrides.put(BLOB_FORMAT_VERSION, overriddenValue);
          });
    }

    @Override
    public boolean equals(Object other) {
      return other.getClass().equals(StreamingClientProperties.class)
          & ((StreamingClientProperties) other).clientProperties.equals(this.clientProperties);
    }

    @Override
    public int hashCode() {
      return this.clientProperties.hashCode();
    }
  }
}
