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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import net.snowflake.ingest.utils.Pair;

/**
 * Static factory that provides streaming client(s). There should only be one provider per KC worker
 * node, meaning that there may be multiple providers serving one connector and/or multiple
 * connectors on one provider. If the optimization is disabled, the provider will not reuse old
 * clients,/ see ENABLE_STREAMING_CLIENT_OPTIMIZATION_CONFIG in the {@link
 * SnowflakeSinkConnectorConfig }
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

  /** ONLY FOR TESTING - to get a provider with injected properties */
  @VisibleForTesting
  public static StreamingClientProvider getStreamingClientProviderForTests(
      Map<Properties, SnowflakeStreamingIngestClient> parameterEnabledPropsAndClients,
      StreamingClientHandler streamingClientHandler) {
    return new StreamingClientProvider(
        parameterEnabledPropsAndClients == null ? new HashMap<>() : parameterEnabledPropsAndClients,
        streamingClientHandler);
  }

  /** ONLY FOR TESTING - private constructor to inject properties for testing */
  private StreamingClientProvider(
      Map<Properties, SnowflakeStreamingIngestClient> registeredClientMap,
      StreamingClientHandler streamingClientHandler) {
    this();
    this.registeredClientMap = registeredClientMap;
    this.streamingClientHandler = streamingClientHandler;
  }

  private static final KCLogger LOGGER = new KCLogger(StreamingClientProvider.class.getName());
  private Map<Properties, SnowflakeStreamingIngestClient> registeredClientMap;
  private StreamingClientHandler streamingClientHandler;
  private Lock providerLock;

  // private constructor for singleton
  private StreamingClientProvider() {
    this.streamingClientHandler = new StreamingClientHandler();
    this.registeredClientMap = new HashMap<>();
    providerLock = new ReentrantLock(true);
  }

  /**
   * Gets the current client or creates a new one from the given connector config. If client
   * optimization is not enabled, it will create a new streaming client and the caller is
   * responsible for closing it
   *
   * @param connectorConfig The connector config
   * @return A streaming client
   */
  public SnowflakeStreamingIngestClient getClient(Map<String, String> connectorConfig) {
    SnowflakeStreamingIngestClient resultClient = null;

    if (Boolean.parseBoolean(
        connectorConfig.getOrDefault(
            SnowflakeSinkConnectorConfig.ENABLE_STREAMING_CLIENT_OPTIMIZATION_CONFIG,
            Boolean.toString(ENABLE_STREAMING_CLIENT_OPTIMIZATION_DEFAULT)))) {
      LOGGER.debug(
          "Streaming client optimization is enabled per worker node. Reusing valid clients when"
              + " possible");

      try {
        this.providerLock.lock();

        // remove invalid clients
        List<Map.Entry<Properties, SnowflakeStreamingIngestClient>> invalidClientProps =
            this.registeredClientMap.entrySet().stream()
                .filter(entry -> !StreamingClientHandler.isClientValid(entry.getValue()))
                .collect(Collectors.toList());
        if (!invalidClientProps.isEmpty()) {
          String invalidClientNames = "";

          for (Map.Entry<Properties, SnowflakeStreamingIngestClient> entry : invalidClientProps) {
            SnowflakeStreamingIngestClient invalidClient = entry.getValue();
            invalidClientNames +=
                invalidClient != null
                        && invalidClient.getName() != null
                        && !invalidClient.getName().isEmpty()
                    ? invalidClient.getName()
                    : "noClientNameFound";
            this.registeredClientMap.remove(entry.getKey());
          }

          LOGGER.error(
              "Found and removed {} invalid clients: {}",
              invalidClientProps.size(),
              invalidClientNames);
        }

        // look for client corresponding to the input properties or create new client
        Properties inputProps = StreamingClientHandler.getClientProperties(connectorConfig);
        if (this.registeredClientMap.containsKey(inputProps)) {
          resultClient = this.registeredClientMap.get(inputProps);
          LOGGER.debug("Using existing streaming client with name: {}", resultClient.getName());
        } else {
          Pair<Properties, SnowflakeStreamingIngestClient> propertiesAndClient =
              this.streamingClientHandler.createClient(connectorConfig);
          resultClient = propertiesAndClient.getSecond();
          this.registeredClientMap.put(
              propertiesAndClient.getFirst(), propertiesAndClient.getSecond());
          LOGGER.debug("Created and registered new client with name: {}", resultClient.getName());
        }
      } finally {
        this.providerLock.unlock();
      }
    } else {
      resultClient = this.streamingClientHandler.createClient(connectorConfig).getSecond();
      LOGGER.info(
          "Streaming client optimization is disabled, creating a new streaming client with name:"
              + " {}",
          resultClient.getName());
    }

    return resultClient;
  }

  /**
   * Closes the given client
   *
   * @param client The client to be closed
   */
  public void closeClient(SnowflakeStreamingIngestClient client) {
    this.providerLock.lock();
    this.streamingClientHandler.closeClient(client);
    this.providerLock.unlock();
  }
}
