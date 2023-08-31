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
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;

/**
 * Factory that provides the streaming client(s). There should only be one provider, but it may
 * provide multiple clients if optimizations are disabled - see
 * ENABLE_STREAMING_CLIENT_OPTIMIZATION_CONFIG in the {@link SnowflakeSinkConnectorConfig }
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
      SnowflakeStreamingIngestClient parameterEnabledClient,
      StreamingClientHandler streamingClientHandler) {
    return new StreamingClientProvider(parameterEnabledClient, streamingClientHandler);
  }

  /** ONLY FOR TESTING - private constructor to inject properties for testing */
  private StreamingClientProvider(
      SnowflakeStreamingIngestClient parameterEnabledClient,
      StreamingClientHandler streamingClientHandler) {
    this();
    this.parameterEnabledClient = parameterEnabledClient;
    this.streamingClientHandler = streamingClientHandler;
  }

  private static final KCLogger LOGGER = new KCLogger(StreamingClientProvider.class.getName());
  private SnowflakeStreamingIngestClient parameterEnabledClient;
  private StreamingClientHandler streamingClientHandler;
  private Lock providerLock;

  // private constructor for singleton
  private StreamingClientProvider() {
    this.streamingClientHandler = new StreamingClientHandler();
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
    if (Boolean.parseBoolean(
        connectorConfig.getOrDefault(
            SnowflakeSinkConnectorConfig.ENABLE_STREAMING_CLIENT_OPTIMIZATION_CONFIG,
            Boolean.toString(ENABLE_STREAMING_CLIENT_OPTIMIZATION_DEFAULT)))) {
      LOGGER.info(
          "Streaming client optimization is enabled, returning the existing streaming client if"
              + " valid");
      this.providerLock.lock();
      // recreate streaming client if needed
      if (!StreamingClientHandler.isClientValid(this.parameterEnabledClient)) {
        LOGGER.error("Current streaming client is invalid, recreating client");
        this.parameterEnabledClient = this.streamingClientHandler.createClient(connectorConfig);
      }
      this.providerLock.unlock();
      return this.parameterEnabledClient;
    } else {
      LOGGER.info("Streaming client optimization is disabled, creating a new streaming client");
      return this.streamingClientHandler.createClient(connectorConfig);
    }
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
