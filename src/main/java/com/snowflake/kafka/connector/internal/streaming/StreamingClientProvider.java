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

import com.google.common.annotations.VisibleForTesting;
import com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig;
import com.snowflake.kafka.connector.internal.KCLogger;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;

/**
 * Singleton that provides the streaming client(s). There should only be one provider, but it may
 * provide multiple clients
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
  public static StreamingClientProvider injectStreamingClientProviderForTests(
      SnowflakeStreamingIngestClient parameterEnabledClient,
      StreamingClientHandler streamingClientHandler) {
    return new StreamingClientProvider(parameterEnabledClient, streamingClientHandler);
  }

  private static final KCLogger LOGGER = new KCLogger(StreamingClientProvider.class.getName());
  private SnowflakeStreamingIngestClient parameterEnabledClient;
  private StreamingClientHandler streamingClientHandler;
  private ReentrantLock providerLock;

  // private constructor for singleton
  private StreamingClientProvider() {
    this.streamingClientHandler = new StreamingClientHandler();
    providerLock = new ReentrantLock();
  }

  // ONLY FOR TESTING - private constructor to inject properties for testing
  @VisibleForTesting
  private StreamingClientProvider(
      SnowflakeStreamingIngestClient parameterEnabledClient,
      StreamingClientHandler streamingClientHandler) {
    this();
    this.parameterEnabledClient = parameterEnabledClient;
    this.streamingClientHandler = streamingClientHandler;
  }

  /**
   * Gets the current client or creates a new one from the given connector config If client
   * optimization is not enabled, it will create a new streaming client and the caller is
   * responsible for closing it
   *
   * @param connectorConfig The connector config
   * @return A streaming client
   */
  public SnowflakeStreamingIngestClient getClient(Map<String, String> connectorConfig) {
    if (Boolean.parseBoolean(
        connectorConfig.get(
            SnowflakeSinkConnectorConfig.ENABLE_STREAMING_CLIENT_OPTIMIZATION_CONFIG))) {
      this.providerLock.lock();
      // recreate streaming client if needed
      if (!StreamingClientHandler.isClientValid(this.parameterEnabledClient)) {
        LOGGER.error("Current streaming client is invalid, recreating client");
        this.parameterEnabledClient = this.streamingClientHandler.createClient(connectorConfig);
      }

      this.providerLock.unlock();
      return this.parameterEnabledClient;
    } else {
      return this.streamingClientHandler.createClient(connectorConfig);
    }
  }

  /** Closes the current client */
  public void closeClient(SnowflakeStreamingIngestClient client) {
    this.streamingClientHandler.closeClient(client);
  }
}
