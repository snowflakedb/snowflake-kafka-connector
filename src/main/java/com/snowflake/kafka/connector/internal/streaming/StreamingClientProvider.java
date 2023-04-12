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

import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.SNOWPIPE_STREAMING_FILE_VERSION;
import static net.snowflake.ingest.utils.ParameterProvider.BLOB_FORMAT_VERSION;

import com.google.common.annotations.VisibleForTesting;
import com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig;
import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.internal.KCLogger;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Stream;

import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClientFactory;
import net.snowflake.ingest.utils.SFException;
import org.apache.kafka.connect.errors.ConnectException;

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

  /**
   * ONLY FOR TESTING - to get a provider with injected properties
   */
  @VisibleForTesting
  public static StreamingClientProvider injectStreamingClientProviderForTests(
          ConcurrentMap<String, SnowflakeStreamingIngestClient> clients) {
    return new StreamingClientProvider(clients);
  }

  private static final KCLogger LOGGER = new KCLogger(StreamingClientProvider.class.getName());
  private ConcurrentMap<String, SnowflakeStreamingIngestClient> streamingIngestClients;
  private SnowflakeStreamingIngestClient parameterEnabledClient;

  // private constructor for singleton
  private StreamingClientProvider() {
    this.streamingIngestClients = new ConcurrentHashMap<>();
  }

  // ONLY FOR TESTING - private constructor to inject properties for testing
  @VisibleForTesting
  private StreamingClientProvider(ConcurrentMap<String, SnowflakeStreamingIngestClient> streamingIngestClients) {
    this.streamingIngestClients = streamingIngestClients;
  }

  /**
   * Gets the current client or creates a new one from the given connector config. If client
   * optimization is not enabled, it will create a new streaming client
   *
   * @param connectorConfig The connector config
   * @return A streaming client
   */
  public SnowflakeStreamingIngestClient getClient(Map<String, String> connectorConfig) {
    if (Boolean.parseBoolean(
            connectorConfig.get(
                    SnowflakeSinkConnectorConfig.ENABLE_STREAMING_CLIENT_OPTIMIZATION_CONFIG))) {
      // recreate streaming client if needed
      if (!StreamingClientHandler.isClientValid(this.parameterEnabledClient)) {
        LOGGER.error("Current streaming client is invalid, recreating client");
        this.parameterEnabledClient = StreamingClientHandler.createClient(connectorConfig);
      }

      return this.parameterEnabledClient;
    } else {
      String taskId = connectorConfig.getOrDefault(Utils.TASK_ID, "-1");
      SnowflakeStreamingIngestClient existingClient = this.streamingIngestClients.get(taskId);

      if (StreamingClientHandler.isClientValid(existingClient)) {
        return this.streamingIngestClients.get(taskId);
      }

      SnowflakeStreamingIngestClient newClient = StreamingClientHandler.createClient(connectorConfig);
      this.streamingIngestClients.put(taskId, newClient);
      return newClient;
    }
  }

  /** Closes the current client */
  public void closeAllClients() {
    StreamingClientHandler.closeClient(this.parameterEnabledClient);

    this.streamingIngestClients.forEach((taskId, client) -> StreamingClientHandler.closeClient(client));
  }
}
