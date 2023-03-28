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
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClientFactory;
import net.snowflake.ingest.utils.SFException;
import org.apache.kafka.connect.errors.ConnectException;

/** Singleton that provides the streaming client */
public class StreamingClientProvider {
  public static final StreamingClientProvider streamingClientProvider =
      new StreamingClientProvider();

  /**
   * Checks if the client is valid by doing a null check and ensuring it is open
   *
   * @param client The client to validate
   * @return If the client is not null and open
   */
  public static boolean isClientValid(SnowflakeStreamingIngestClient client) {
    return client != null && !client.isClosed();
  }

  /**
   * ONLY FOR TESTING - to get a provider with injected properties
   *
   * @param createdClientId the number of times a client has been created
   * @param connectorConfig the connector config
   * @param client the injected streaming client
   * @return a provider with the injected properties
   */
  @VisibleForTesting
  public static StreamingClientProvider injectStreamingClientProviderForTests(
      int createdClientId,
      Map<String, String> connectorConfig,
      SnowflakeStreamingIngestClient client) {
    return new StreamingClientProvider(createdClientId, connectorConfig, client);
  }

  private static final KCLogger LOGGER = new KCLogger(Utils.class.getName());
  private static final String STREAMING_CLIENT_PREFIX_NAME = "KC_CLIENT_";

  private int createdClientId;
  private Map<String, String> connectorConfig;
  private SnowflakeStreamingIngestClient streamingIngestClient;

  // private constructor for singleton
  private StreamingClientProvider() {
    this.createdClientId = 0;
    this.connectorConfig = new HashMap<>();
  }

  @VisibleForTesting
  // ONLY FOR TESTING - private constructor to inject properties for testing
  private StreamingClientProvider(
      int createdClientId,
      Map<String, String> connectorConfig,
      SnowflakeStreamingIngestClient client) {
    this.createdClientId = createdClientId;
    this.connectorConfig = connectorConfig;
    this.streamingIngestClient = client;
  }

  /**
   * Creates a new streaming client, will replace the existing one if necessary
   *
   * @param connectorConfig The connector config to define the client
   */
  public void createClient(Map<String, String> connectorConfig) {
    // replace previous connector config and client if applicable
    if (!this.connectorConfig.isEmpty()) {
      LOGGER.warn("Overriding previous connector config");
    }

    if (isClientValid(this.streamingIngestClient)) {
      LOGGER.warn(
          "Replacing previous valid streaming client. ClientName:{}",
          this.streamingIngestClient.getName());
      this.closeClient();
    }

    this.connectorConfig = connectorConfig;
    this.streamingIngestClient = this.initStreamingClient(this.connectorConfig);
  }

  /** Closes the current client */
  public void closeClient() {
    // don't do anything if client is already invalid
    if (!isClientValid(this.streamingIngestClient)) {
      return;
    }

    LOGGER.info("Closing Streaming Client:{}", this.streamingIngestClient.getName());
    try {
      this.streamingIngestClient.close();
    } catch (Exception e) {
      // the client should auto close, so don't throw an exception here
      String message =
          e.getMessage() == null || e.getMessage().isEmpty()
              ? "missing exception message"
              : e.getMessage();
      String cause =
          e.getCause() == null || e.getCause().getStackTrace() == null
              ? "missing exception cause"
              : Arrays.toString(e.getCause().getStackTrace());

      LOGGER.error("Failure closing Streaming client msg:{}, cause:{}", message, cause);
    }
  }

  /**
   * Gets the current client or creates a new one from the given connector config
   * If client optimization is not enabled, just create a new streaming client
   *
   * @param connectorConfig The connector config, given as a backup in case the current client is invalid
   * @return The current or newly created streaming client
   */
  public SnowflakeStreamingIngestClient getClient(Map<String, String> connectorConfig) {
    if (Boolean.parseBoolean(connectorConfig.get(SnowflakeSinkConnectorConfig.ENABLE_STREAMING_CLIENT_OPTIMIZATION_CONFIG))) {
      // recreate streaming client if needed
      if (!isClientValid(this.streamingIngestClient)) {
        LOGGER.error("Current streaming client is invalid, recreating client");
        this.createClient(connectorConfig);
      }
      return this.streamingIngestClient;
    } else {
      return this.initStreamingClient(connectorConfig);
    }
  }

  /**
   * Initialize the streaming client
   *
   * @param connectorConfig The connector config required to create the client
   * @return An initialized client
   */
  private SnowflakeStreamingIngestClient initStreamingClient(Map<String, String> connectorConfig) {
    String clientName =
        STREAMING_CLIENT_PREFIX_NAME
            + connectorConfig.getOrDefault(Utils.NAME, "DEFAULT")
            + "_"
            + this.createdClientId;
    LOGGER.info("Initializing Streaming Client... ClientName:{}", clientName);

    // get streaming properties from config
    Properties streamingClientProps = new Properties();
    streamingClientProps.putAll(
        StreamingUtils.convertConfigForStreamingClient(new HashMap<>(connectorConfig)));

    try {
      // Override only if bdec version is explicitly set in config, default to the version set
      // inside Ingest SDK
      Map<String, Object> parameterOverrides = new HashMap<>();
      Optional<String> snowpipeStreamingBdecVersion =
          Optional.ofNullable(connectorConfig.get(SNOWPIPE_STREAMING_FILE_VERSION));
      snowpipeStreamingBdecVersion.ifPresent(
          overriddenValue -> {
            LOGGER.info("Config is overridden for {} ", SNOWPIPE_STREAMING_FILE_VERSION);
            parameterOverrides.put(BLOB_FORMAT_VERSION, overriddenValue);
          });

      SnowflakeStreamingIngestClient createdClient =
          SnowflakeStreamingIngestClientFactory.builder(clientName)
              .setProperties(streamingClientProps)
              .setParameterOverrides(parameterOverrides)
              .build();

      this.createdClientId++;
      LOGGER.info("Successfully initialized Streaming Client. ClientName:{}", clientName);

      return createdClient;
    } catch (SFException ex) {
      LOGGER.error("Exception creating streamingIngestClient with name:{}", clientName);
      throw new ConnectException(ex);
    }
  }
}
