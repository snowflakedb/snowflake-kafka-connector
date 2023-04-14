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

import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.internal.KCLogger;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClientFactory;
import net.snowflake.ingest.utils.SFException;
import org.apache.kafka.connect.errors.ConnectException;

/** This class handles all calls to manage the streaming ingestion client */
public class StreamingClientHandler {
  private static final KCLogger LOGGER = new KCLogger(StreamingClientHandler.class.getName());
  private static final String STREAMING_CLIENT_PREFIX_NAME = "KC_CLIENT_";
  private static final String TEST_CLIENT_NAME = "TEST_CLIENT";

  private AtomicInteger createdClientId = new AtomicInteger(0);

  /**
   * Checks if a given client is valid (not null, open and has a name)
   *
   * @param client The client to validate
   * @return If the client is valid
   */
  public static boolean isClientValid(SnowflakeStreamingIngestClient client) {
    return client != null && !client.isClosed() && client.getName() != null;
  }

  /**
   * Creates a streaming client from the given config
   *
   * @param connectorConfig The config to create the client
   * @return A newly created client
   */
  public SnowflakeStreamingIngestClient createClient(Map<String, String> connectorConfig) {
    LOGGER.info("Initializing Streaming Client...");

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

      String clientName = this.getNewClientName(connectorConfig);

      SnowflakeStreamingIngestClient createdClient =
          SnowflakeStreamingIngestClientFactory.builder(clientName)
              .setProperties(streamingClientProps)
              .setParameterOverrides(parameterOverrides)
              .build();

      LOGGER.info("Successfully initialized Streaming Client:{}", clientName);

      return createdClient;
    } catch (SFException ex) {
      LOGGER.error("Exception creating streamingIngestClient");
      throw new ConnectException(ex);
    }
  }

  /**
   * Closes the given client. Swallows any exceptions
   *
   * @param client The client to be closed
   */
  public void closeClient(SnowflakeStreamingIngestClient client) {
    LOGGER.info("Closing Streaming Client...");

    // don't do anything if client is already invalid
    if (!isClientValid(client)) {
      LOGGER.info("Streaming Client is already closed");
      return;
    }

    try {
      String clientName = client.getName();
      client.close();
      LOGGER.info("Successfully closed Streaming Client:{}", clientName);
    } catch (Exception e) {
      LOGGER.error(Utils.getExceptionMessage("Failure closing Streaming client", e));
    }
  }

  private String getNewClientName(Map<String, String> connectorConfig) {
    return STREAMING_CLIENT_PREFIX_NAME
        + connectorConfig.getOrDefault(Utils.NAME, TEST_CLIENT_NAME)
        + "_"
        + createdClientId.getAndIncrement();
  }
}
