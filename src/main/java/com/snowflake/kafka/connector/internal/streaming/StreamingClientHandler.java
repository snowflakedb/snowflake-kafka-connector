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
import java.util.Arrays;
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
  private static AtomicInteger createdClientId = new AtomicInteger(0);

  public static SnowflakeStreamingIngestClient createClient(Map<String, String> connectorConfig) {
    String clientName = getClientName(connectorConfig);
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

      createdClientId.incrementAndGet();
      LOGGER.info("Successfully initialized Streaming Client. ClientName:{}", clientName);

      return createdClient;
    } catch (SFException ex) {
      LOGGER.error("Exception creating streamingIngestClient with name:{}", clientName);
      throw new ConnectException(ex);
    }
  }

  public static void closeClient(SnowflakeStreamingIngestClient client) {
    // don't do anything if client is already invalid
    if (!isClientValid(client)) {
      return;
    }

    LOGGER.info("Closing Streaming Client:{}", client.getName());
    try {
      client.close();
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

  public static String getClientName(Map<String, String> connectorConfig) {
    return STREAMING_CLIENT_PREFIX_NAME
        + connectorConfig.getOrDefault(Utils.NAME, "DEFAULT")
        + "_"
        + createdClientId.get();
  }

  public static boolean isClientValid(SnowflakeStreamingIngestClient client) {
    return client != null && !client.isClosed() && client.getName() != null;
  }
}
