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

import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.internal.KCLogger;
import com.snowflake.kafka.connector.internal.streaming.StreamingClientProvider.StreamingClientProperties;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClientFactory;
import net.snowflake.ingest.utils.Constants;
import net.snowflake.ingest.utils.SFException;
import org.apache.kafka.connect.errors.ConnectException;

/** This class handles all calls to manage the streaming ingestion client */
public class StreamingClientHandler {
  private static final KCLogger LOGGER = new KCLogger(StreamingClientHandler.class.getName());

  // contains config properties that are loggable (not PII data)
  public static final List<String> LOGGABLE_STREAMING_CONFIG_PROPERTIES =
      Stream.of(
              Constants.ACCOUNT_URL,
              Constants.ROLE,
              Constants.USER,
              StreamingUtils.STREAMING_CONSTANT_AUTHORIZATION_TYPE)
          .collect(Collectors.toList());

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
   * Gets the loggable properties (see {@link
   * StreamingClientHandler#LOGGABLE_STREAMING_CONFIG_PROPERTIES} passed into the client.
   *
   * @param properties The client properties
   * @return A string with the loggable properties
   */
  public static String getLoggableClientProperties(Properties properties) {
    return properties == null
        ? ""
        : properties.entrySet().stream()
            .filter(
                propKvp ->
                    LOGGABLE_STREAMING_CONFIG_PROPERTIES.stream()
                        .anyMatch(propKvp.getKey().toString()::equalsIgnoreCase))
            .collect(Collectors.toList())
            .toString();
  }

  /**
   * Creates a streaming client from the given properties
   *
   * @param streamingClientProperties The properties to create the client
   * @return A newly created client
   */
  public SnowflakeStreamingIngestClient createClient(
      StreamingClientProperties streamingClientProperties) {
    LOGGER.info("Initializing Streaming Client...");

    try {
      StreamingClientProvider.createdClientId.getAndIncrement();

      SnowflakeStreamingIngestClient createdClient =
          SnowflakeStreamingIngestClientFactory.builder(streamingClientProperties.getClientName())
              .setProperties(streamingClientProperties.clientProperties)
              .setParameterOverrides(streamingClientProperties.parameterOverrides)
              .build();

      LOGGER.info(
          "Successfully initialized Streaming Client:{} with properties {}",
          streamingClientProperties.clientName,
          getLoggableClientProperties(streamingClientProperties.clientProperties));

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
}
