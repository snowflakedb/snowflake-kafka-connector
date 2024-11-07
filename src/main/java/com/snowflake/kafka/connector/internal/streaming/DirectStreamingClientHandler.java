/*
 * Copyright (c) 2024 Snowflake Inc. All rights reserved.
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
import java.util.concurrent.atomic.AtomicInteger;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClientFactory;
import net.snowflake.ingest.utils.SFException;
import org.apache.kafka.connect.errors.ConnectException;

/** This class handles all calls to manage the streaming ingestion client */
public class DirectStreamingClientHandler implements StreamingClientHandler {
  private static final KCLogger LOGGER = new KCLogger(DirectStreamingClientHandler.class.getName());
  private final AtomicInteger createdClientId = new AtomicInteger(0);

  /**
   * Creates a streaming client from the given properties
   *
   * @param streamingClientProperties The properties to create the client
   * @return A newly created client
   */
  @Override
  public SnowflakeStreamingIngestClient createClient(
      StreamingClientProperties streamingClientProperties) {
    LOGGER.info("Initializing Streaming Client...");

    try {
      SnowflakeStreamingIngestClientFactory.Builder builder =
          SnowflakeStreamingIngestClientFactory.builder(
                  streamingClientProperties.clientName + "_" + createdClientId.getAndIncrement())
              .setProperties(streamingClientProperties.clientProperties)
              .setParameterOverrides(streamingClientProperties.parameterOverrides);

      SnowflakeStreamingIngestClient createdClient = builder.build();

      LOGGER.info(
          "Successfully initialized Streaming Client:{} with properties {}",
          streamingClientProperties.clientName,
          streamingClientProperties.getLoggableClientProperties());

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
  @Override
  public void closeClient(SnowflakeStreamingIngestClient client) {
    LOGGER.info("Closing Streaming Client...");

    // don't do anything if client is already invalid
    if (!StreamingClientHandler.isClientValid(client)) {
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
