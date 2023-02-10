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
package com.snowflake.kafka.connector.internal.ingestsdk;

import com.google.common.annotations.VisibleForTesting;
import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.internal.LoggerHandler;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import net.snowflake.ingest.streaming.OpenChannelRequest;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClientFactory;
import net.snowflake.ingest.utils.SFException;
import org.apache.kafka.connect.errors.ConnectException;

/** This is a wrapper to help define a contract with the streaming ingest clients */
public class KcStreamingIngestClient {
  private static final String STREAMING_CLIENT_PREFIX_NAME = "KC_CLIENT";
  private LoggerHandler LOGGER = new LoggerHandler(this.getClass().getName());

  private final SnowflakeStreamingIngestClient client;

  /**
   * Builds the client's name based on the kc instance and expected client id
   *
   * @param kcInstanceId the kafka connector instance id
   * @param clientId the client id
   * @return the client's name as 'KC_CLIENT_kcInstanceId_clientId'
   */
  public static String buildStreamingIngestClientName(String kcInstanceId, int clientId) {
    return Utils.formatString("{}_{}_{}", STREAMING_CLIENT_PREFIX_NAME, kcInstanceId, clientId);
  }

  // TESTING ONLY - inject the client
  @VisibleForTesting
  public KcStreamingIngestClient(SnowflakeStreamingIngestClient client) {
    this.client = client;
  }

  /**
   * Creates a streaming client from the given properties and requested name. Validates the
   * requested client is not null and has the correct name
   *
   * <p>Any exceptions will be passed up, a sfexception will be converted to a connectexception
   *
   * @param streamingClientProps the properties for the client
   * @param clientName the client name to uniquely identify the client
   */
  protected KcStreamingIngestClient(Properties streamingClientProps, String clientName) {
    try {
      LOGGER.info("Creating Streaming Client: {}", clientName);
      this.client =
          SnowflakeStreamingIngestClientFactory.builder(clientName)
              .setProperties(streamingClientProps)
              .build();

      assert this.client != null; // client is final, so never need to do another null check
      assert this.client.getName().equals(clientName);
    } catch (SFException ex) {
      throw new ConnectException(ex);
    }
  }

  /**
   * Creates an ingest sdk OpenChannelRequest and opens the client's channel
   *
   * <p>No exception handling done, all exceptions will be passed through
   *
   * @param channelName the name of the channel to open
   * @param config config to get the database and schema names for the channel
   * @param tableName table name of the channel
   * @return the opened channel
   */
  public SnowflakeStreamingIngestChannel openChannel(
      String channelName, Map<String, String> config, String tableName) {
    OpenChannelRequest channelRequest =
        OpenChannelRequest.builder(channelName)
            .setDBName(config.get(Utils.SF_DATABASE))
            .setSchemaName(config.get(Utils.SF_SCHEMA))
            .setTableName(tableName)
            .setOnErrorOption(OpenChannelRequest.OnErrorOption.CONTINUE)
            .build();
    LOGGER.info("Opening a channel with name:{} for table name:{}", channelName, tableName);

    return this.client.openChannel(channelRequest);
  }

  /**
   * Calls the ingest sdk to close the client sdk
   *
   * <p>Swallows all exceptions and returns t/f if the client was closed because closing is best
   * effort
   *
   * @return if the client was successfully closed
   */
  public boolean close() {
    if (this.client.isClosed()) {
      LOGGER.debug("Streaming client is already closed");
      return true;
    }

    LOGGER.info("Closing Streaming Client:{}", this.client.getName());

    try {
      this.client.close();
      return true;
    } catch (Exception e) {
      String message =
          e.getMessage() != null && !e.getMessage().isEmpty()
              ? e.getMessage()
              : "no error message provided";

      String cause =
          e.getCause() != null
                  && e.getCause().getStackTrace() != null
                  && !Arrays.toString(e.getCause().getStackTrace()).isEmpty()
              ? Arrays.toString(e.getCause().getStackTrace())
              : "no cause provided";

      // don't throw an exception because closing the client is best effort
      // the actual close, not in the catch or finally
      LOGGER.error("Failure closing Streaming client msg:{}, cause:{}", message, cause);
      return false;
    }
  }

  /**
   * Checks if the current client is closed
   *
   * @return if the client is closed
   */
  public boolean isClosed() {
    return this.client.isClosed();
  }

  /**
   * Returns the clients name. We treat this as the id
   *
   * @return the clients name
   */
  public String getName() {
    return this.client.getName();
  }

  /**
   * Equality between clients is verified by the client name and the state (if it is closed or not)
   *
   * @param o Other object to check equality
   * @return If the given object is the same
   */
  @Override
  public boolean equals(Object o) {
    if (!(o instanceof KcStreamingIngestClient)) {
      return false;
    }

    KcStreamingIngestClient otherClient = (KcStreamingIngestClient) o;
    return otherClient.getName().equals(this.getName())
        && otherClient.isClosed() == this.isClosed();
  }
}
