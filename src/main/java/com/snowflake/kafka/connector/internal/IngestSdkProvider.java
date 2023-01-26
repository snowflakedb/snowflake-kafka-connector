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

package com.snowflake.kafka.connector.internal;

import com.snowflake.kafka.connector.internal.streaming.StreamingUtils;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClientFactory;
import net.snowflake.ingest.utils.SFException;
import org.apache.kafka.connect.errors.ConnectException;

public class IngestSdkProvider {
  private static final String STREAMING_CLIENT_PREFIX_NAME = "KC_CLIENT_";

  private LoggerHandler LOGGER;
  private int streamingIngestClientCount;
  private SnowflakeStreamingIngestClient streamingIngestClient;

  /**
   * This is a wrapper for the ingest sdk to help define a contract between KC and Ingest
   *
   * <p>Ideally all ingest sdk calls will go through this to make mocking and integration tests
   * easier to write (potentially also to run most of the integration tests without needing a
   * snowflake connection). Currently it only manages the client.
   */
  public IngestSdkProvider() {
    LOGGER = new LoggerHandler(this.getClass().getName());
    this.streamingIngestClientCount = 0;
  }

  public SnowflakeStreamingIngestClient createStreamingClient(
      Map<String, String> connectorConfig, String connectorName) {
    Map<String, String> streamingPropertiesMap =
        StreamingUtils.convertConfigForStreamingClient(new HashMap<>(connectorConfig));
    Properties streamingClientProps = new Properties();
    streamingClientProps.putAll(streamingPropertiesMap);

    String streamingIngestClientName = this.getStreamingIngestClientName(connectorName);

    try {
      LOGGER.info("Creating Streaming Client. ClientName:{}", streamingIngestClientName);
      this.streamingIngestClientCount++;
      this.streamingIngestClient =
          SnowflakeStreamingIngestClientFactory.builder(streamingIngestClientName)
              .setProperties(streamingClientProps)
              .build();
      return this.getStreamingIngestClient();
    } catch (SFException ex) {
      // note: unable to test this exception since the factory is not mockable. be careful changing
      // logic here
      LOGGER.error(
          "Exception creating streamingIngestClient with name:{}", streamingIngestClientName);
      throw new ConnectException(ex);
    }
  }

  public void closeStreamingClient() {
    String streamingIngestClientName = this.streamingIngestClient.getName();
    LOGGER.info("Closing Streaming Client:{}", streamingIngestClientName);
    try {
      this.streamingIngestClient.close();
    } catch (Exception e) {
      // note: unable to test this exception since the factory is not mockable. be careful changing
      // logic here
      LOGGER.error(
          "Failure closing Streaming client msg:{}, cause:{}",
          e.getMessage(),
          Arrays.toString(e.getCause().getStackTrace()));
    }
  }

  public SnowflakeStreamingIngestClient getStreamingIngestClient() {
    if (this.streamingIngestClient != null && !this.streamingIngestClient.isClosed()) {
      return this.streamingIngestClient;
    }

    LOGGER.error("Streaming ingest client was null or closed. It must be initialized");
    throw SnowflakeErrors.ERROR_3009.getException();
  }

  private String getStreamingIngestClientName(String connectorName) {
    return STREAMING_CLIENT_PREFIX_NAME + connectorName + this.streamingIngestClientCount;
  }
}
