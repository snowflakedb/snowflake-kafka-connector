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

import com.snowflake.kafka.connector.Constants.StreamingIngestClientConfigParams;
import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.config.SinkTaskConfig;
import com.snowflake.kafka.connector.internal.KCLogger;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

/**
 * Object to convert and store properties for {@code
 * net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient}. This object is used to compare
 * equality between clients in {@code StreamingClientProvider}.
 */
public class StreamingClientProperties {
  public static final String STREAMING_CLIENT_PREFIX_NAME = "KC_CLIENT_";
  public static final String DEFAULT_CLIENT_NAME = "DEFAULT_CLIENT";

  private static final KCLogger LOGGER = new KCLogger(StreamingClientProperties.class.getName());
  public final Properties clientProperties;
  public final String clientName;
  public final Map<String, Object> parameterOverrides;

  /** Constructor used by {@link #from(SinkTaskConfig)}. */
  private StreamingClientProperties(
      Properties clientProperties, String clientName, Map<String, Object> parameterOverrides) {
    this.clientProperties = clientProperties;
    this.clientName = clientName;
    this.parameterOverrides = parameterOverrides;
  }

  /** Creates streaming client properties from parsed {@link SinkTaskConfig}. */
  public static StreamingClientProperties from(SinkTaskConfig config) {
    Properties clientProperties = new Properties();
    if (config.getSnowflakeUrl() != null) {
      clientProperties.put(StreamingIngestClientConfigParams.ACCOUNT_URL, config.getSnowflakeUrl());
    }
    if (config.getSnowflakeRole() != null) {
      clientProperties.put(StreamingIngestClientConfigParams.ROLE, config.getSnowflakeRole());
    }
    if (config.getSnowflakeUser() != null) {
      clientProperties.put(StreamingIngestClientConfigParams.USER, config.getSnowflakeUser());
    }
    if (config.getSnowflakePrivateKey() != null) {
      clientProperties.put(
          StreamingIngestClientConfigParams.PRIVATE_KEY, config.getSnowflakePrivateKey());
    }

    String clientName =
        STREAMING_CLIENT_PREFIX_NAME
            + (config.getConnectorName() != null ? config.getConnectorName() : DEFAULT_CLIENT_NAME);

    Map<String, Object> parameterOverrides = new HashMap<>();
    String overrideMap = config.getStreamingClientProviderOverrideMap();
    if (overrideMap != null && !overrideMap.isEmpty()) {
      Utils.parseCommaSeparatedKeyValuePairs(overrideMap)
          .forEach((key, value) -> parameterOverrides.put(key.toLowerCase(), value));
      LOGGER.info("Streaming Client config overrides: {}", parameterOverrides);
    }

    return new StreamingClientProperties(clientProperties, clientName, parameterOverrides);
  }

  /**
   * Determines equality between StreamingClientProperties by only looking at the parsed
   * clientProperties. This is used in {@code StreamingClientProvider} to determine equality in
   * registered clients
   *
   * @param other other object to determine equality
   * @return if the given object's clientProperties exists and is equal
   */
  @Override
  public boolean equals(Object other) {
    return other.getClass().equals(StreamingClientProperties.class)
        && ((StreamingClientProperties) other).clientProperties.equals(this.clientProperties)
        && ((StreamingClientProperties) other).parameterOverrides.equals(this.parameterOverrides);
  }

  /**
   * Creates the hashcode for this object from the clientProperties. This is used in {@code
   * StreamingClientProvider} to determine equality in registered clients
   *
   * @return the clientProperties' hashcode
   */
  @Override
  public int hashCode() {
    return Objects.hash(this.clientProperties, this.parameterOverrides);
  }
}
