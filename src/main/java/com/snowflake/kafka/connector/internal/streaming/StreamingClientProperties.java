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

import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.SNOWFLAKE_STREAMING_CLIENT_PROVIDER_OVERRIDE_MAP;

import com.snowflake.kafka.connector.Constants.StreamingIngestClientConfigParams;
import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.internal.KCLogger;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Object to convert and store properties for {@code
 * net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient}. This object is used to compare
 * equality between clients in {@code StreamingClientProvider}.
 */
public class StreamingClientProperties {
  public static final String STREAMING_CLIENT_PREFIX_NAME = "KC_CLIENT_";
  public static final String DEFAULT_CLIENT_NAME = "DEFAULT_CLIENT";

  // contains converted config properties that are loggable (not PII data)
  public static final List<String> LOGGABLE_STREAMING_CONFIG_PROPERTIES =
      Stream.of(
              StreamingIngestClientConfigParams.ACCOUNT_URL,
              StreamingIngestClientConfigParams.ROLE,
              StreamingIngestClientConfigParams.USER,
              StreamingUtils.STREAMING_CONSTANT_AUTHORIZATION_TYPE)
          .collect(Collectors.toList());
  private static final KCLogger LOGGER = new KCLogger(StreamingClientProperties.class.getName());
  public final Properties clientProperties;
  public final String clientName;
  public final Map<String, Object> parameterOverrides;

  /**
   * Creates non-null properties, client name and parameter overrides for the {@code
   * net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient} from the given connectorConfig
   * Properties are created by {@link StreamingUtils#convertConfigForStreamingClient(Map)} and are a
   * subset of the given connector configuration
   *
   * @param connectorConfig Given connector configuration. Null configs are treated as an empty map
   */
  public StreamingClientProperties(Map<String, String> connectorConfig) {
    // treat null config as empty config
    if (connectorConfig == null) {
      LOGGER.warn(
          "Creating empty streaming client properties because given connector config was empty");
      connectorConfig = new HashMap<>();
    }

    this.clientProperties = StreamingUtils.convertConfigForStreamingClient(connectorConfig);

    this.clientName =
        STREAMING_CLIENT_PREFIX_NAME
            + connectorConfig.getOrDefault(
                com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.NAME,
                DEFAULT_CLIENT_NAME);

    // Override only if the streaming client properties are explicitly set in config
    this.parameterOverrides = new HashMap<>();
    String overrideMap = connectorConfig.get(SNOWFLAKE_STREAMING_CLIENT_PROVIDER_OVERRIDE_MAP);
    if (overrideMap != null && !overrideMap.isEmpty()) {
      Utils.parseCommaSeparatedKeyValuePairs(overrideMap)
          .forEach((key, value) -> this.parameterOverrides.put(key.toLowerCase(), value));
      LOGGER.info("Streaming Client config overrides: {}", this.parameterOverrides);
    }
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
