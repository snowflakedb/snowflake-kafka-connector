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

import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.SNOWPIPE_STREAMING_CLIENT_PROVIDER_OVERRIDE_MAP;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.SNOWPIPE_STREAMING_MAX_CLIENT_LAG;
import static net.snowflake.ingest.utils.ParameterProvider.MAX_CLIENT_LAG;

import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.internal.KCLogger;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import net.snowflake.ingest.utils.Constants;

/**
 * Object to convert and store properties for {@link
 * net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient}. This object is used to compare
 * equality between clients in {@link StreamingClientProvider}.
 */
public class StreamingClientProperties {
  public static final String STREAMING_CLIENT_PREFIX_NAME = "KC_CLIENT_";
  public static final String DEFAULT_CLIENT_NAME = "DEFAULT_CLIENT";

  private static final KCLogger LOGGER = new KCLogger(StreamingClientProperties.class.getName());

  // contains converted config properties that are loggable (not PII data)
  public static final List<String> LOGGABLE_STREAMING_CONFIG_PROPERTIES =
      Stream.of(
              Constants.ACCOUNT_URL,
              Constants.ROLE,
              Constants.USER,
              StreamingUtils.STREAMING_CONSTANT_AUTHORIZATION_TYPE)
          .collect(Collectors.toList());

  public final Properties clientProperties;
  public final String clientName;
  public final Map<String, Object> parameterOverrides;

  /**
   * Creates non-null properties, client name and parameter overrides for the {@link
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
            + connectorConfig.getOrDefault(Utils.NAME, DEFAULT_CLIENT_NAME);

    // Override only if the max client lag is explicitly set in config
    this.parameterOverrides = new HashMap<>();
    Optional<String> snowpipeStreamingMaxClientLag =
        Optional.ofNullable(connectorConfig.get(SNOWPIPE_STREAMING_MAX_CLIENT_LAG));
    snowpipeStreamingMaxClientLag.ifPresent(
        overriddenValue -> {
          parameterOverrides.put(MAX_CLIENT_LAG, String.format("%s second", overriddenValue));
        });
    combineStreamingClientOverriddenProperties(connectorConfig);
  }

  /**
   * Fetches the properties from {@link SNOWPIPE_STREAMING_CLIENT_PARAMETER_OVERRIDE_MAP} and
   * combines it with parameter provider. This parameter provider needs a lowercase String in its
   * key since Ingest SDK fetches the key from <a
   * href="https://github.com/snowflakedb/snowflake-ingest-java/blob/master/src/main/java/net/snowflake/ingest/utils/ParameterProvider.java">Ingest
   * SDK</a>
   *
   * <p>MAX_CLIENT_LAG can be provided in SNOWPIPE_STREAMING_CLIENT_PARAMETER_OVERRIDE_MAP or in
   * SNOWPIPE_STREAMING_MAX_CLIENT_LAG.
   *
   * <p>We will honor the value provided in SNOWPIPE_STREAMING_MAX_CLIENT_LAG
   *
   * <p>Example, if two configs are provided and map has MAX_CLIENT_LAG, Value from
   * snowflake.streaming.max.client.lag will be honored.
   *
   * <ol>
   *   <li>snowflake.streaming.client.provider.override.map =
   *       MAX_CLIENT_LAG:30,MAX_CHANNEL_SIZE_IN_BYTES:10000000
   *   <li>snowflake.streaming.max.client.lag = 60
   *   <li>MAX_CLIENT_LAG will honor 60 seconds.
   * </ol>
   *
   * If <b>snowflake.streaming.max.client.lag</b> is not provided, we pass in the map as is to
   * Streaming Client (all lowercase keys).
   *
   * <p>Please note, Streaming Client
   *
   * @param connectorConfig Input connector config
   */
  private void combineStreamingClientOverriddenProperties(Map<String, String> connectorConfig) {
    // MAX_CLIENT_LAG property can also be present in override map, in that case, we will honor the
    // value provided in SNOWPIPE_STREAMING_MAX_CLIENT_LAG since it preceded and was released in
    // earlier versions.
    Optional<String> clientOverrideProperties =
        Optional.ofNullable(connectorConfig.get(SNOWPIPE_STREAMING_CLIENT_PROVIDER_OVERRIDE_MAP));
    Map<String, String> clientOverridePropertiesMap = new HashMap<>();
    clientOverrideProperties.ifPresent(
        overriddenKeyValuePairs -> {
          LOGGER.info(
              "Overridden map provided for streaming client properties: {}",
              overriddenKeyValuePairs);
          Map<String, String> overriddenPairs =
              Utils.parseCommaSeparatedKeyValuePairs(overriddenKeyValuePairs);
          overriddenPairs.forEach(
              (overriddenKey, overriddenValue) -> {
                if (overriddenKey.equalsIgnoreCase(MAX_CLIENT_LAG)) {
                  clientOverridePropertiesMap.put(
                      MAX_CLIENT_LAG, String.format("%s second", overriddenValue));
                } else {
                  clientOverridePropertiesMap.put(overriddenKey.toLowerCase(), overriddenValue);
                }
              });
          if (clientOverridePropertiesMap.containsKey(MAX_CLIENT_LAG)
              && parameterOverrides.containsKey(MAX_CLIENT_LAG)) {
            LOGGER.info(
                "Honoring {} value in MAX_CLIENT_LAG for streaming client provider, since it is"
                    + " explicitly provided. Using: {}",
                SNOWPIPE_STREAMING_MAX_CLIENT_LAG,
                parameterOverrides.get(MAX_CLIENT_LAG));
            clientOverridePropertiesMap.remove(MAX_CLIENT_LAG);
          }
          parameterOverrides.putAll(clientOverridePropertiesMap);
        });
    parameterOverrides.forEach(
        (key, value) -> LOGGER.info("Streaming Client Config is overridden for {}={}", key, value));
  }

  /**
   * Gets the loggable properties, see {@link
   * StreamingClientProperties#LOGGABLE_STREAMING_CONFIG_PROPERTIES}
   *
   * @return A formatted string with the loggable properties
   */
  public String getLoggableClientProperties() {
    return this.clientProperties == null | this.clientProperties.isEmpty()
        ? ""
        : this.clientProperties.entrySet().stream()
            .filter(
                propKvp ->
                    LOGGABLE_STREAMING_CONFIG_PROPERTIES.stream()
                        .anyMatch(propKvp.getKey().toString()::equalsIgnoreCase))
            .collect(Collectors.toList())
            .toString();
  }

  /**
   * Determines equality between StreamingClientProperties by only looking at the parsed
   * clientProperties. This is used in {@link StreamingClientProvider} to determine equality in
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
   * Creates the hashcode for this object from the clientProperties. This is used in {@link
   * StreamingClientProvider} to determine equality in registered clients
   *
   * @return the clientProperties' hashcode
   */
  @Override
  public int hashCode() {
    return Objects.hash(this.clientProperties, this.parameterOverrides);
  }
}
