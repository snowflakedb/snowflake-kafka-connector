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

import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.BUFFER_SIZE_BYTES;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.SNOWPIPE_STREAMING_CLIENT_PROVIDER_OVERRIDE_MAP;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.SNOWPIPE_STREAMING_ENABLE_SINGLE_BUFFER;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.SNOWPIPE_STREAMING_MAX_CLIENT_LAG;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.SNOWPIPE_STREAMING_MAX_MEMORY_LIMIT_IN_BYTES;
import static com.snowflake.kafka.connector.internal.streaming.StreamingClientProperties.DEFAULT_CLIENT_NAME;
import static com.snowflake.kafka.connector.internal.streaming.StreamingClientProperties.LOGGABLE_STREAMING_CONFIG_PROPERTIES;
import static com.snowflake.kafka.connector.internal.streaming.StreamingClientProperties.STREAMING_CLIENT_PREFIX_NAME;
import static net.snowflake.ingest.utils.ParameterProvider.*;
import static org.assertj.core.api.Assertions.assertThat;

import com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig;
import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.config.SnowflakeSinkConnectorConfigBuilder;
import com.snowflake.kafka.connector.internal.SnowflakeKafkaConnectorException;
import com.snowflake.kafka.connector.internal.TestUtils;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.junit.Assert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

public class StreamingClientPropertiesTest {

  @Test
  public void testGetValidProperties() {
    String overrideValue = "10";

    // setup config with all loggable properties and parameter override
    Map<String, String> connectorConfig = TestUtils.getConfForStreaming();
    connectorConfig.put(Utils.NAME, "testName");
    connectorConfig.put(Utils.SF_URL, "testUrl");
    connectorConfig.put(Utils.SF_ROLE, "testRole");
    connectorConfig.put(Utils.SF_USER, "testUser");
    connectorConfig.put(Utils.SF_AUTHENTICATOR, Utils.SNOWFLAKE_JWT);
    connectorConfig.put(SNOWPIPE_STREAMING_MAX_CLIENT_LAG, overrideValue);

    Properties expectedProps = StreamingUtils.convertConfigForStreamingClient(connectorConfig);
    String expectedClientName = STREAMING_CLIENT_PREFIX_NAME + connectorConfig.get(Utils.NAME);
    Map<String, String> expectedParameterOverrides = new HashMap<>();
    expectedParameterOverrides.put(MAX_CLIENT_LAG, String.format("%s second", overrideValue));

    // test get properties
    StreamingClientProperties resultProperties = new StreamingClientProperties(connectorConfig);

    // verify
    assert resultProperties.clientProperties.equals(expectedProps);
    assert resultProperties.clientName.equals(expectedClientName);
    assert resultProperties.parameterOverrides.equals(expectedParameterOverrides);

    // verify only loggable props are returned
    String loggableProps = resultProperties.getLoggableClientProperties();
    for (Object key : expectedProps.keySet()) {
      Object value = expectedProps.get(key);
      if (LOGGABLE_STREAMING_CONFIG_PROPERTIES.stream()
          .anyMatch(key.toString()::equalsIgnoreCase)) {
        assert loggableProps.contains(
            Utils.formatString("{}={}", key.toString(), value.toString()));
      } else {
        assert !loggableProps.contains(key.toString()) && !loggableProps.contains(value.toString());
      }
    }
  }

  @Test
  void shouldNotPropagateStreamingClientProperties_SingleBufferDisabled() {
    // GIVEN
    Map<String, String> connectorConfig =
        SnowflakeSinkConnectorConfigBuilder.streamingConfig()
            .withSingleBufferEnabled(false)
            .build();

    connectorConfig.put(BUFFER_SIZE_BYTES, "10000000");
    connectorConfig.put(SNOWPIPE_STREAMING_MAX_MEMORY_LIMIT_IN_BYTES, "20000000");

    // WHEN
    StreamingClientProperties resultProperties = new StreamingClientProperties(connectorConfig);

    // THEN
    assertThat(resultProperties.parameterOverrides).isEmpty();
  }

  @ParameterizedTest
  @CsvSource({"true", "false"})
  void shouldPropagateStreamingClientPropertiesFromOverrideMap(String singleBufferEnabled) {
    // GIVEN
    Map<String, String> connectorConfig =
        SnowflakeSinkConnectorConfigBuilder.streamingConfig()
            .withSingleBufferEnabled(Boolean.parseBoolean(singleBufferEnabled))
            .build();

    connectorConfig.remove(BUFFER_SIZE_BYTES);
    connectorConfig.put(
        SNOWPIPE_STREAMING_CLIENT_PROVIDER_OVERRIDE_MAP,
        "MAX_CHANNEL_SIZE_IN_BYTES:1,MAX_MEMORY_LIMIT_IN_BYTES:2");
    connectorConfig.put(SNOWPIPE_STREAMING_ENABLE_SINGLE_BUFFER, singleBufferEnabled);

    Map<String, String> expectedParameterOverrides = new HashMap<>();
    expectedParameterOverrides.put(MAX_CHANNEL_SIZE_IN_BYTES, "1");
    expectedParameterOverrides.put(MAX_MEMORY_LIMIT_IN_BYTES, "2");

    // WHEN
    StreamingClientProperties resultProperties = new StreamingClientProperties(connectorConfig);

    // THEN
    assertThat(resultProperties.parameterOverrides).isEqualTo(expectedParameterOverrides);
  }

  @Test
  void shouldPropagateStreamingClientProperties_SingleBufferEnabled() {
    // GIVEN
    Map<String, String> connectorConfig =
        SnowflakeSinkConnectorConfigBuilder.streamingConfig().withSingleBufferEnabled(true).build();

    connectorConfig.put(BUFFER_SIZE_BYTES, "10000000");
    connectorConfig.put(SNOWPIPE_STREAMING_MAX_MEMORY_LIMIT_IN_BYTES, "20000000");

    Map<String, String> expectedParameterOverrides = new HashMap<>();
    expectedParameterOverrides.put(MAX_CHANNEL_SIZE_IN_BYTES, "10000000");
    expectedParameterOverrides.put(MAX_MEMORY_LIMIT_IN_BYTES, "20000000");

    // WHEN
    StreamingClientProperties resultProperties = new StreamingClientProperties(connectorConfig);

    // THEN
    assertThat(resultProperties.parameterOverrides).isEqualTo(expectedParameterOverrides);
  }

  @Test
  void explicitStreamingClientPropertiesTakePrecedenceOverOverrideMap_SingleBufferEnabled() {
    // GIVEN
    Map<String, String> connectorConfig =
        SnowflakeSinkConnectorConfigBuilder.streamingConfig().withSingleBufferEnabled(true).build();

    connectorConfig.put(BUFFER_SIZE_BYTES, "10000000");
    connectorConfig.put(SNOWPIPE_STREAMING_MAX_MEMORY_LIMIT_IN_BYTES, "20000000");
    connectorConfig.put(
        SNOWPIPE_STREAMING_CLIENT_PROVIDER_OVERRIDE_MAP,
        "MAX_CHANNEL_SIZE_IN_BYTES:1,MAX_MEMORY_LIMIT_IN_BYTES:2");

    Map<String, String> expectedParameterOverrides = new HashMap<>();
    expectedParameterOverrides.put(MAX_CHANNEL_SIZE_IN_BYTES, "10000000");
    expectedParameterOverrides.put(MAX_MEMORY_LIMIT_IN_BYTES, "20000000");

    // WHEN
    StreamingClientProperties resultProperties = new StreamingClientProperties(connectorConfig);

    // THEN
    assertThat(resultProperties.parameterOverrides).isEqualTo(expectedParameterOverrides);
  }

  @Test
  void
      explicitStreamingClientPropertiesShouldNOTTakePrecedenceOverOverrideMap_SingleBufferDisabled() {
    // GIVEN
    Map<String, String> connectorConfig =
        SnowflakeSinkConnectorConfigBuilder.streamingConfig()
            .withSingleBufferEnabled(false)
            .build();

    connectorConfig.put(BUFFER_SIZE_BYTES, "10000000");
    connectorConfig.put(SNOWPIPE_STREAMING_MAX_MEMORY_LIMIT_IN_BYTES, "20000000");
    connectorConfig.put(
        SNOWPIPE_STREAMING_CLIENT_PROVIDER_OVERRIDE_MAP,
        "MAX_CHANNEL_SIZE_IN_BYTES:1,MAX_MEMORY_LIMIT_IN_BYTES:2");

    Map<String, String> expectedParameterOverrides = new HashMap<>();
    expectedParameterOverrides.put(MAX_CHANNEL_SIZE_IN_BYTES, "1");
    expectedParameterOverrides.put(MAX_MEMORY_LIMIT_IN_BYTES, "2");

    // WHEN
    StreamingClientProperties resultProperties = new StreamingClientProperties(connectorConfig);

    // THEN
    assertThat(resultProperties.parameterOverrides).isEqualTo(expectedParameterOverrides);
  }

  @Test
  public void testValidPropertiesWithOverriddenStreamingPropertiesMap_withoutMaxClientLagInMap() {
    String overrideValue = "10";

    Map<String, String> connectorConfig = TestUtils.getConfForStreaming();
    connectorConfig.put(Utils.NAME, "testName");
    connectorConfig.put(Utils.SF_URL, "testUrl");
    connectorConfig.put(Utils.SF_ROLE, "testRole");
    connectorConfig.put(Utils.SF_USER, "testUser");
    connectorConfig.put(Utils.SF_AUTHENTICATOR, Utils.SNOWFLAKE_JWT);
    connectorConfig.put(SNOWPIPE_STREAMING_MAX_CLIENT_LAG, overrideValue);
    connectorConfig.put(
        SNOWPIPE_STREAMING_CLIENT_PROVIDER_OVERRIDE_MAP,
        "MAX_CHANNEL_SIZE_IN_BYTES:10000000,ENABLE_SNOWPIPE_STREAMING_JMX_METRICS:false");

    Properties expectedProps = StreamingUtils.convertConfigForStreamingClient(connectorConfig);
    String expectedClientName = STREAMING_CLIENT_PREFIX_NAME + connectorConfig.get(Utils.NAME);
    Map<String, String> expectedParameterOverrides = new HashMap<>();
    expectedParameterOverrides.put(MAX_CLIENT_LAG, String.format("%s second", overrideValue));
    expectedParameterOverrides.put(MAX_CHANNEL_SIZE_IN_BYTES, "10000000");
    expectedParameterOverrides.put(ENABLE_SNOWPIPE_STREAMING_METRICS, "false");

    // test get properties
    StreamingClientProperties resultProperties = new StreamingClientProperties(connectorConfig);

    // verify
    assert resultProperties.clientProperties.equals(expectedProps);
    assert resultProperties.clientName.equals(expectedClientName);
    assert resultProperties.parameterOverrides.equals(expectedParameterOverrides);
  }

  @Test
  public void testValidPropertiesWithOverriddenStreamingPropertiesMap_withMaxClientLagInMap() {
    String overrideValue = "10";

    Map<String, String> connectorConfig = TestUtils.getConfForStreaming();
    connectorConfig.put(Utils.NAME, "testName");
    connectorConfig.put(Utils.SF_URL, "testUrl");
    connectorConfig.put(Utils.SF_ROLE, "testRole");
    connectorConfig.put(Utils.SF_USER, "testUser");
    connectorConfig.put(Utils.SF_AUTHENTICATOR, Utils.SNOWFLAKE_JWT);
    connectorConfig.put(SNOWPIPE_STREAMING_MAX_CLIENT_LAG, overrideValue);
    connectorConfig.put(
        SNOWPIPE_STREAMING_CLIENT_PROVIDER_OVERRIDE_MAP,
        "MAX_CHANNEL_SIZE_IN_BYTES:10000000,MAX_CLIENT_LAG:100");

    Properties expectedProps = StreamingUtils.convertConfigForStreamingClient(connectorConfig);
    String expectedClientName = STREAMING_CLIENT_PREFIX_NAME + connectorConfig.get(Utils.NAME);
    Map<String, String> expectedParameterOverrides = new HashMap<>();
    expectedParameterOverrides.put(MAX_CLIENT_LAG, String.format("%s second", overrideValue));
    expectedParameterOverrides.put(MAX_CHANNEL_SIZE_IN_BYTES, "10000000");

    // test get properties
    StreamingClientProperties resultProperties = new StreamingClientProperties(connectorConfig);

    // verify
    assert resultProperties.clientProperties.equals(expectedProps);
    assert resultProperties.clientName.equals(expectedClientName);
    assert resultProperties.parameterOverrides.equals(expectedParameterOverrides);
  }

  @Test
  public void
      testValidPropertiesWithOverriddenStreamingPropertiesMap_withMaxClientLagOnlyInMapOfProperties() {

    Map<String, String> connectorConfig =
        SnowflakeSinkConnectorConfigBuilder.streamingConfig()
            .withSingleBufferEnabled(false)
            .build();
    connectorConfig.put(Utils.NAME, "testName");
    connectorConfig.put(Utils.SF_URL, "testUrl");
    connectorConfig.put(Utils.SF_ROLE, "testRole");
    connectorConfig.put(Utils.SF_USER, "testUser");
    connectorConfig.put(Utils.SF_AUTHENTICATOR, Utils.SNOWFLAKE_JWT);
    connectorConfig.put(
        SNOWPIPE_STREAMING_CLIENT_PROVIDER_OVERRIDE_MAP,
        "MAX_CHANNEL_SIZE_IN_BYTES:10000000,MAX_CLIENT_LAG:100");

    Properties expectedProps = StreamingUtils.convertConfigForStreamingClient(connectorConfig);
    String expectedClientName = STREAMING_CLIENT_PREFIX_NAME + connectorConfig.get(Utils.NAME);
    Map<String, String> expectedParameterOverrides = new HashMap<>();
    expectedParameterOverrides.put(MAX_CLIENT_LAG, String.format("%s second", 100));
    expectedParameterOverrides.put(MAX_CHANNEL_SIZE_IN_BYTES, "10000000");

    // test get properties
    StreamingClientProperties resultProperties = new StreamingClientProperties(connectorConfig);

    // verify
    Assertions.assertEquals(expectedProps, resultProperties.clientProperties);
    Assertions.assertEquals(expectedClientName, resultProperties.clientName);
    Assertions.assertEquals(expectedParameterOverrides, resultProperties.parameterOverrides);
  }

  @Test
  public void testGetInvalidProperties() {
    StreamingClientProperties nullProperties = new StreamingClientProperties(null);
    StreamingClientProperties emptyProperties = new StreamingClientProperties(new HashMap<>());

    assert nullProperties.equals(emptyProperties);
    assert nullProperties.clientName.equals(STREAMING_CLIENT_PREFIX_NAME + DEFAULT_CLIENT_NAME);
    assert nullProperties.getLoggableClientProperties().equals("");
  }

  @Test
  public void testInvalidStreamingClientPropertiesMap() {
    Map<String, String> connectorConfig = TestUtils.getConfForStreaming();
    connectorConfig.put(Utils.NAME, "testName");
    connectorConfig.put(Utils.SF_URL, "testUrl");
    connectorConfig.put(Utils.SF_ROLE, "testRole");
    connectorConfig.put(Utils.SF_USER, "testUser");
    connectorConfig.put(Utils.SF_AUTHENTICATOR, Utils.SNOWFLAKE_JWT);
    connectorConfig.put(
        SNOWPIPE_STREAMING_CLIENT_PROVIDER_OVERRIDE_MAP,
        "MAX_CHANNEL_SIZE_IN_BYTES->10000000,MAX_CLIENT_LAG100");

    // test get properties
    try {
      StreamingClientProperties resultProperties = new StreamingClientProperties(connectorConfig);
      Assert.fail("Should throw an exception");
    } catch (SnowflakeKafkaConnectorException exception) {
      assert exception
          .getMessage()
          .contains(SnowflakeSinkConnectorConfig.SNOWPIPE_STREAMING_CLIENT_PROVIDER_OVERRIDE_MAP);
    }

    connectorConfig.put(
        SNOWPIPE_STREAMING_CLIENT_PROVIDER_OVERRIDE_MAP, "MAX_CHANNEL_SIZE_IN_BYTES->10000000");

    // test get properties
    try {
      StreamingClientProperties resultProperties = new StreamingClientProperties(connectorConfig);
      Assert.fail("Should throw an exception");
    } catch (SnowflakeKafkaConnectorException exception) {
      assert exception
          .getMessage()
          .contains(SnowflakeSinkConnectorConfig.SNOWPIPE_STREAMING_CLIENT_PROVIDER_OVERRIDE_MAP);
    }
  }

  @Test
  public void testStreamingClientPropertiesEquality() {
    Map<String, String> config1 = TestUtils.getConfForStreaming();
    config1.put(Utils.NAME, "catConnector");
    config1.put(SnowflakeSinkConnectorConfig.BUFFER_COUNT_RECORDS, "100");

    Map<String, String> config2 = TestUtils.getConfForStreaming();
    config1.put(Utils.NAME, "dogConnector");
    config1.put(SnowflakeSinkConnectorConfig.BUFFER_COUNT_RECORDS, "1000000");

    // get properties
    StreamingClientProperties prop1 = new StreamingClientProperties(config1);
    StreamingClientProperties prop2 = new StreamingClientProperties(config2);

    assert prop1.equals(prop2);
    assert prop1.hashCode() == prop2.hashCode();

    config1.put(SNOWPIPE_STREAMING_MAX_CLIENT_LAG, "1");
    config2.put(SNOWPIPE_STREAMING_MAX_CLIENT_LAG, "10");

    prop1 = new StreamingClientProperties(config1);
    prop2 = new StreamingClientProperties(config2);

    assert !prop1.equals(prop2);
    assert prop1.hashCode() != prop2.hashCode();
  }
}
