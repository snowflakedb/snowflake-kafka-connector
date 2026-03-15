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
import static com.snowflake.kafka.connector.internal.TestUtils.getConnectorConfigurationForStreaming;
import static com.snowflake.kafka.connector.internal.streaming.StreamingClientProperties.STREAMING_CLIENT_PREFIX_NAME;
import static org.assertj.core.api.Assertions.assertThat;

import com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams;
import com.snowflake.kafka.connector.Constants.StreamingIngestClientConfigParams;
import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.config.SinkTaskConfig;
import com.snowflake.kafka.connector.config.SnowflakeSinkConnectorConfigBuilder;
import com.snowflake.kafka.connector.internal.SnowflakeKafkaConnectorException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.junit.Assert;
import org.junit.jupiter.api.Test;

public class StreamingClientPropertiesTest {

  private static final String EXAMPLE_PARAM1 = "EXAMPLE_PARAM1".toLowerCase();
  private static final String EXAMPLE_PARAM2 = "EXAMPLE_PARAM2".toLowerCase();

  @Test
  public void testGetValidProperties() {
    // setup config with all loggable properties and parameter override
    Map<String, String> connectorConfig = getConnectorConfigurationForStreaming(false);
    connectorConfig.put(KafkaConnectorConfigParams.NAME, "testName");
    connectorConfig.put(KafkaConnectorConfigParams.SNOWFLAKE_URL_NAME, "testUrl");
    connectorConfig.put(KafkaConnectorConfigParams.SNOWFLAKE_ROLE_NAME, "testRole");
    connectorConfig.put(KafkaConnectorConfigParams.SNOWFLAKE_USER_NAME, "testUser");

    Properties expectedProps = new Properties();
    expectedProps.put(StreamingIngestClientConfigParams.ACCOUNT_URL, "testUrl");
    expectedProps.put(StreamingIngestClientConfigParams.ROLE, "testRole");
    expectedProps.put(StreamingIngestClientConfigParams.USER, "testUser");
    String privateKey = connectorConfig.get(KafkaConnectorConfigParams.SNOWFLAKE_PRIVATE_KEY);
    if (privateKey != null) {
      expectedProps.put(StreamingIngestClientConfigParams.PRIVATE_KEY, privateKey);
    }
    String expectedClientName = STREAMING_CLIENT_PREFIX_NAME + "testName";
    Map<String, Object> expectedParameterOverrides = new HashMap<>();

    // test get properties
    SinkTaskConfig config = SinkTaskConfig.from(connectorConfig);
    StreamingClientProperties resultProperties = StreamingClientProperties.from(config);

    // verify
    assert resultProperties.clientProperties.equals(expectedProps);
    assert resultProperties.clientName.equals(expectedClientName);
    assert resultProperties.parameterOverrides.equals(expectedParameterOverrides);
  }

  @Test
  void shouldPropagateStreamingClientPropertiesFromOverrideMap() {
    // GIVEN
    Map<String, String> connectorConfig =
        SnowflakeSinkConnectorConfigBuilder.streamingConfig().build();

    connectorConfig.put(Utils.TASK_ID, "0");
    connectorConfig.put(
        SNOWFLAKE_STREAMING_CLIENT_PROVIDER_OVERRIDE_MAP, "EXAMPLE_PARAM1:1,EXAMPLE_PARAM2:2");

    Map<String, Object> expectedParameterOverrides = new HashMap<>();
    expectedParameterOverrides.put(EXAMPLE_PARAM1, "1");
    expectedParameterOverrides.put(EXAMPLE_PARAM2, "2");

    // WHEN
    SinkTaskConfig config = SinkTaskConfig.from(connectorConfig);
    StreamingClientProperties resultProperties = StreamingClientProperties.from(config);

    // THEN
    assertThat(resultProperties.parameterOverrides).isEqualTo(expectedParameterOverrides);
  }

  @Test
  void explicitStreamingClientPropertiesTakePrecedenceOverOverrideMap_SingleBufferEnabled() {
    // GIVEN
    Map<String, String> connectorConfig =
        SnowflakeSinkConnectorConfigBuilder.streamingConfig().build();

    connectorConfig.put(Utils.TASK_ID, "0");
    connectorConfig.put(
        SNOWFLAKE_STREAMING_CLIENT_PROVIDER_OVERRIDE_MAP, "EXAMPLE_PARAM1:1,EXAMPLE_PARAM2:2");

    Map<String, Object> expectedParameterOverrides = new HashMap<>();
    expectedParameterOverrides.put(EXAMPLE_PARAM1, "1");
    expectedParameterOverrides.put(EXAMPLE_PARAM2, "2");

    // WHEN
    SinkTaskConfig config = SinkTaskConfig.from(connectorConfig);
    StreamingClientProperties resultProperties = StreamingClientProperties.from(config);

    // THEN
    assertThat(resultProperties.parameterOverrides).isEqualTo(expectedParameterOverrides);
  }

  @Test
  public void testValidPropertiesWithOverriddenStreamingPropertiesMap() {
    Map<String, String> connectorConfig = getConnectorConfigurationForStreaming(true);
    connectorConfig.put(KafkaConnectorConfigParams.NAME, "testName");
    connectorConfig.put(KafkaConnectorConfigParams.SNOWFLAKE_URL_NAME, "testUrl");
    connectorConfig.put(KafkaConnectorConfigParams.SNOWFLAKE_ROLE_NAME, "testRole");
    connectorConfig.put(KafkaConnectorConfigParams.SNOWFLAKE_USER_NAME, "testUser");
    connectorConfig.put(
        SNOWFLAKE_STREAMING_CLIENT_PROVIDER_OVERRIDE_MAP, "EXAMPLE_PARAM2:10000000");

    Properties expectedProps = new Properties();
    expectedProps.put(StreamingIngestClientConfigParams.ACCOUNT_URL, "testUrl");
    expectedProps.put(StreamingIngestClientConfigParams.ROLE, "testRole");
    expectedProps.put(StreamingIngestClientConfigParams.USER, "testUser");
    String privateKey = connectorConfig.get(KafkaConnectorConfigParams.SNOWFLAKE_PRIVATE_KEY);
    if (privateKey != null) {
      expectedProps.put(StreamingIngestClientConfigParams.PRIVATE_KEY, privateKey);
    }
    String expectedClientName = STREAMING_CLIENT_PREFIX_NAME + "testName";
    Map<String, Object> expectedParameterOverrides = new HashMap<>();
    expectedParameterOverrides.put(EXAMPLE_PARAM2, "10000000");

    // test get properties
    SinkTaskConfig config = SinkTaskConfig.from(connectorConfig);
    StreamingClientProperties resultProperties = StreamingClientProperties.from(config);

    // verify
    assert resultProperties.clientProperties.equals(expectedProps);
    assert resultProperties.clientName.equals(expectedClientName);
    assert resultProperties.parameterOverrides.equals(expectedParameterOverrides);
  }

  @Test
  public void testInvalidStreamingClientPropertiesMap() {
    Map<String, String> connectorConfig = getConnectorConfigurationForStreaming(true);
    connectorConfig.put(KafkaConnectorConfigParams.NAME, "testName");
    connectorConfig.put(KafkaConnectorConfigParams.SNOWFLAKE_URL_NAME, "testUrl");
    connectorConfig.put(KafkaConnectorConfigParams.SNOWFLAKE_ROLE_NAME, "testRole");
    connectorConfig.put(KafkaConnectorConfigParams.SNOWFLAKE_USER_NAME, "testUser");
    connectorConfig.put(
        SNOWFLAKE_STREAMING_CLIENT_PROVIDER_OVERRIDE_MAP,
        "MAX_CHANNEL_SIZE_IN_BYTES->10000000,MAX_CLIENT_LAG100");

    // test get properties
    try {
      SinkTaskConfig config = SinkTaskConfig.from(connectorConfig);
      StreamingClientProperties.from(config);
      Assert.fail("Should throw an exception");
    } catch (SnowflakeKafkaConnectorException exception) {
      assert exception
          .getMessage()
          .contains(KafkaConnectorConfigParams.SNOWFLAKE_STREAMING_CLIENT_PROVIDER_OVERRIDE_MAP);
    }

    connectorConfig.put(
        SNOWFLAKE_STREAMING_CLIENT_PROVIDER_OVERRIDE_MAP, "MAX_CHANNEL_SIZE_IN_BYTES->10000000");

    // test get properties
    try {
      SinkTaskConfig config = SinkTaskConfig.from(connectorConfig);
      StreamingClientProperties.from(config);
      Assert.fail("Should throw an exception");
    } catch (SnowflakeKafkaConnectorException exception) {
      assert exception
          .getMessage()
          .contains(KafkaConnectorConfigParams.SNOWFLAKE_STREAMING_CLIENT_PROVIDER_OVERRIDE_MAP);
    }
  }

  @Test
  public void testStreamingClientPropertiesEquality() {
    Map<String, String> config1 = getConnectorConfigurationForStreaming(true);
    config1.put(KafkaConnectorConfigParams.NAME, "catConnector");

    Map<String, String> config2 = getConnectorConfigurationForStreaming(true);
    config2.put(KafkaConnectorConfigParams.NAME, "dogConnector");

    // get properties
    StreamingClientProperties prop1 = StreamingClientProperties.from(SinkTaskConfig.from(config1));
    StreamingClientProperties prop2 = StreamingClientProperties.from(SinkTaskConfig.from(config2));

    assert prop1.equals(prop2);
    assert prop1.hashCode() == prop2.hashCode();

    config1.put(
        SNOWFLAKE_STREAMING_CLIENT_PROVIDER_OVERRIDE_MAP,
        "max_append_request_buffer_duration_ms:1000");
    config2.put(
        SNOWFLAKE_STREAMING_CLIENT_PROVIDER_OVERRIDE_MAP,
        "max_append_request_buffer_duration_ms:10000");

    prop1 = StreamingClientProperties.from(SinkTaskConfig.from(config1));
    prop2 = StreamingClientProperties.from(SinkTaskConfig.from(config2));

    assert !prop1.equals(prop2);
    assert prop1.hashCode() != prop2.hashCode();
  }
}
