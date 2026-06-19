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
import static com.snowflake.kafka.connector.internal.TestUtils.generatePrivateKey;
import static com.snowflake.kafka.connector.internal.TestUtils.getConnectorConfigurationForStreaming;
import static com.snowflake.kafka.connector.internal.streaming.StreamingClientProperties.STREAMING_CLIENT_V2_PREFIX_NAME;
import static org.assertj.core.api.Assertions.assertThat;

import com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams;
import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.config.SinkTaskConfig;
import com.snowflake.kafka.connector.config.SnowflakeSinkConnectorConfigBuilder;
import com.snowflake.kafka.connector.internal.PrivateKeyTool;
import com.snowflake.kafka.connector.internal.SnowflakeKafkaConnectorException;
import com.snowflake.kafka.connector.internal.SnowflakeURL;
import java.security.PrivateKey;
import java.util.Base64;
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
    String privateKeyPem = Base64.getEncoder().encodeToString(generatePrivateKey().getEncoded());
    String testUrl = "https://testaccount.us-east-1.snowflakecomputing.com";

    Map<String, String> connectorConfig = new HashMap<>();
    connectorConfig.put(KafkaConnectorConfigParams.NAME, "testName");
    connectorConfig.put(Utils.TASK_ID, "0");
    connectorConfig.put(KafkaConnectorConfigParams.SNOWFLAKE_URL_NAME, testUrl);
    connectorConfig.put(KafkaConnectorConfigParams.SNOWFLAKE_ROLE_NAME, "testRole");
    connectorConfig.put(KafkaConnectorConfigParams.SNOWFLAKE_USER_NAME, "testUser");
    connectorConfig.put(KafkaConnectorConfigParams.SNOWFLAKE_PRIVATE_KEY, privateKeyPem);

    SinkTaskConfig config = SinkTaskConfig.from(connectorConfig);
    StreamingClientProperties result = StreamingClientProperties.from(config);

    // verify client properties
    Properties clientProps = result.clientProperties;
    assertThat(clientProps.getProperty("user")).isEqualTo("testUser");
    assertThat(clientProps.getProperty("role")).isEqualTo("testRole");
    assertThat(clientProps.getProperty("account")).isEqualTo("testaccount");
    assertThat(clientProps.getProperty("host"))
        .isEqualTo("testaccount.us-east-1.snowflakecomputing.com");
    assertThat(clientProps.getProperty("private_key")).isEqualTo(privateKeyPem);
    assertThat(clientProps.getProperty("application"))
        .isEqualTo("SnowflakeKafkaConnector/" + Utils.VERSION);
    assertThat(clientProps).hasSize(6);

    // verify client name prefix and empty parameter overrides
    assertThat(result.clientNamePrefix).isEqualTo(STREAMING_CLIENT_V2_PREFIX_NAME + "testName");
    assertThat(result.parameterOverrides).isEmpty();
  }

  @Test
  void shouldPropagateStreamingClientPropertiesFromOverrideMap() {
    // GIVEN
    Map<String, String> connectorConfig =
        SnowflakeSinkConnectorConfigBuilder.streamingConfig().build();

    connectorConfig.put(Utils.TASK_ID, "0");
    connectorConfig.put(
        KafkaConnectorConfigParams.SNOWFLAKE_PRIVATE_KEY,
        Base64.getEncoder().encodeToString(generatePrivateKey().getEncoded()));
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
        KafkaConnectorConfigParams.SNOWFLAKE_PRIVATE_KEY,
        Base64.getEncoder().encodeToString(generatePrivateKey().getEncoded()));
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
    String testUrl = "https://testaccount.us-east-1.snowflakecomputing.com";
    connectorConfig.put(KafkaConnectorConfigParams.SNOWFLAKE_URL_NAME, testUrl);
    connectorConfig.put(KafkaConnectorConfigParams.SNOWFLAKE_ROLE_NAME, "testRole");
    connectorConfig.put(KafkaConnectorConfigParams.SNOWFLAKE_USER_NAME, "testUser");
    connectorConfig.put(
        SNOWFLAKE_STREAMING_CLIENT_PROVIDER_OVERRIDE_MAP, "EXAMPLE_PARAM2:10000000");

    SnowflakeURL parsedUrl = new SnowflakeURL(testUrl);
    Properties expectedProps = new Properties();
    expectedProps.put("user", "testUser");
    expectedProps.put("role", "testRole");
    expectedProps.put("account", parsedUrl.getAccount());
    expectedProps.put("host", parsedUrl.getUrlWithoutPort());
    expectedProps.put("application", "SnowflakeKafkaConnector/" + Utils.VERSION);
    String privateKeyStr = connectorConfig.get(KafkaConnectorConfigParams.SNOWFLAKE_PRIVATE_KEY);
    if (privateKeyStr != null) {
      String passphrase =
          connectorConfig.get(KafkaConnectorConfigParams.SNOWFLAKE_PRIVATE_KEY_PASSPHRASE);
      PrivateKey privateKey = PrivateKeyTool.parsePrivateKey(privateKeyStr, passphrase);
      expectedProps.put("private_key", Base64.getEncoder().encodeToString(privateKey.getEncoded()));
    }
    String expectedClientName = STREAMING_CLIENT_V2_PREFIX_NAME + "testName";
    Map<String, Object> expectedParameterOverrides = new HashMap<>();
    expectedParameterOverrides.put(EXAMPLE_PARAM2, "10000000");

    // test get properties
    SinkTaskConfig config = SinkTaskConfig.from(connectorConfig);
    StreamingClientProperties resultProperties = StreamingClientProperties.from(config);

    // verify
    assert resultProperties.clientProperties.equals(expectedProps);
    assert resultProperties.clientNamePrefix.equals(expectedClientName);
    assert resultProperties.parameterOverrides.equals(expectedParameterOverrides);
  }

  @Test
  public void testInvalidStreamingClientPropertiesMap() {
    Map<String, String> connectorConfig = getConnectorConfigurationForStreaming(true);
    connectorConfig.put(KafkaConnectorConfigParams.NAME, "testName");
    connectorConfig.put(
        KafkaConnectorConfigParams.SNOWFLAKE_URL_NAME,
        "https://testaccount.us-east-1.snowflakecomputing.com");
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
