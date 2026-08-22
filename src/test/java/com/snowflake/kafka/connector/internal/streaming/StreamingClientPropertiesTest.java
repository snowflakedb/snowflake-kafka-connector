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
import com.snowflake.kafka.connector.config.AuthenticatorType;
import com.snowflake.kafka.connector.config.SinkTaskConfig;
import com.snowflake.kafka.connector.config.SnowflakeSinkConnectorConfigBuilder;
import com.snowflake.kafka.connector.internal.PrivateKeyTool;
import com.snowflake.kafka.connector.internal.SnowflakeKafkaConnectorException;
import com.snowflake.kafka.connector.internal.SnowflakeURL;
import com.snowflake.kafka.connector.internal.spcs.SpcsEnvironment;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.PrivateKey;
import java.util.Base64;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import org.apache.kafka.common.config.types.Password;
import org.junit.Assert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

public class StreamingClientPropertiesTest {

  private static final String EXAMPLE_PARAM1 = "EXAMPLE_PARAM1".toLowerCase(Locale.ROOT);
  private static final String EXAMPLE_PARAM2 = "EXAMPLE_PARAM2".toLowerCase(Locale.ROOT);

  @AfterEach
  void resetSpcsOverride() {
    SpcsEnvironment.resetForTests();
  }

  /**
   * Guards the exhaustiveness of the authenticator switch in {@link
   * StreamingClientProperties#from}. Before the explicit {@code default: throw}, an unhandled
   * {@link AuthenticatorType} fell through silently and produced client properties with no
   * credential material at all, surfacing later as an unrelated error inside the streaming SDK.
   * Adding a new enum constant without handling it here now fails this test immediately.
   */
  @ParameterizedTest
  @EnumSource(AuthenticatorType.class)
  void shouldHandleEveryAuthenticatorType(AuthenticatorType authenticator, @TempDir Path tempDir)
      throws IOException {
    // GIVEN a config that is valid for this authenticator
    SnowflakeSinkConnectorConfigBuilder builder =
        SnowflakeSinkConnectorConfigBuilder.streamingConfig()
            .withAuthenticator(authenticator.toConfigValue())
            .withPrivateKey(Base64.getEncoder().encodeToString(generatePrivateKey().getEncoded()));
    if (authenticator == AuthenticatorType.OAUTH) {
      builder =
          builder
              .withOauthClientId("testClientId")
              .withOauthClientSecret("testClientSecret")
              .withOauthRefreshToken("testRefreshToken");
    }
    if (authenticator == AuthenticatorType.SPCS) {
      // ambient auth is only valid inside SPCS, so simulate that runtime
      Path token = tempDir.resolve("token");
      Files.write(token, "ambient-token".getBytes(StandardCharsets.UTF_8));
      Map<String, String> env = new HashMap<>();
      env.put(SpcsEnvironment.ENV_HOST, "myaccount.us-east-1.snowflakecomputing.com");
      SpcsEnvironment.overrideForTests(env::get, token);
    }
    Map<String, String> connectorConfig = builder.build();
    connectorConfig.put(Utils.TASK_ID, "0");

    // WHEN
    Properties clientProperties =
        StreamingClientProperties.from(SinkTaskConfig.from(connectorConfig)).clientProperties;

    // THEN the switch handled it and emitted this authenticator's credential material
    assertThat(clientProperties.stringPropertyNames())
        .containsAnyOf("authorization_type", "private_key");
  }

  /**
   * Ambient SPCS mode must send only the authorization type and let the SDK default the token
   * paths. Setting any oauth_* key here, or a spcs_*_path differing from the SDK default, would be
   * rejected by the SDK's exclusive-field validation.
   *
   * <p>The account comes from SNOWFLAKE_ACCOUNT, not from parsing the host. The host format is
   * undocumented and was verified on one deployment; SNOWFLAKE_ACCOUNT is the canonical identifier
   * published by every SPCS runtime and is how the Snowflake CLI establishes its account.
   */
  @Test
  void shouldSetSpcsAuthorizationTypeAndUseAccountEnvVar(@TempDir Path tempDir) throws IOException {
    Path token = tempDir.resolve("token");
    Files.write(token, "ambient-token".getBytes(StandardCharsets.UTF_8));
    Map<String, String> env = new HashMap<>();
    env.put(SpcsEnvironment.ENV_HOST, "my-account.prod3.us-west-2.aws.snowflakecomputing.com");
    env.put(SpcsEnvironment.ENV_ACCOUNT, "MY_ACCOUNT_FROM_ENV");
    SpcsEnvironment.overrideForTests(env::get, token);
    Map<String, String> connectorConfig =
        SnowflakeSinkConnectorConfigBuilder.streamingConfig()
            .withAuthenticator(AuthenticatorType.SPCS.toConfigValue())
            .withoutUrl()
            .build();
    connectorConfig.put(Utils.TASK_ID, "0");

    // WHEN
    Properties clientProperties =
        StreamingClientProperties.from(SinkTaskConfig.from(connectorConfig)).clientProperties;

    // THEN: account comes from SNOWFLAKE_ACCOUNT, not from parsing the host
    assertThat(clientProperties.getProperty("authorization_type")).isEqualTo("spcs");
    assertThat(clientProperties.getProperty("account"))
        .as("account from SNOWFLAKE_ACCOUNT, case-normalized to lower case")
        .isEqualTo("my_account_from_env");
    assertThat(clientProperties.stringPropertyNames())
        .doesNotContain(
            "oauth_client_id",
            "oauth_client_secret",
            "oauth_refresh_token",
            "oauth_token_endpoint",
            "oauth_scope",
            "oauth_include_scope",
            "private_key",
            "spcs_token_path",
            "spcs_service_token_path");
    assertThat(clientProperties.stringPropertyNames()).doesNotContain("user");
    assertThat(clientProperties.getProperty("role")).isNotNull();
  }

  /**
   * When SNOWFLAKE_ACCOUNT is absent, fall back to parsing the first label of the host URL. This is
   * the pre-existing behavior and is kept so that any SPCS deployment that does not publish
   * SNOWFLAKE_ACCOUNT still works.
   */
  @Test
  void shouldFallBackToHostParsingWhenAccountEnvVarIsAbsent(@TempDir Path tempDir)
      throws IOException {
    Path token = tempDir.resolve("token");
    Files.write(token, "ambient-token".getBytes(StandardCharsets.UTF_8));
    Map<String, String> env = new HashMap<>();
    // No ENV_ACCOUNT: only host is present, simulating an older SPCS deployment.
    env.put(SpcsEnvironment.ENV_HOST, "my-account.prod3.us-west-2.aws.snowflakecomputing.com");
    SpcsEnvironment.overrideForTests(env::get, token);
    Map<String, String> connectorConfig =
        SnowflakeSinkConnectorConfigBuilder.streamingConfig()
            .withAuthenticator(AuthenticatorType.SPCS.toConfigValue())
            .withoutUrl()
            .build();
    connectorConfig.put(Utils.TASK_ID, "0");

    Properties clientProperties =
        StreamingClientProperties.from(SinkTaskConfig.from(connectorConfig)).clientProperties;

    assertThat(clientProperties.getProperty("account"))
        .as("falls back to the first label of the host when SNOWFLAKE_ACCOUNT is absent")
        .isEqualTo("my-account");
  }

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
      Optional<Password> passphrase =
          Optional.ofNullable(
                  connectorConfig.get(KafkaConnectorConfigParams.SNOWFLAKE_PRIVATE_KEY_PASSPHRASE))
              .map(Password::new);
      PrivateKey privateKey =
          PrivateKeyTool.parsePrivateKey(new Password(privateKeyStr), passphrase);
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

  @Test
  void oAuthConfig_setsAuthorizationTypeAndCredentials() {
    Map<String, String> connectorConfig =
        SnowflakeSinkConnectorConfigBuilder.streamingConfig()
            .withAuthenticator(AuthenticatorType.OAUTH.toConfigValue())
            .withOauthClientId("test_client_id")
            .withOauthClientSecret("test_client_secret")
            .withOauthRefreshToken("test_refresh_token")
            .withOauthTokenEndpoint("https://oauth.example.com/token")
            .withoutPrivateKey()
            .build();
    connectorConfig.put(Utils.TASK_ID, "0");

    SinkTaskConfig config = SinkTaskConfig.from(connectorConfig);
    StreamingClientProperties properties = StreamingClientProperties.from(config);

    assertThat(properties.clientProperties)
        .containsEntry("authorization_type", AuthenticatorType.OAUTH.toConfigValue())
        .containsEntry("oauth_client_id", "test_client_id")
        .containsEntry("oauth_client_secret", "test_client_secret")
        .containsEntry("oauth_refresh_token", "test_refresh_token")
        .containsEntry("oauth_token_endpoint", "https://oauth.example.com/token")
        // Defaults to disabling scope so the SDK doesn't derive session:role:{role}.
        .containsEntry("oauth_include_scope", "false")
        .doesNotContainKey("oauth_scope")
        .doesNotContainKey("private_key");
  }

  @Test
  void oAuthConfig_includeScopeEnabled_forwardsScope() {
    Map<String, String> connectorConfig =
        SnowflakeSinkConnectorConfigBuilder.streamingConfig()
            .withAuthenticator(AuthenticatorType.OAUTH.toConfigValue())
            .withOauthClientId("test_client_id")
            .withOauthClientSecret("test_client_secret")
            .withOauthRefreshToken("test_refresh_token")
            .withOauthTokenEndpoint("https://oauth.example.com/token")
            .withOauthIncludeScope(true)
            .withOauthScope("session:role:MY_ROLE")
            .withoutPrivateKey()
            .build();
    connectorConfig.put(Utils.TASK_ID, "0");

    SinkTaskConfig config = SinkTaskConfig.from(connectorConfig);
    StreamingClientProperties properties = StreamingClientProperties.from(config);

    assertThat(properties.clientProperties)
        .containsEntry("oauth_include_scope", "true")
        .containsEntry("oauth_scope", "session:role:MY_ROLE");
  }

  @Test
  void oAuthConfig_includeScopeEnabledWithoutExplicitScope_letsSdkDeriveScope() {
    Map<String, String> connectorConfig =
        SnowflakeSinkConnectorConfigBuilder.streamingConfig()
            .withAuthenticator(AuthenticatorType.OAUTH.toConfigValue())
            .withOauthClientId("test_client_id")
            .withOauthClientSecret("test_client_secret")
            .withOauthRefreshToken("test_refresh_token")
            .withOauthTokenEndpoint("https://oauth.example.com/token")
            .withOauthIncludeScope(true)
            .withoutPrivateKey()
            .build();
    connectorConfig.put(Utils.TASK_ID, "0");

    SinkTaskConfig config = SinkTaskConfig.from(connectorConfig);
    StreamingClientProperties properties = StreamingClientProperties.from(config);

    assertThat(properties.clientProperties)
        .containsEntry("oauth_include_scope", "true")
        .doesNotContainKey("oauth_scope");
  }

  @Test
  void oAuthConfig_clientCredentials_omitsRefreshToken() {
    Map<String, String> connectorConfig =
        SnowflakeSinkConnectorConfigBuilder.streamingConfig()
            .withAuthenticator(AuthenticatorType.OAUTH.toConfigValue())
            .withOauthClientId("test_client_id")
            .withOauthClientSecret("test_client_secret")
            .withoutPrivateKey()
            .build();
    connectorConfig.put(Utils.TASK_ID, "0");

    SinkTaskConfig config = SinkTaskConfig.from(connectorConfig);
    StreamingClientProperties properties = StreamingClientProperties.from(config);

    assertThat(properties.clientProperties)
        .containsEntry("authorization_type", AuthenticatorType.OAUTH.toConfigValue())
        .containsEntry("oauth_client_id", "test_client_id")
        .containsEntry("oauth_client_secret", "test_client_secret")
        .doesNotContainKey("oauth_refresh_token")
        .doesNotContainKey("private_key");
  }

  @Test
  void jwtConfig_setsPrivateKey_noOAuthProperties() {
    Map<String, String> connectorConfig =
        SnowflakeSinkConnectorConfigBuilder.streamingConfig()
            .withPrivateKey(Base64.getEncoder().encodeToString(generatePrivateKey().getEncoded()))
            .build();
    connectorConfig.put(Utils.TASK_ID, "0");

    SinkTaskConfig config = SinkTaskConfig.from(connectorConfig);
    StreamingClientProperties properties = StreamingClientProperties.from(config);

    assertThat(properties.clientProperties)
        .containsKey("private_key")
        .doesNotContainKey("authorization_type")
        .doesNotContainKey("oauth_client_id");
  }
}
