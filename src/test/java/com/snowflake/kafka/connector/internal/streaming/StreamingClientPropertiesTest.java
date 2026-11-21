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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

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
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import org.apache.kafka.common.config.types.Password;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.Assert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

public class StreamingClientPropertiesTest {

  private static final String EXAMPLE_PARAM1 = "max_append_request_buffer_duration_ms";
  private static final String EXAMPLE_PARAM2 = "data_http_timeout_seconds";

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
        SNOWFLAKE_STREAMING_CLIENT_PROVIDER_OVERRIDE_MAP,
        "MAX_APPEND_REQUEST_BUFFER_DURATION_MS:1,DATA_HTTP_TIMEOUT_SECONDS:2");

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
        SNOWFLAKE_STREAMING_CLIENT_PROVIDER_OVERRIDE_MAP,
        "MAX_APPEND_REQUEST_BUFFER_DURATION_MS:1,DATA_HTTP_TIMEOUT_SECONDS:2");

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
        SNOWFLAKE_STREAMING_CLIENT_PROVIDER_OVERRIDE_MAP, "DATA_HTTP_TIMEOUT_SECONDS:10000000");

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

  private static Properties spcsClientProperties(Path tempDir, String role) throws IOException {
    Path token = tempDir.resolve("token");
    Files.write(token, "ambient-token".getBytes(StandardCharsets.UTF_8));
    Map<String, String> env = new HashMap<>();
    env.put(SpcsEnvironment.ENV_HOST, "myaccount.us-east-1.snowflakecomputing.com");
    env.put(SpcsEnvironment.ENV_ACCOUNT, "myaccount");
    SpcsEnvironment.overrideForTests(env::get, token);
    SnowflakeSinkConnectorConfigBuilder builder =
        SnowflakeSinkConnectorConfigBuilder.streamingConfig()
            .withAuthenticator(AuthenticatorType.SPCS.toConfigValue())
            .withoutUrl();
    builder = role == null ? builder.withoutRole() : builder.withRole(role);
    Map<String, String> connectorConfig = builder.build();
    connectorConfig.put(Utils.TASK_ID, "0");
    return StreamingClientProperties.from(SinkTaskConfig.from(connectorConfig)).clientProperties;
  }

  /** SNOW-4392681: with no role configured, none is sent, so the service owner role applies. */
  @Test
  void shouldOmitRoleUnderSpcsWhenNoneConfigured(@TempDir Path tempDir) throws IOException {
    assertThat(spcsClientProperties(tempDir, null)).doesNotContainKey("role");
  }

  @Test
  void shouldSendConfiguredRoleUnderSpcs(@TempDir Path tempDir) throws IOException {
    assertThat(spcsClientProperties(tempDir, "SOME_ROLE")).containsEntry("role", "SOME_ROLE");
  }

  @Test
  void shouldSendConfiguredRoleForKeyPairAuthentication() {
    Map<String, String> connectorConfig =
        SnowflakeSinkConnectorConfigBuilder.streamingConfig()
            .withPrivateKey(Base64.getEncoder().encodeToString(generatePrivateKey().getEncoded()))
            .withRole("KEYPAIR_ROLE")
            .build();
    connectorConfig.put(Utils.TASK_ID, "0");

    Properties clientProperties =
        StreamingClientProperties.from(SinkTaskConfig.from(connectorConfig)).clientProperties;

    assertThat(clientProperties).containsEntry("role", "KEYPAIR_ROLE");
  }

  @Test
  void connectorProxySettingsAreCopiedOntoSdkClientProperties() {
    Map<String, String> connectorConfig = streamingConfigWithKey();
    connectorConfig.put(KafkaConnectorConfigParams.SNOWFLAKE_HTTPS_PROXY_HOST, "proxy.example.com");
    connectorConfig.put(KafkaConnectorConfigParams.SNOWFLAKE_HTTPS_PROXY_PORT, "3128");
    connectorConfig.put(
        KafkaConnectorConfigParams.SNOWFLAKE_PROXY_BYPASS_HOSTS,
        "localhost|*.internal.example.com");
    connectorConfig.put(KafkaConnectorConfigParams.SNOWFLAKE_HTTPS_PROXY_USERNAME, "proxy-user");
    connectorConfig.put(
        KafkaConnectorConfigParams.SNOWFLAKE_HTTPS_PROXY_PASSWORD, "proxy-secret-value");

    StreamingClientProperties properties =
        StreamingClientProperties.from(SinkTaskConfig.from(connectorConfig));

    assertThat(properties.clientProperties)
        .containsEntry("https_proxy_url", "http://proxy.example.com:3128")
        .containsEntry("no_proxy", "localhost|*.internal.example.com")
        .containsEntry("proxy_user", "proxy-user")
        .containsEntry("proxy_password", "proxy-secret-value");
    assertThat(properties.parameterOverrides).isEmpty();
    assertThat(properties.parameterOverrides.toString()).doesNotContain("proxy-secret-value");
  }

  @Test
  void unsetProxyDoesNotSetSdkProxyProperties() {
    Properties clientProperties =
        StreamingClientProperties.from(SinkTaskConfig.from(streamingConfigWithKey()))
            .clientProperties;

    assertThat(clientProperties)
        .doesNotContainKey("https_proxy_url")
        .doesNotContainKey("no_proxy")
        .doesNotContainKey("proxy_user")
        .doesNotContainKey("proxy_password");
  }

  @Test
  void jvmSystemPropertiesAreUsedWhenConnectorProxyIsUnset() {
    String previousHost = System.getProperty(KafkaConnectorConfigParams.HTTPS_PROXY_HOST);
    String previousPort = System.getProperty(KafkaConnectorConfigParams.HTTPS_PROXY_PORT);
    String previousBypass = System.getProperty(KafkaConnectorConfigParams.HTTP_NON_PROXY_HOSTS);
    String previousUser = System.getProperty(KafkaConnectorConfigParams.HTTPS_PROXY_USER);
    String previousPassword = System.getProperty(KafkaConnectorConfigParams.HTTPS_PROXY_PASSWORD);
    System.setProperty(KafkaConnectorConfigParams.HTTPS_PROXY_HOST, "jvm-proxy.example.com");
    System.setProperty(KafkaConnectorConfigParams.HTTPS_PROXY_PORT, "8443");
    System.setProperty(
        KafkaConnectorConfigParams.HTTP_NON_PROXY_HOSTS, "*.snowflakecomputing.com|localhost");
    System.setProperty(KafkaConnectorConfigParams.HTTPS_PROXY_USER, "jvm-user");
    System.setProperty(KafkaConnectorConfigParams.HTTPS_PROXY_PASSWORD, "jvm-secret-value");
    try {
      Properties clientProperties =
          StreamingClientProperties.from(SinkTaskConfig.from(streamingConfigWithKey()))
              .clientProperties;

      assertThat(clientProperties)
          .containsEntry("https_proxy_url", "http://jvm-proxy.example.com:8443")
          .containsEntry("no_proxy", "*.snowflakecomputing.com|localhost")
          .containsEntry("proxy_user", "jvm-user")
          .containsEntry("proxy_password", "jvm-secret-value");
      assertThat(System.getProperty(KafkaConnectorConfigParams.HTTPS_PROXY_HOST))
          .isEqualTo("jvm-proxy.example.com");
    } finally {
      restoreProperty(KafkaConnectorConfigParams.HTTPS_PROXY_HOST, previousHost);
      restoreProperty(KafkaConnectorConfigParams.HTTPS_PROXY_PORT, previousPort);
      restoreProperty(KafkaConnectorConfigParams.HTTP_NON_PROXY_HOSTS, previousBypass);
      restoreProperty(KafkaConnectorConfigParams.HTTPS_PROXY_USER, previousUser);
      restoreProperty(KafkaConnectorConfigParams.HTTPS_PROXY_PASSWORD, previousPassword);
    }
  }

  private static void restoreProperty(String key, String previous) {
    if (previous == null) {
      System.clearProperty(key);
    } else {
      System.setProperty(key, previous);
    }
  }

  private static Map<String, String> streamingConfigWithKey() {
    Map<String, String> connectorConfig =
        SnowflakeSinkConnectorConfigBuilder.streamingConfig()
            .withPrivateKey(Base64.getEncoder().encodeToString(generatePrivateKey().getEncoded()))
            .build();
    connectorConfig.put(Utils.TASK_ID, "0");
    return connectorConfig;
  }

  private static Map<String, String> configWithOverrideMap(String overrideMap) {
    Map<String, String> connectorConfig =
        SnowflakeSinkConnectorConfigBuilder.streamingConfig().build();
    connectorConfig.put(Utils.TASK_ID, "0");
    connectorConfig.put(
        KafkaConnectorConfigParams.SNOWFLAKE_PRIVATE_KEY,
        Base64.getEncoder().encodeToString(generatePrivateKey().getEncoded()));
    connectorConfig.put(SNOWFLAKE_STREAMING_CLIENT_PROVIDER_OVERRIDE_MAP, overrideMap);
    return connectorConfig;
  }

  @Test
  void shouldNormalizeAllowedOverrideKeysToLowerCase() {
    Map<String, String> connectorConfig =
        configWithOverrideMap(
            " Max_Append_Request_Buffer_Duration_Ms : 250 ,ENABLE_ROWSET_COMPRESSION:false");

    StreamingClientProperties result =
        StreamingClientProperties.from(SinkTaskConfig.from(connectorConfig));

    assertThat(result.parameterOverrides)
        .containsOnly(
            Map.entry("max_append_request_buffer_duration_ms", "250"),
            Map.entry("enable_rowset_compression", "false"));
  }

  /**
   * The SDK merges the override map over the client properties, so any of these keys would let a
   * connector config editor send authenticated requests and row payloads to another endpoint or
   * replace the identity and credentials the connector sends.
   */
  @ParameterizedTest
  @ValueSource(
      strings = {
        "scheme:http",
        "host:other.example.com",
        "port:8080",
        "HOST:other.example.com",
        " Scheme :http",
        "url:other",
        "ssl:off",
        "account:other",
        "user:other",
        "role:other",
        "private_key:abc",
        "private_key_file:/tmp/key",
        "authorization_type:pat",
        "personal_access_token:abc",
        "oauth_token_endpoint:other",
        "oauth_client_secret:abc",
        "spcs_token_path:/tmp/token",
        "file_upload_endpoint:other",
        "data_header_overrides:x",
        "replace_underscore_with_dash_in_host:false",
        "encode_streaming_url_components:false",
        "application:other",
        "max_append_request_buffer_duration_ms:1000,host:other.example.com",
        "unknown_param:1",
        "MAX_CLIENT_LAG:1"
      })
  void shouldRejectOverrideKeysOutsideTheTuningAllowlist(String overrideMap) {
    SinkTaskConfig config = SinkTaskConfig.from(configWithOverrideMap(overrideMap));

    assertThatThrownBy(() -> StreamingClientProperties.from(config))
        .isInstanceOf(SnowflakeKafkaConnectorException.class)
        .hasMessageContaining(SNOWFLAKE_STREAMING_CLIENT_PROVIDER_OVERRIDE_MAP)
        .hasMessageContaining("Unsupported key");
  }

  @Test
  void shouldRejectOverrideKeysThatDifferOnlyInCase() {
    SinkTaskConfig config =
        SinkTaskConfig.from(
            configWithOverrideMap(
                "max_append_request_buffer_duration_ms:1,MAX_APPEND_REQUEST_BUFFER_DURATION_MS:2"));

    assertThatThrownBy(() -> StreamingClientProperties.from(config))
        .isInstanceOf(SnowflakeKafkaConnectorException.class)
        .hasMessageContaining("more than once");
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "max_append_request_buffer_duration_ms:1\n[main] INFO injected",
        "max_append_request_buffer_duration_ms:1\r[main] INFO injected",
        "max_append_request_buffer_duration_ms\n[main] INFO injected:1",
        "max_append_request_buffer_duration_ms\r\n[main] INFO injected:1"
      })
  void shouldNotSplitLogLinesFromOverrideMap(String overrideMap) {
    SinkTaskConfig config = SinkTaskConfig.from(configWithOverrideMap(overrideMap));

    List<String> logged = new ArrayList<>();
    Throwable thrown;
    CapturingAppender appender = new CapturingAppender(logged);
    Logger.getRootLogger().addAppender(appender);
    try {
      thrown = catchThrowable(() -> StreamingClientProperties.from(config));
    } finally {
      Logger.getRootLogger().removeAppender(appender);
    }

    assertThat(thrown).isInstanceOf(SnowflakeKafkaConnectorException.class);
    logged.add(thrown.getMessage());
    assertThat(logged)
        .as("no log record or error message may start a line with override map text")
        .noneMatch(message -> message.matches("(?s).*[\\r\\n]\\[main\\] INFO injected.*"));
  }

  @Test
  void shouldNotLogOverrideValues() {
    SinkTaskConfig config =
        SinkTaskConfig.from(configWithOverrideMap("max_append_request_buffer_duration_ms:4321"));

    List<String> logged = new ArrayList<>();
    CapturingAppender appender = new CapturingAppender(logged);
    Logger.getRootLogger().addAppender(appender);
    try {
      StreamingClientProperties.from(config);
    } finally {
      Logger.getRootLogger().removeAppender(appender);
    }

    assertThat(logged).anyMatch(m -> m.contains("max_append_request_buffer_duration_ms"));
    assertThat(logged).noneMatch(m -> m.contains("4321"));
  }

  private static class CapturingAppender extends AppenderSkeleton {
    private final List<String> messages;

    CapturingAppender(List<String> messages) {
      this.messages = messages;
    }

    @Override
    protected void append(LoggingEvent event) {
      messages.add(event.getRenderedMessage());
    }

    @Override
    public void close() {}

    @Override
    public boolean requiresLayout() {
      return false;
    }
  }
}
