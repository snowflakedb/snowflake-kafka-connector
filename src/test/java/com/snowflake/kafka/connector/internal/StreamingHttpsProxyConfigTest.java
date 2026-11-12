/*
 * Copyright (c) 2026 Snowflake Inc. All rights reserved.
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

import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.HTTPS_PROXY_HOST;
import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.HTTPS_PROXY_PASSWORD;
import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.HTTPS_PROXY_PORT;
import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.HTTPS_PROXY_USER;
import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.HTTP_NON_PROXY_HOSTS;
import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.HTTP_PROXY_HOST;
import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.HTTP_PROXY_PASSWORD;
import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.HTTP_PROXY_PORT;
import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.HTTP_PROXY_USER;
import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.SNOWFLAKE_HTTPS_PROXY_HOST;
import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.SNOWFLAKE_HTTPS_PROXY_PASSWORD;
import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.SNOWFLAKE_HTTPS_PROXY_PORT;
import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.SNOWFLAKE_HTTPS_PROXY_USERNAME;
import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.SNOWFLAKE_PROXY_BYPASS_HOSTS;
import static com.snowflake.kafka.connector.internal.TestUtils.generatePrivateKey;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.config.SinkTaskConfig;
import com.snowflake.kafka.connector.config.SnowflakeSinkConnectorConfigBuilder;
import com.snowflake.kafka.connector.internal.streaming.StreamingClientProperties;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.Test;

public class StreamingHttpsProxyConfigTest {

  @Test
  void connectorProxyIsPassedToSdkAndJdbcWithoutWritingJvmProperties() {
    String previousHost = System.getProperty(HTTPS_PROXY_HOST);
    System.clearProperty(HTTPS_PROXY_HOST);
    try {
      Map<String, String> config = baseConfig();
      config.put(SNOWFLAKE_HTTPS_PROXY_HOST, "proxy.example.com");
      config.put(SNOWFLAKE_HTTPS_PROXY_PORT, "3128");
      config.put(SNOWFLAKE_PROXY_BYPASS_HOSTS, "localhost|*.internal.example.com");
      config.put(SNOWFLAKE_HTTPS_PROXY_USERNAME, "proxy-user");
      config.put(SNOWFLAKE_HTTPS_PROXY_PASSWORD, "proxy-secret-value");
      assertThat(Utils.validateProxySettings(config)).isEmpty();

      SinkTaskConfig parsed = SinkTaskConfig.from(config);
      Properties sdk = StreamingClientProperties.from(parsed).clientProperties;
      Properties jdbc = InternalUtils.generateProxyParametersIfRequired(parsed);

      assertThat(sdk)
          .containsEntry("https_proxy_url", "http://proxy.example.com:3128")
          .containsEntry("no_proxy", "localhost|*.internal.example.com")
          .containsEntry("proxy_user", "proxy-user")
          .containsEntry("proxy_password", "proxy-secret-value");
      assertThat(jdbc.getProperty(JdbcPropertyKeys.USE_PROXY)).isEqualTo("true");
      assertThat(jdbc.getProperty(JdbcPropertyKeys.PROXY_HOST)).isEqualTo("proxy.example.com");
      assertThat(jdbc.getProperty(JdbcPropertyKeys.PROXY_PORT)).isEqualTo("3128");
      assertThat(jdbc.getProperty(JdbcPropertyKeys.NON_PROXY_HOSTS))
          .isEqualTo("localhost|*.internal.example.com");
      assertThat(jdbc.getProperty(JdbcPropertyKeys.PROXY_USER)).isEqualTo("proxy-user");
      assertThat(jdbc.getProperty(JdbcPropertyKeys.PROXY_PASSWORD)).isEqualTo("proxy-secret-value");
      assertThat(System.getProperty(HTTPS_PROXY_HOST)).isNull();
    } finally {
      restoreProperty(HTTPS_PROXY_HOST, previousHost);
    }
  }

  @Test
  void jvmHttpsProxyHostWithoutPortUsesTheJdkDefaultPort() {
    withJvmProxyProperties(
        Map.of(
            HTTPS_PROXY_HOST, "https-proxy.example.com",
            HTTPS_PROXY_PORT, "",
            HTTP_PROXY_HOST, "http-proxy.example.com",
            HTTP_PROXY_PORT, "8080",
            HTTP_NON_PROXY_HOSTS, "*.snowflakecomputing.com"),
        () -> {
          Properties sdk =
              StreamingClientProperties.from(SinkTaskConfig.from(baseConfig())).clientProperties;

          // The JDK sends https traffic through https.proxyHost on port 443 when no port is set,
          // so the SDK must not skip it and fall through to http.proxyHost.
          assertThat(sdk)
              .containsEntry("https_proxy_url", "http://https-proxy.example.com:443")
              .containsEntry("no_proxy", "*.snowflakecomputing.com")
              .doesNotContainKey("proxy_user");
        });
  }

  @Test
  void jvmHttpProxyAloneIsIgnoredBecauseAllSnowflakeTrafficIsHttps() {
    // The JDK applies http.proxyHost only to http:// URLs, so JDBC and the OAuth fetch go direct
    // for Snowflake. The SDK must not use a proxy the other clients do not.
    withJvmProxyProperties(
        Map.of(
            HTTP_PROXY_HOST, "http-proxy.example.com",
            HTTP_PROXY_PORT, "8080",
            HTTP_NON_PROXY_HOSTS, "localhost"),
        () -> {
          SinkTaskConfig parsed = SinkTaskConfig.from(baseConfig());
          Properties sdk = StreamingClientProperties.from(parsed).clientProperties;

          assertThat(sdk)
              .doesNotContainKey("https_proxy_url")
              .doesNotContainKey("no_proxy")
              .doesNotContainKey("proxy_user");
          assertThat(InternalUtils.generateProxyParametersIfRequired(parsed)).isEmpty();
        });
  }

  @Test
  void invalidJvmProxyPortIsRejectedNotIgnored() {
    withJvmProxyProperties(
        Map.of(HTTPS_PROXY_HOST, "https-proxy.example.com", HTTPS_PROXY_PORT, "abc"),
        () ->
            assertThatThrownBy(
                    () -> StreamingClientProperties.from(SinkTaskConfig.from(baseConfig())))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid JVM proxy settings")
                .hasMessageContaining(HTTPS_PROXY_PORT)
                .hasMessageContaining("'abc'"));
  }

  @Test
  void jvmBypassListOutsideTheJdkFormatIsRejectedNotIgnored() {
    withJvmProxyProperties(
        Map.of(
            HTTPS_PROXY_HOST, "https-proxy.example.com",
            HTTPS_PROXY_PORT, "3128",
            HTTP_NON_PROXY_HOSTS, "localhost|10.0.0.0/8"),
        () ->
            assertThatThrownBy(
                    () -> StreamingClientProperties.from(SinkTaskConfig.from(baseConfig())))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(HTTP_NON_PROXY_HOSTS)
                .hasMessageContaining("'10.0.0.0/8'"));
  }

  @Test
  void connectorProxyWinsOverJvmSystemProperties() {
    String previousHost = System.getProperty(HTTPS_PROXY_HOST);
    String previousPort = System.getProperty(HTTPS_PROXY_PORT);
    String previousBypass = System.getProperty(HTTP_NON_PROXY_HOSTS);
    System.setProperty(HTTPS_PROXY_HOST, "jvm-proxy.example.com");
    System.setProperty(HTTPS_PROXY_PORT, "8443");
    System.setProperty(HTTP_NON_PROXY_HOSTS, "*.jvm-only.example.com");
    try {
      Map<String, String> config = baseConfig();
      config.put(SNOWFLAKE_HTTPS_PROXY_HOST, "proxy.example.com");
      config.put(SNOWFLAKE_HTTPS_PROXY_PORT, "3128");
      Properties sdk = StreamingClientProperties.from(SinkTaskConfig.from(config)).clientProperties;

      assertThat(sdk)
          .containsEntry("https_proxy_url", "http://proxy.example.com:3128")
          .doesNotContainKey("no_proxy");
    } finally {
      restoreProperty(HTTPS_PROXY_HOST, previousHost);
      restoreProperty(HTTPS_PROXY_PORT, previousPort);
      restoreProperty(HTTP_NON_PROXY_HOSTS, previousBypass);
    }
  }

  @Test
  void unsetProxyLeavesSdkAndJdbcAlone() {
    String previousHost = System.getProperty(HTTPS_PROXY_HOST);
    String previousPort = System.getProperty(HTTPS_PROXY_PORT);
    String previousHttpHost = System.getProperty(HTTP_PROXY_HOST);
    String previousHttpPort = System.getProperty(HTTP_PROXY_PORT);
    System.clearProperty(HTTPS_PROXY_HOST);
    System.clearProperty(HTTPS_PROXY_PORT);
    System.clearProperty(HTTP_PROXY_HOST);
    System.clearProperty(HTTP_PROXY_PORT);
    try {
      SinkTaskConfig parsed = SinkTaskConfig.from(baseConfig());
      Properties sdk = StreamingClientProperties.from(parsed).clientProperties;

      assertThat(sdk).doesNotContainKey("https_proxy_url").doesNotContainKey("no_proxy");
      assertThat(InternalUtils.generateProxyParametersIfRequired(parsed)).isEmpty();
    } finally {
      restoreProperty(HTTPS_PROXY_HOST, previousHost);
      restoreProperty(HTTPS_PROXY_PORT, previousPort);
      restoreProperty(HTTP_PROXY_HOST, previousHttpHost);
      restoreProperty(HTTP_PROXY_PORT, previousHttpPort);
    }
  }

  @Test
  void missingPortIsRejectedNotSkipped() {
    Map<String, String> config = baseConfig();
    config.put(SNOWFLAKE_HTTPS_PROXY_HOST, "proxy.example.com");

    assertThat(Utils.validateProxySettings(config))
        .containsKeys(SNOWFLAKE_HTTPS_PROXY_HOST, SNOWFLAKE_HTTPS_PROXY_PORT);
    assertThatThrownBy(() -> SinkTaskConfig.from(config))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(SNOWFLAKE_HTTPS_PROXY_PORT);
  }

  @Test
  void blankPortIsRejectedNotSkipped() {
    Map<String, String> config = baseConfig();
    config.put(SNOWFLAKE_HTTPS_PROXY_HOST, "proxy.example.com");
    config.put(SNOWFLAKE_HTTPS_PROXY_PORT, " ");

    assertThat(Utils.validateProxySettings(config))
        .containsKeys(SNOWFLAKE_HTTPS_PROXY_HOST, SNOWFLAKE_HTTPS_PROXY_PORT);
    assertThatThrownBy(() -> SinkTaskConfig.from(config))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void nonProxyHostsOutsideTheJdkFormatAreRejected() {
    Map<String, String> config = baseConfig();
    config.put(SNOWFLAKE_HTTPS_PROXY_HOST, "proxy.example.com");
    config.put(SNOWFLAKE_HTTPS_PROXY_PORT, "3128");
    config.put(SNOWFLAKE_PROXY_BYPASS_HOSTS, "localhost,10.0.0.0/8");

    assertThat(Utils.validateProxySettings(config)).containsOnlyKeys(SNOWFLAKE_PROXY_BYPASS_HOSTS);
    assertThatThrownBy(() -> SinkTaskConfig.from(config))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(SNOWFLAKE_PROXY_BYPASS_HOSTS);
  }

  /**
   * Runs {@code body} with exactly these JVM proxy system properties set (the other proxy
   * properties are cleared), then restores whatever was there before.
   */
  private static void withJvmProxyProperties(Map<String, String> properties, Runnable body) {
    String[] keys = {
      HTTPS_PROXY_HOST,
      HTTPS_PROXY_PORT,
      HTTP_PROXY_HOST,
      HTTP_PROXY_PORT,
      HTTP_NON_PROXY_HOSTS,
      HTTPS_PROXY_USER,
      HTTPS_PROXY_PASSWORD,
      HTTP_PROXY_USER,
      HTTP_PROXY_PASSWORD
    };
    Map<String, String> previous = new HashMap<>();
    for (String key : keys) {
      previous.put(key, System.getProperty(key));
      System.clearProperty(key);
    }
    try {
      properties.forEach(System::setProperty);
      body.run();
    } finally {
      previous.forEach(StreamingHttpsProxyConfigTest::restoreProperty);
    }
  }

  private static Map<String, String> baseConfig() {
    Map<String, String> config =
        new HashMap<>(
            SnowflakeSinkConnectorConfigBuilder.streamingConfig()
                .withPrivateKey(
                    Base64.getEncoder().encodeToString(generatePrivateKey().getEncoded()))
                .build());
    config.put(Utils.TASK_ID, "0");
    return config;
  }

  private static void restoreProperty(String key, String previous) {
    if (previous == null) {
      System.clearProperty(key);
    } else {
      System.setProperty(key, previous);
    }
  }
}
