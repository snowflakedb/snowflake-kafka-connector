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
import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.HTTPS_PROXY_PORT;
import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.HTTP_NON_PROXY_HOSTS;
import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.HTTP_PROXY_HOST;
import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.HTTP_PROXY_PORT;
import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.JVM_NON_PROXY_HOSTS;
import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.JVM_PROXY_HOST;
import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.JVM_PROXY_PASSWORD;
import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.JVM_PROXY_PORT;
import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.JVM_PROXY_USERNAME;
import static com.snowflake.kafka.connector.internal.TestUtils.generatePrivateKey;
import static org.assertj.core.api.Assertions.assertThat;

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
      config.put(JVM_PROXY_HOST, "proxy.example.com");
      config.put(JVM_PROXY_PORT, "3128");
      config.put(JVM_NON_PROXY_HOSTS, "localhost|*.internal.example.com");
      config.put(JVM_PROXY_USERNAME, "proxy-user");
      config.put(JVM_PROXY_PASSWORD, "proxy-secret-value");
      assertThat(Utils.validateProxySettings(config)).isEmpty();

      SinkTaskConfig parsed = SinkTaskConfig.from(config);
      Properties sdk = StreamingClientProperties.from(parsed).clientProperties;
      Properties jdbc = InternalUtils.generateProxyParametersIfRequired(parsed);

      assertThat(sdk)
          .containsEntry("https_proxy_url", "http://proxy.example.com:3128")
          .containsEntry("no_proxy", "localhost,.internal.example.com")
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
  void jvmSystemPropertiesAreASeparateSdkTier() {
    String previousHttpsHost = System.getProperty(HTTPS_PROXY_HOST);
    String previousHttpsPort = System.getProperty(HTTPS_PROXY_PORT);
    String previousHttpHost = System.getProperty(HTTP_PROXY_HOST);
    String previousHttpPort = System.getProperty(HTTP_PROXY_PORT);
    String previousBypass = System.getProperty(HTTP_NON_PROXY_HOSTS);
    System.setProperty(HTTPS_PROXY_HOST, "https-proxy.example.com");
    System.setProperty(HTTPS_PROXY_PORT, "");
    System.setProperty(HTTP_PROXY_HOST, "http-proxy.example.com");
    System.setProperty(HTTP_PROXY_PORT, "8080");
    System.setProperty(HTTP_NON_PROXY_HOSTS, "*.snowflakecomputing.com");
    try {
      Properties sdk =
          StreamingClientProperties.from(SinkTaskConfig.from(baseConfig())).clientProperties;

      assertThat(sdk)
          .containsEntry("jvm_https_proxy_url", "http://http-proxy.example.com:8080")
          .containsEntry("jvm_no_proxy", ".snowflakecomputing.com")
          .doesNotContainKey("https_proxy_url")
          .doesNotContainKey("jvm_proxy_user");
    } finally {
      restoreProperty(HTTPS_PROXY_HOST, previousHttpsHost);
      restoreProperty(HTTPS_PROXY_PORT, previousHttpsPort);
      restoreProperty(HTTP_PROXY_HOST, previousHttpHost);
      restoreProperty(HTTP_PROXY_PORT, previousHttpPort);
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

      assertThat(sdk).doesNotContainKey("https_proxy_url").doesNotContainKey("jvm_https_proxy_url");
      assertThat(InternalUtils.generateProxyParametersIfRequired(parsed)).isEmpty();
    } finally {
      restoreProperty(HTTPS_PROXY_HOST, previousHost);
      restoreProperty(HTTPS_PROXY_PORT, previousPort);
      restoreProperty(HTTP_PROXY_HOST, previousHttpHost);
      restoreProperty(HTTP_PROXY_PORT, previousHttpPort);
    }
  }

  @Test
  void missingPortSkipsTheConnectorTier() {
    Map<String, String> config = baseConfig();
    config.put(JVM_PROXY_HOST, "proxy.example.com");

    assertThat(Utils.validateProxySettings(config)).containsKey(JVM_PROXY_HOST);
    Properties sdk = StreamingClientProperties.from(SinkTaskConfig.from(config)).clientProperties;
    assertThat(sdk).doesNotContainKey("https_proxy_url");
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
