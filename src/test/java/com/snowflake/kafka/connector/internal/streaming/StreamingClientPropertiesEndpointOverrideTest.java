package com.snowflake.kafka.connector.internal.streaming;

import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.SNOWFLAKE_STREAMING_CLIENT_PROVIDER_OVERRIDE_MAP;
import static com.snowflake.kafka.connector.internal.TestUtils.generatePrivateKey;
import static com.snowflake.kafka.connector.internal.streaming.StreamingClientProperties.TEST_ENDPOINT_OVERRIDE_ENV;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams;
import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.config.SinkTaskConfig;
import com.snowflake.kafka.connector.config.SnowflakeSinkConnectorConfigBuilder;
import com.snowflake.kafka.connector.internal.SnowflakeKafkaConnectorException;
import java.util.Base64;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class StreamingClientPropertiesEndpointOverrideTest {

  private static SinkTaskConfig config(String overrideMap) {
    Map<String, String> connectorConfig =
        SnowflakeSinkConnectorConfigBuilder.streamingConfig().build();
    connectorConfig.put(Utils.TASK_ID, "0");
    connectorConfig.put(
        KafkaConnectorConfigParams.SNOWFLAKE_PRIVATE_KEY,
        Base64.getEncoder().encodeToString(generatePrivateKey().getEncoded()));
    connectorConfig.put(SNOWFLAKE_STREAMING_CLIENT_PROVIDER_OVERRIDE_MAP, overrideMap);
    return SinkTaskConfig.from(connectorConfig);
  }

  private static StreamingClientProperties fromEnv(SinkTaskConfig config, String endpoint) {
    return StreamingClientProperties.from(
        config, name -> TEST_ENDPOINT_OVERRIDE_ENV.equals(name) ? endpoint : null);
  }

  @Test
  void shouldRedirectEndpointFromWorkerEnvironment() {
    StreamingClientProperties result = fromEnv(config(""), "https://mitmproxy:8080");

    assertThat(result.clientProperties)
        .containsEntry("scheme", "https")
        .containsEntry("host", "mitmproxy")
        .containsEntry("port", "8080");
    assertThat(result.parameterOverrides).isEmpty();
  }

  @Test
  void shouldAcceptPlainHttpEndpointWithTrailingSlash() {
    StreamingClientProperties result = fromEnv(config(""), "http://host.docker.internal:9999/");

    assertThat(result.clientProperties)
        .containsEntry("scheme", "http")
        .containsEntry("host", "host.docker.internal")
        .containsEntry("port", "9999");
  }

  @ParameterizedTest
  @ValueSource(strings = {"", "   "})
  void shouldKeepSnowflakeEndpointWhenEnvironmentIsBlank(String endpoint) {
    SinkTaskConfig config = config("");

    StreamingClientProperties redirected = fromEnv(config, endpoint);
    StreamingClientProperties unset = fromEnv(config, null);

    assertThat(redirected.clientProperties).doesNotContainKeys("scheme", "port");
    assertThat(redirected).isEqualTo(unset);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "mitmproxy:8080",
        "ftp://mitmproxy:8080",
        "https://mitmproxy",
        "https://:8080",
        "https://user:secret@mitmproxy:8080",
        "https://mitmproxy:8080/path",
        "https://mitmproxy:8080?q=1",
        "https://mitmproxy:8080#f",
        "https://mitmproxy:99999",
        "https://mitm proxy:8080"
      })
  void shouldFailFastOnMalformedEndpoint(String endpoint) {
    SinkTaskConfig config = config("");

    assertThatThrownBy(() -> fromEnv(config, endpoint))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(TEST_ENDPOINT_OVERRIDE_ENV)
        .hasMessageNotContaining("secret");
  }

  @Test
  void shouldStillRejectEndpointKeysInOverrideMapWhenEnvironmentIsSet() {
    SinkTaskConfig config = config("host:other.example.com");

    assertThatThrownBy(() -> fromEnv(config, "https://mitmproxy:8080"))
        .isInstanceOf(SnowflakeKafkaConnectorException.class)
        .hasMessageContaining("Unsupported key");
  }
}
