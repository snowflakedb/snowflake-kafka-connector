package com.snowflake.kafka.connector.internal.streaming;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.snowflake.kafka.connector.internal.SnowflakeKafkaConnectorException;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class StreamingClientOverridesTest {

  /**
   * SDK parameters that select the endpoint, transport, identity, credentials, or request headers.
   * None of them may ever be accepted from connector configuration.
   */
  private static final Set<String> SECURITY_SENSITIVE_SDK_KEYS =
      Set.of(
          "scheme",
          "host",
          "port",
          "url",
          "ssl",
          "account",
          "user",
          "role",
          "authorization_type",
          "private_key",
          "private_key_file",
          "private_key_passphrase",
          "oauth_client_id",
          "oauth_client_secret",
          "oauth_refresh_token",
          "oauth_token_endpoint",
          "oauth_scope",
          "oauth_include_scope",
          "oauth_access_token_default_expires_in_seconds",
          "auth_refresh_buffer_duration_seconds",
          "personal_access_token",
          "spcs_token_path",
          "spcs_service_token_path",
          "replace_underscore_with_dash_in_host",
          "encode_streaming_url_components",
          "file_upload_endpoint",
          "data_header_overrides",
          "application",
          "https_proxy_url",
          "proxy_user",
          "proxy_password",
          "no_proxy");

  @Test
  void allowlistExcludesSecuritySensitiveSdkKeys() {
    assertThat(StreamingClientOverrides.ALLOWED_KEYS)
        .doesNotContainAnyElementsOf(SECURITY_SENSITIVE_SDK_KEYS);
  }

  @Test
  void allowlistKeysAreLowerCase() {
    assertThat(StreamingClientOverrides.ALLOWED_KEYS).allMatch(k -> k.equals(k.toLowerCase()));
  }

  @Test
  void perChannelInflightCapsAreAllowed() {
    assertThat(
            StreamingClientOverrides.parse(
                "max_inflight_rows_per_channel:1000,max_inflight_megabytes_per_channel:64"))
        .isEqualTo(
            Map.of(
                "max_inflight_rows_per_channel",
                "1000",
                "max_inflight_megabytes_per_channel",
                "64"));
  }

  @Test
  void emptyOrNullMapYieldsNoOverrides() {
    assertThat(StreamingClientOverrides.parse(null)).isEmpty();
    assertThat(StreamingClientOverrides.parse("")).isEmpty();
  }

  @Test
  void allowedKeysAreTrimmedAndLowerCased() {
    assertThat(
            StreamingClientOverrides.parse(
                " MAX_APPEND_REQUEST_BUFFER_DURATION_MS : 250 , Data_Http_Timeout_Seconds:30"))
        .isEqualTo(
            Map.of(
                "max_append_request_buffer_duration_ms", "250", "data_http_timeout_seconds", "30"));
  }

  @Test
  void everySecuritySensitiveKeyIsRejected() {
    for (String key : SECURITY_SENSITIVE_SDK_KEYS) {
      assertThatThrownBy(() -> StreamingClientOverrides.parse(key + ":x"))
          .as(key)
          .isInstanceOf(SnowflakeKafkaConnectorException.class)
          .hasMessageContaining("Unsupported key '" + key + "'");
      assertThatThrownBy(() -> StreamingClientOverrides.parse(" " + key.toUpperCase() + " :x"))
          .as(key)
          .isInstanceOf(SnowflakeKafkaConnectorException.class)
          .hasMessageContaining("Unsupported key");
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"a:b,c:d", "host", "host:", ":x", "host:a:b"})
  void malformedOrUnknownEntriesAreRejected(String overrideMap) {
    assertThatThrownBy(() -> StreamingClientOverrides.parse(overrideMap))
        .isInstanceOf(SnowflakeKafkaConnectorException.class);
  }

  @Test
  void rejectedKeyIsEscapedInErrorMessage() {
    assertThatThrownBy(() -> StreamingClientOverrides.parse("host\r\nINFO injected:x"))
        .isInstanceOf(SnowflakeKafkaConnectorException.class)
        .hasMessageContaining("hostU+000DU+000AINFO injected")
        .hasMessageNotContaining("\nINFO injected");
  }

  @ParameterizedTest
  @ValueSource(
      strings = {"1\nINFO injected", "1\rINFO injected", "1\u2028INFO injected", "1\u0000x"})
  void valuesWithControlCharactersAreRejected(String value) {
    assertThatThrownBy(
            () -> StreamingClientOverrides.parse("max_append_request_buffer_duration_ms:" + value))
        .isInstanceOf(SnowflakeKafkaConnectorException.class)
        .hasMessageContaining("control characters")
        .hasMessageNotContaining("INFO injected");
  }

  @Test
  void escapeControlCharactersLeavesPrintableTextUntouched() {
    assertThat(StreamingClientOverrides.escapeControlCharacters("a-b_c.d e"))
        .isEqualTo("a-b_c.d e");
    assertThat(StreamingClientOverrides.escapeControlCharacters("a\tb\u2029"))
        .isEqualTo("aU+0009bU+2029");
  }
}
