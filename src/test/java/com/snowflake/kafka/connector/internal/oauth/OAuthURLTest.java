package com.snowflake.kafka.connector.internal.oauth;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import com.google.common.collect.ImmutableList;
import com.snowflake.kafka.connector.internal.SnowflakeKafkaConnectorException;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class OAuthURLTest {

  static Stream<Arguments> correctUrls() {
    return Stream.of(
        Arguments.of(
            "https://localhost:8085/push/token",
            ImmutableList.of("https", "localhost:8085", "/push/token", true)),
        Arguments.of("localhost:8085", ImmutableList.of("https", "localhost:8085", "", true)),
        Arguments.of(
            "http://localhost:8085", ImmutableList.of("http", "localhost:8085", "", false)),
        Arguments.of("localhost", ImmutableList.of("https", "localhost:443", "", true)),
        Arguments.of(
            "https://login.test.com/xxxxxxx/oauth2/v2.0/token",
            ImmutableList.of("https", "login.test.com:443", "/xxxxxxx/oauth2/v2.0/token", true)),
        Arguments.of(
            "https://example.com/my-api/v2.0/get-token",
            ImmutableList.of("https", "example.com:443", "/my-api/v2.0/get-token", true)),
        // Path is case-sensitive (RFC 3986) and must be preserved verbatim.
        Arguments.of(
            "https://login.test.com/oauth2/v2.0/Token",
            ImmutableList.of("https", "login.test.com:443", "/oauth2/v2.0/Token", true)),
        // Scheme and host are case-insensitive and normalized to lowercase; path case preserved.
        Arguments.of(
            "HTTPS://Login.Test.COM/oauth2/v2.0/Token",
            ImmutableList.of("https", "login.test.com:443", "/oauth2/v2.0/Token", true)),
        Arguments.of(
            "HTTP://Login.Test.COM:8085/Push/Token",
            ImmutableList.of("http", "login.test.com:8085", "/Push/Token", false)));
  }

  @ParameterizedTest(name = "url: {0}, parsed: {1}")
  @MethodSource("correctUrls")
  void shouldParseUrlCorrectly(String url, List<String> parts) {
    assertThat(OAuthURL.from(url))
        .extracting(
            OAuthURL::getScheme, OAuthURL::hostWithPort, OAuthURL::path, OAuthURL::sslEnabled)
        .doesNotContainNull()
        .containsExactlyElementsOf(parts);
  }

  @Test
  void shouldNotParseIncorrectUrls() {
    assertThatExceptionOfType(SnowflakeKafkaConnectorException.class)
        .isThrownBy(() -> OAuthURL.from("wrong url"));
  }
}
