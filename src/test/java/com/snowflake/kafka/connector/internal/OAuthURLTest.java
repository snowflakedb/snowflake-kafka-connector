package com.snowflake.kafka.connector.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import com.google.common.collect.ImmutableList;
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
        Arguments.of("localhost", ImmutableList.of("https", "localhost:443", "", true)));
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
