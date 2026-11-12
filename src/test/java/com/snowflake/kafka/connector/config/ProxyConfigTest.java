package com.snowflake.kafka.connector.config;

import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.SNOWFLAKE_HTTPS_PROXY_HOST;
import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.SNOWFLAKE_HTTPS_PROXY_PASSWORD;
import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.SNOWFLAKE_HTTPS_PROXY_PORT;
import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.SNOWFLAKE_HTTPS_PROXY_USERNAME;
import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.SNOWFLAKE_PROXY_BYPASS_HOSTS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

class ProxyConfigTest {

  @Test
  void noneIsNotConfiguredAndHasNoCredentials() {
    assertThat(ProxyConfig.none().isConfigured()).isFalse();
    assertThat(ProxyConfig.none().hasCredentials()).isFalse();
  }

  @Test
  void blankValuesCountAsUnset() {
    ProxyConfig proxy = ProxyConfig.create(" ", "", "  ", "", " ");

    assertThat(proxy.isConfigured()).isFalse();
    assertThat(proxy.hasCredentials()).isFalse();
    assertThat(proxy.getHost()).isNull();
    assertThat(proxy.getPort()).isNull();
    assertThat(proxy.getNonProxyHosts()).isNull();
  }

  @Test
  void hostPortAndBypassListAreTrimmed() {
    ProxyConfig proxy =
        ProxyConfig.create(
            " proxy.example.com ", " 3128 ", " localhost|*.example.com ", "user", "secret");

    assertThat(proxy.isConfigured()).isTrue();
    assertThat(proxy.getHost()).isEqualTo("proxy.example.com");
    assertThat(proxy.getPort()).isEqualTo("3128");
    assertThat(proxy.getNonProxyHosts()).isEqualTo("localhost|*.example.com");
    assertThat(proxy.hasCredentials()).isTrue();
  }

  @Test
  void hostWithoutPortIsRejectedNotDropped() {
    assertThatThrownBy(() -> ProxyConfig.create("proxy.example.com", null, null, null, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(SNOWFLAKE_HTTPS_PROXY_HOST)
        .hasMessageContaining(SNOWFLAKE_HTTPS_PROXY_PORT)
        .hasMessageContaining("must be provided together");
  }

  @Test
  void portWithoutHostIsRejected() {
    assertThatThrownBy(() -> ProxyConfig.create(null, "3128", null, null, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("must be provided together");
  }

  @Test
  void aBlankPortIsRejectedNotDropped() {
    // A whitespace-only port used to pass connector validation and then be ignored silently.
    assertThatThrownBy(() -> ProxyConfig.create("proxy.example.com", " ", null, null, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(SNOWFLAKE_HTTPS_PROXY_PORT);
    assertThat(ProxyConfig.validate("proxy.example.com", " ", null, null, null))
        .containsKeys(SNOWFLAKE_HTTPS_PROXY_HOST, SNOWFLAKE_HTTPS_PROXY_PORT);
  }

  @Test
  void usernameWithoutPasswordAndPasswordWithoutUsernameAreRejected() {
    assertThatThrownBy(() -> ProxyConfig.create("h.example.com", "1", null, "user", null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(SNOWFLAKE_HTTPS_PROXY_USERNAME);
    assertThatThrownBy(() -> ProxyConfig.create("h.example.com", "1", null, null, "secret"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(SNOWFLAKE_HTTPS_PROXY_PASSWORD);
    assertThat(ProxyConfig.create("h.example.com", "1", null, "user", "secret").hasCredentials())
        .isTrue();
  }

  @ParameterizedTest
  @ValueSource(strings = {"abc", "3128.5", "0", "-1", "65536", "70000"})
  void aPortOutsideTheValidRangeIsRejected(String port) {
    assertThat(ProxyConfig.validate("proxy.example.com", port, null, null, null))
        .containsOnlyKeys(SNOWFLAKE_HTTPS_PROXY_PORT)
        .extractingByKey(SNOWFLAKE_HTTPS_PROXY_PORT)
        .asString()
        .contains("proxy port must be an integer between 1 and 65535")
        .contains("'" + port + "'");
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "bad host",
        "http://proxy.example.com",
        "proxy.example.com/",
        "proxy.example.com/path",
        "proxy.example.com?x=1",
        "proxy.example.com#frag",
        "user@proxy.example.com"
      })
  void aMalformedHostIsRejected(String host) {
    assertThat(ProxyConfig.validate(host, "3128", null, null, null))
        .containsOnlyKeys(SNOWFLAKE_HTTPS_PROXY_HOST)
        .extractingByKey(SNOWFLAKE_HTTPS_PROXY_HOST)
        .asString()
        .contains("proxy host is not valid");
  }

  @Test
  void everyProblemIsReportedAgainstItsOwnKey() {
    Map<String, String> errors =
        ProxyConfig.validate("bad host", "abc", "10.0.0.0/8", "user", null);

    assertThat(errors)
        .containsOnlyKeys(
            SNOWFLAKE_HTTPS_PROXY_HOST,
            SNOWFLAKE_HTTPS_PROXY_PORT,
            SNOWFLAKE_PROXY_BYPASS_HOSTS,
            SNOWFLAKE_HTTPS_PROXY_USERNAME,
            SNOWFLAKE_HTTPS_PROXY_PASSWORD);
  }

  @Test
  void noSettingsAreValid() {
    assertThat(ProxyConfig.validate(null, null, null, null, null)).isEmpty();
    assertThat(ProxyConfig.validate("", "", "", "", "")).isEmpty();
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "localhost",
        "localhost|*.internal.example.com",
        "127.*|10.0.0.1|[::1]|*",
        "Host.Example.com|web*proxy|my_host-1.example.com",
        "a||b",
        " localhost | 127.* "
      })
  void aJdkFormatBypassListIsValid(String list) {
    assertThat(ProxyConfig.invalidBypassHostsEntry(list)).isNull();
    assertThat(ProxyConfig.validate("p.example.com", "3128", list, null, null)).isEmpty();
  }

  @ParameterizedTest
  @CsvSource(
      delimiter = ';',
      value = {
        "localhost,127.0.0.1;localhost,127.0.0.1",
        "ok|10.0.0.0/8;10.0.0.0/8",
        "ok|not valid;not valid",
        "ok|http://x.example.com;http://x.example.com",
        "ok|a@b;a@b"
      })
  void aBypassEntryOutsideTheJdkFormatIsRejected(String list, String badEntry) {
    assertThat(ProxyConfig.invalidBypassHostsEntry(list)).isEqualTo(badEntry);
    assertThat(ProxyConfig.validate("p.example.com", "3128", list, null, null))
        .containsOnlyKeys(SNOWFLAKE_PROXY_BYPASS_HOSTS)
        .extractingByKey(SNOWFLAKE_PROXY_BYPASS_HOSTS)
        .asString()
        .contains("'" + badEntry + "'")
        .contains("http.nonProxyHosts");
  }

  @Test
  void toStringMasksPassword() {
    assertThat(
            ProxyConfig.create("h.example.com", "1", "localhost", "user", "secret-value")
                .toString())
        .contains("user")
        .doesNotContain("secret-value");
  }

  @ParameterizedTest
  @CsvSource({
    "proxy.example.com, 3128, http://proxy.example.com:3128",
    "'  proxy.example.com  ', ' 3128 ', http://proxy.example.com:3128",
    "10.1.2.3, 8080, http://10.1.2.3:8080",
    "::1, 3128, http://[::1]:3128",
    "[::1], 3128, http://[::1]:3128",
    "2001:db8::1, 443, http://[2001:db8::1]:443",
    "proxy_host.example.com, 3128, http://proxy_host.example.com:3128",
    "proxy.example.com, 1, http://proxy.example.com:1",
    "proxy.example.com, 65535, http://proxy.example.com:65535",
  })
  void toUrlBuildsTheProxyUrl(String host, String port, String expected) {
    assertThat(ProxyConfig.toUrl(host, port)).isEqualTo(expected);
  }

  @Test
  void toUrlIsNullWhenHostOrPortIsMissing() {
    assertThat(ProxyConfig.toUrl(null, "3128")).isNull();
    assertThat(ProxyConfig.toUrl("proxy.example.com", null)).isNull();
    assertThat(ProxyConfig.toUrl("", "3128")).isNull();
    assertThat(ProxyConfig.toUrl("proxy.example.com", "  ")).isNull();
  }

  @ParameterizedTest
  @ValueSource(strings = {"abc", "3128.5", "0", "-1", "65536", "70000"})
  void toUrlRejectsAPortThatIsNotBetween1And65535(String port) {
    assertThatThrownBy(() -> ProxyConfig.toUrl("proxy.example.com", port))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("proxy port must be an integer between 1 and 65535")
        .hasMessageContaining("'" + port + "'");
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "bad host",
        "http://proxy.example.com",
        "proxy.example.com/",
        "proxy.example.com/path",
        "proxy.example.com?x=1",
        "proxy.example.com#frag",
        "user@proxy.example.com"
      })
  void toUrlRejectsAMalformedHost(String host) {
    assertThatThrownBy(() -> ProxyConfig.toUrl(host, "3128"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("proxy host is not valid");
  }
}
