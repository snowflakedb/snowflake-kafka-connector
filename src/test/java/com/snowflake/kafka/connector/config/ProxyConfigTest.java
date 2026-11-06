package com.snowflake.kafka.connector.config;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class ProxyConfigTest {

  @Test
  void configuredOnlyWhenHostAndPortAreSet() {
    assertThat(ProxyConfig.none().isConfigured()).isFalse();
    assertThat(ProxyConfig.create("proxy.example.com", " ", null, null, null).isConfigured())
        .isFalse();
    assertThat(ProxyConfig.create("proxy.example.com", "3128", null, null, null).isConfigured())
        .isTrue();
  }

  @Test
  void credentialsRequireUserAndPassword() {
    assertThat(ProxyConfig.create("h", "1", null, "user", null).hasCredentials()).isFalse();
    assertThat(ProxyConfig.create("h", "1", null, "user", "secret").hasCredentials()).isTrue();
  }

  @Test
  void toStringMasksPassword() {
    assertThat(ProxyConfig.create("h", "1", "localhost", "user", "secret-value").toString())
        .contains("user")
        .doesNotContain("secret-value");
  }
}
