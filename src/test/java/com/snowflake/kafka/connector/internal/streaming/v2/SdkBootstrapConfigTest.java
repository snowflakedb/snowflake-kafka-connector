package com.snowflake.kafka.connector.internal.streaming.v2;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Optional;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

class SdkBootstrapConfigTest {

  @Test
  void shouldSetDefaultLogLevelWhenBothNull() {
    assertThat(SdkBootstrapConfig.shouldSetDefaultLogLevel(null, null)).isTrue();
  }

  @Test
  void shouldNotSetWhenSyspropPresent() {
    assertThat(SdkBootstrapConfig.shouldSetDefaultLogLevel("info", null)).isFalse();
  }

  @Test
  void shouldNotSetWhenEnvPresent() {
    assertThat(SdkBootstrapConfig.shouldSetDefaultLogLevel(null, "debug")).isFalse();
  }

  @Test
  void validatePrometheusAcceptsValid() {
    // Should not throw
    SdkBootstrapConfig.validatePrometheus(50000, "0.0.0.0");
  }

  @Test
  void validatePrometheusRejectsBadPort() {
    assertThatThrownBy(() -> SdkBootstrapConfig.validatePrometheus(70000, "0.0.0.0"))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> SdkBootstrapConfig.validatePrometheus(0, "0.0.0.0"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void validatePrometheusRejectsBlankHost() {
    assertThatThrownBy(() -> SdkBootstrapConfig.validatePrometheus(50000, "  "))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> SdkBootstrapConfig.validatePrometheus(50000, null))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void applySetsLogLevelToWarnWhenUnset() {
    String prior = System.getProperty(SdkBootstrapConfig.SS_LOG_LEVEL);
    System.clearProperty(SdkBootstrapConfig.SS_LOG_LEVEL);
    try {
      // Only meaningful when the env var is also unset; skip if the env happens to set it.
      Assumptions.assumeTrue(System.getenv(SdkBootstrapConfig.SS_LOG_LEVEL) == null);
      SdkBootstrapConfig.apply(false, Optional.empty(), Optional.empty());
      assertThat(System.getProperty(SdkBootstrapConfig.SS_LOG_LEVEL)).isEqualTo("warn");
    } finally {
      if (prior == null) System.clearProperty(SdkBootstrapConfig.SS_LOG_LEVEL);
      else System.setProperty(SdkBootstrapConfig.SS_LOG_LEVEL, prior);
    }
  }

  @Test
  void applyRespectsExistingLogLevelSysprop() {
    String prior = System.getProperty(SdkBootstrapConfig.SS_LOG_LEVEL);
    System.setProperty(SdkBootstrapConfig.SS_LOG_LEVEL, "info");
    try {
      SdkBootstrapConfig.apply(false, Optional.empty(), Optional.empty());
      assertThat(System.getProperty(SdkBootstrapConfig.SS_LOG_LEVEL)).isEqualTo("info");
    } finally {
      if (prior == null) System.clearProperty(SdkBootstrapConfig.SS_LOG_LEVEL);
      else System.setProperty(SdkBootstrapConfig.SS_LOG_LEVEL, prior);
    }
  }
}
