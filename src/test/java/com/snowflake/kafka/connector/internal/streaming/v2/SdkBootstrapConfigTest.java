package com.snowflake.kafka.connector.internal.streaming.v2;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

class SdkBootstrapConfigTest {

  @Test
  void logLevelDefaultsToWarnWhenUnset() {
    assertThat(SdkBootstrapConfig.logLevelToSet(null, null)).contains("warn");
  }

  @Test
  void logLevelLeftUnchangedWhenSyspropSet() {
    assertThat(SdkBootstrapConfig.logLevelToSet("info", null)).isEmpty();
  }

  @Test
  void logLevelLeftUnchangedWhenEnvSet() {
    assertThat(SdkBootstrapConfig.logLevelToSet(null, "debug")).isEmpty();
  }

  @Test
  void prometheusDisabledByDefault() {
    assertThat(SdkBootstrapConfig.prometheusToSet(new HashMap<>())).isEmpty();
  }

  @Test
  void prometheusEnabledReturnsPort() {
    Map<String, String> cfg = new HashMap<>();
    cfg.put("snowflake.streaming.metrics.prometheus.enable", "true");
    cfg.put("snowflake.streaming.metrics.prometheus.port", "51000");
    assertThat(SdkBootstrapConfig.prometheusToSet(cfg)).contains(51000);
  }

  @Test
  void prometheusInvalidPortFailsFast() {
    Map<String, String> cfg = new HashMap<>();
    cfg.put("snowflake.streaming.metrics.prometheus.enable", "true");
    cfg.put("snowflake.streaming.metrics.prometheus.port", "not-a-number");
    assertThatThrownBy(() -> SdkBootstrapConfig.prometheusToSet(cfg))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void prometheusOutOfRangePortFailsFast() {
    Map<String, String> cfg = new HashMap<>();
    cfg.put("snowflake.streaming.metrics.prometheus.enable", "true");
    cfg.put("snowflake.streaming.metrics.prometheus.port", "70000");
    assertThatThrownBy(() -> SdkBootstrapConfig.prometheusToSet(cfg))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void prometheusHostDefaultsToLoopback() {
    assertThat(SdkBootstrapConfig.prometheusHost(new java.util.HashMap<>())).isEqualTo("127.0.0.1");
  }

  @Test
  void prometheusHostFromConfig() {
    java.util.Map<String, String> cfg = new java.util.HashMap<>();
    cfg.put("snowflake.streaming.metrics.prometheus.host", "0.0.0.0");
    assertThat(SdkBootstrapConfig.prometheusHost(cfg)).isEqualTo("0.0.0.0");
  }

  @Test
  void applySetsLogLevelToWarnWhenUnset() {
    String prior = System.getProperty("SS_LOG_LEVEL");
    System.clearProperty("SS_LOG_LEVEL");
    try {
      // Only meaningful when the env var is also unset; skip if the env happens to set it.
      Assumptions.assumeTrue(System.getenv("SS_LOG_LEVEL") == null);
      SdkBootstrapConfig.apply(new java.util.HashMap<>());
      assertThat(System.getProperty("SS_LOG_LEVEL")).isEqualTo("warn");
    } finally {
      if (prior == null) System.clearProperty("SS_LOG_LEVEL");
      else System.setProperty("SS_LOG_LEVEL", prior);
    }
  }

  @Test
  void applyRespectsExistingLogLevelSysprop() {
    String prior = System.getProperty("SS_LOG_LEVEL");
    System.setProperty("SS_LOG_LEVEL", "info");
    try {
      SdkBootstrapConfig.apply(new java.util.HashMap<>());
      assertThat(System.getProperty("SS_LOG_LEVEL")).isEqualTo("info");
    } finally {
      if (prior == null) System.clearProperty("SS_LOG_LEVEL");
      else System.setProperty("SS_LOG_LEVEL", prior);
    }
  }
}
