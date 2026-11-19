package com.snowflake.kafka.connector.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import net.snowflake.client.internal.core.SFSessionProperty;
import org.junit.jupiter.api.Test;

public class InternalUtilsProxyTest {
  @Test
  public void withEmptyNonProxyHostsIfMissing_setsEmptyWhenProxyUsedWithoutNonProxyHosts() {
    Map<String, String> config = new HashMap<>();
    config.put(SnowflakeSinkConnectorConfig.JVM_PROXY_HOST, "localhost");
    config.put(SnowflakeSinkConnectorConfig.JVM_PROXY_PORT, "3128");
    Properties proxyProperties = InternalUtils.generateProxyParametersIfRequired(config);

    Properties result = InternalUtils.withEmptyNonProxyHostsIfMissing(proxyProperties);

    assertEquals("", result.getProperty(SFSessionProperty.NON_PROXY_HOSTS.getPropertyKey()));
    assertEquals("localhost", result.getProperty(SFSessionProperty.PROXY_HOST.getPropertyKey()));
    assertFalse(proxyProperties.containsKey(SFSessionProperty.NON_PROXY_HOSTS.getPropertyKey()));
  }

  @Test
  public void withEmptyNonProxyHostsIfMissing_keepsConfiguredNonProxyHosts() {
    Map<String, String> config = new HashMap<>();
    config.put(SnowflakeSinkConnectorConfig.JVM_PROXY_HOST, "localhost");
    config.put(SnowflakeSinkConnectorConfig.JVM_PROXY_PORT, "3128");
    config.put(SnowflakeSinkConnectorConfig.JVM_NON_PROXY_HOSTS, "*.internal");

    Properties result =
        InternalUtils.withEmptyNonProxyHostsIfMissing(
            InternalUtils.generateProxyParametersIfRequired(config));

    assertEquals(
        "*.internal", result.getProperty(SFSessionProperty.NON_PROXY_HOSTS.getPropertyKey()));
  }

  @Test
  public void withEmptyNonProxyHostsIfMissing_noProxy() {
    assertTrue(InternalUtils.withEmptyNonProxyHostsIfMissing(new Properties()).isEmpty());
    assertTrue(InternalUtils.withEmptyNonProxyHostsIfMissing(null).isEmpty());
  }
}
