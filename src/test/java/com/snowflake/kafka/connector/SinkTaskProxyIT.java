package com.snowflake.kafka.connector;

import com.snowflake.kafka.connector.internal.EmbeddedProxyServer;
import com.snowflake.kafka.connector.internal.SnowflakeConnectionService;
import com.snowflake.kafka.connector.internal.SnowflakeKafkaConnectorException;
import com.snowflake.kafka.connector.internal.TestUtils;
import java.util.Map;
import java.util.Optional;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Integration tests for Snowflake Sink Task proxy configuration. Uses Testcontainers with a real
 * Squid proxy server running in Docker to test JVM proxy settings with authentication. The proxy
 * server uses a random available port on the host to avoid conflicts.
 *
 * <p>Note: This test requires Docker to be installed and running.
 */
public class SinkTaskProxyIT {

  private static final String PROXY_USERNAME = "admin";
  private static final String PROXY_PASSWORD = "test";

  private EmbeddedProxyServer proxyServer;

  @Before
  public void setUp() {
    proxyServer = new EmbeddedProxyServer(PROXY_USERNAME, PROXY_PASSWORD);
    proxyServer.start();
  }

  @After
  public void testCleanup() {
    if (proxyServer != null && proxyServer.isRunning()) {
      proxyServer.stop();
    }
    TestUtils.resetProxyParametersInJVM();
  }

  @Test(expected = SnowflakeKafkaConnectorException.class)
  @Ignore
  public void testSinkTaskProxyConfigMock() {
    Map<String, String> config = TestUtils.getConf();
    SnowflakeSinkConnectorConfig.setDefaultValues(config);

    config.put(SnowflakeSinkConnectorConfig.JVM_PROXY_HOST, "wronghost");
    config.put(SnowflakeSinkConnectorConfig.JVM_PROXY_PORT, "9093"); // wrongport
    config.put(SnowflakeSinkConnectorConfig.JVM_PROXY_USERNAME, "user");
    config.put(SnowflakeSinkConnectorConfig.JVM_PROXY_PASSWORD, "password");
    SnowflakeSinkTask sinkTask = new SnowflakeSinkTask();
    try {
      sinkTask.start(config);
    } catch (SnowflakeKafkaConnectorException e) {
      assert System.getProperty(Utils.HTTP_USE_PROXY).equals("true");
      assert System.getProperty(Utils.HTTP_PROXY_HOST).equals("wronghost");
      assert System.getProperty(Utils.HTTP_PROXY_PORT).equals("9093");
      assert System.getProperty(Utils.HTTPS_PROXY_HOST).equals("wronghost");
      assert System.getProperty(Utils.HTTPS_PROXY_PORT).equals("9093");
      assert System.getProperty(Utils.JDK_HTTP_AUTH_TUNNELING).isEmpty();
      assert System.getProperty(Utils.HTTP_PROXY_USER).equals("user");
      assert System.getProperty(Utils.HTTP_PROXY_PASSWORD).equals("password");
      assert System.getProperty(Utils.HTTPS_PROXY_USER).equals("user");
      assert System.getProperty(Utils.HTTPS_PROXY_PASSWORD).equals("password");

      // unset the system parameters please.
      TestUtils.resetProxyParametersInJVM();
      throw e;
    }
  }

  /**
   * Tests that the Snowflake Sink Task properly configures JVM proxy settings. This test verifies
   * that the JVM system properties are correctly set when proxy configuration is provided, without
   * actually connecting through a proxy or to Snowflake.
   *
   * <p>This is a focused unit test that verifies the proxy configuration logic.
   */
  @Test
  public void testProxyJvmPropertiesConfiguration() {
    Map<String, String> config = TestUtils.getConf();
    SnowflakeSinkConnectorConfig.setDefaultValues(config);

    // Configure proxy settings
    config.put(SnowflakeSinkConnectorConfig.JVM_PROXY_HOST, "test-proxy.example.com");
    config.put(SnowflakeSinkConnectorConfig.JVM_PROXY_PORT, "8080");
    config.put(SnowflakeSinkConnectorConfig.JVM_PROXY_USERNAME, PROXY_USERNAME);
    config.put(SnowflakeSinkConnectorConfig.JVM_PROXY_PASSWORD, PROXY_PASSWORD);

    // Set proxy properties (this is what the connector does internally)
    Utils.enableJVMProxy(config);

    // Verify all JVM proxy properties are set correctly
    Assert.assertEquals("true", System.getProperty(Utils.HTTP_USE_PROXY));
    Assert.assertEquals("test-proxy.example.com", System.getProperty(Utils.HTTP_PROXY_HOST));
    Assert.assertEquals("8080", System.getProperty(Utils.HTTP_PROXY_PORT));
    Assert.assertEquals("test-proxy.example.com", System.getProperty(Utils.HTTPS_PROXY_HOST));
    Assert.assertEquals("8080", System.getProperty(Utils.HTTPS_PROXY_PORT));
    Assert.assertEquals(PROXY_USERNAME, System.getProperty(Utils.HTTP_PROXY_USER));
    Assert.assertEquals(PROXY_PASSWORD, System.getProperty(Utils.HTTP_PROXY_PASSWORD));
    Assert.assertEquals(PROXY_USERNAME, System.getProperty(Utils.HTTPS_PROXY_USER));
    Assert.assertEquals(PROXY_PASSWORD, System.getProperty(Utils.HTTPS_PROXY_PASSWORD));
  }

  @Test
  public void testSinkTaskProxyConfig() {
    Map<String, String> config = TestUtils.getConf();
    SnowflakeSinkConnectorConfig.setDefaultValues(config);

    int proxyPort = proxyServer.getPort();
    config.put(SnowflakeSinkConnectorConfig.JVM_PROXY_HOST, "localhost");
    config.put(SnowflakeSinkConnectorConfig.JVM_PROXY_PORT, String.valueOf(proxyPort));
    config.put(SnowflakeSinkConnectorConfig.JVM_PROXY_USERNAME, PROXY_USERNAME);
    config.put(SnowflakeSinkConnectorConfig.JVM_PROXY_PASSWORD, PROXY_PASSWORD);
    SnowflakeSinkTask sinkTask = new SnowflakeSinkTask();

    sinkTask.start(config);

    assert System.getProperty(Utils.HTTP_USE_PROXY).equals("true");
    assert System.getProperty(Utils.HTTP_PROXY_HOST).equals("localhost");
    assert System.getProperty(Utils.HTTP_PROXY_PORT).equals(String.valueOf(proxyPort));
    assert System.getProperty(Utils.HTTPS_PROXY_HOST).equals("localhost");
    assert System.getProperty(Utils.HTTPS_PROXY_PORT).equals(String.valueOf(proxyPort));
    assert System.getProperty(Utils.JDK_HTTP_AUTH_TUNNELING).isEmpty();
    assert System.getProperty(Utils.HTTP_PROXY_USER).equals(PROXY_USERNAME);
    assert System.getProperty(Utils.HTTP_PROXY_PASSWORD).equals(PROXY_PASSWORD);
    assert System.getProperty(Utils.HTTPS_PROXY_USER).equals(PROXY_USERNAME);
    assert System.getProperty(Utils.HTTPS_PROXY_PASSWORD).equals(PROXY_PASSWORD);

    // Verify the snowflake connection service was created successfully
    Optional<SnowflakeConnectionService> optSfConnectionService = sinkTask.getSnowflakeConnection();
    Assert.assertTrue(optSfConnectionService.isPresent());
    
    // Cleanup
    sinkTask.stop();
  }
}

