package com.snowflake.kafka.connector;

import com.snowflake.kafka.connector.internal.EmbeddedProxyServer;
import com.snowflake.kafka.connector.internal.SnowflakeConnectionService;
import com.snowflake.kafka.connector.internal.SnowflakeKafkaConnectorException;
import com.snowflake.kafka.connector.internal.TestUtils;
import java.util.Map;
import java.util.Optional;
import org.junit.After;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

/**
 * Integration tests for Snowflake Sink Task proxy configuration. Uses Testcontainers with a real
 * Squid proxy server running in Docker to test JVM proxy settings with authentication. The proxy
 * server uses a random available port on the host to avoid conflicts.
 *
 * <p>Each test method gets its own proxy server instance via JUnit {@code @Rule}, ensuring tests
 * can run in parallel without port conflicts.
 *
 * <p>Note: This test requires Docker to be installed and running.
 */
public class SinkTaskProxyIT {

  private static final String PROXY_USERNAME = "admin";
  private static final String PROXY_PASSWORD = "test";

  @Rule
  public final EmbeddedProxyServer proxyServer =
      new EmbeddedProxyServer(PROXY_USERNAME, PROXY_PASSWORD);

  @After
  public void testCleanup() {
    TestUtils.resetProxyParametersInJVM();
  }

  @Test(expected = SnowflakeKafkaConnectorException.class)
  @Ignore
  public void testSinkTaskProxyConfigMock() {
    Map<String, String> config = TestUtils.transformProfileFileToConnectorConfiguration(false);
    SnowflakeSinkConnectorConfig.setDefaultValues(config);

    config.put(Utils.TASK_ID, "0");
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
    Map<String, String> config = TestUtils.transformProfileFileToConnectorConfiguration(false);
    SnowflakeSinkConnectorConfig.setDefaultValues(config);

    // Configure proxy settings
    config.put(SnowflakeSinkConnectorConfig.JVM_PROXY_HOST, "test-proxy.example.com");
    config.put(SnowflakeSinkConnectorConfig.JVM_PROXY_PORT, "8080");
    config.put(SnowflakeSinkConnectorConfig.JVM_PROXY_USERNAME, proxyServer.getUsername());
    config.put(SnowflakeSinkConnectorConfig.JVM_PROXY_PASSWORD, proxyServer.getPassword());

    // Set proxy properties (this is what the connector does internally)
    Utils.enableJVMProxy(config);

    // Verify all JVM proxy properties are set correctly
    Assert.assertEquals("true", System.getProperty(Utils.HTTP_USE_PROXY));
    Assert.assertEquals("test-proxy.example.com", System.getProperty(Utils.HTTP_PROXY_HOST));
    Assert.assertEquals("8080", System.getProperty(Utils.HTTP_PROXY_PORT));
    Assert.assertEquals("test-proxy.example.com", System.getProperty(Utils.HTTPS_PROXY_HOST));
    Assert.assertEquals("8080", System.getProperty(Utils.HTTPS_PROXY_PORT));
    Assert.assertEquals(proxyServer.getUsername(), System.getProperty(Utils.HTTP_PROXY_USER));
    Assert.assertEquals(proxyServer.getPassword(), System.getProperty(Utils.HTTP_PROXY_PASSWORD));
    Assert.assertEquals(proxyServer.getUsername(), System.getProperty(Utils.HTTPS_PROXY_USER));
    Assert.assertEquals(proxyServer.getPassword(), System.getProperty(Utils.HTTPS_PROXY_PASSWORD));
  }

  @Test
  public void testSinkTaskProxyConfig() {
    Map<String, String> config = TestUtils.transformProfileFileToConnectorConfiguration(false);
    SnowflakeSinkConnectorConfig.setDefaultValues(config);

    config.put(Utils.TASK_ID, "0");
    int proxyPort = proxyServer.getPort();
    config.put(SnowflakeSinkConnectorConfig.JVM_PROXY_HOST, "localhost");
    config.put(SnowflakeSinkConnectorConfig.JVM_PROXY_PORT, String.valueOf(proxyPort));
    config.put(SnowflakeSinkConnectorConfig.JVM_PROXY_USERNAME, proxyServer.getUsername());
    config.put(SnowflakeSinkConnectorConfig.JVM_PROXY_PASSWORD, proxyServer.getPassword());
    SnowflakeSinkTask sinkTask = new SnowflakeSinkTask();

    sinkTask.start(config);

    assert System.getProperty(Utils.HTTP_USE_PROXY).equals("true");
    assert System.getProperty(Utils.HTTP_PROXY_HOST).equals("localhost");
    assert System.getProperty(Utils.HTTP_PROXY_PORT).equals(String.valueOf(proxyPort));
    assert System.getProperty(Utils.HTTPS_PROXY_HOST).equals("localhost");
    assert System.getProperty(Utils.HTTPS_PROXY_PORT).equals(String.valueOf(proxyPort));
    assert System.getProperty(Utils.JDK_HTTP_AUTH_TUNNELING).isEmpty();
    assert System.getProperty(Utils.HTTP_PROXY_USER).equals(proxyServer.getUsername());
    assert System.getProperty(Utils.HTTP_PROXY_PASSWORD).equals(proxyServer.getPassword());
    assert System.getProperty(Utils.HTTPS_PROXY_USER).equals(proxyServer.getUsername());
    assert System.getProperty(Utils.HTTPS_PROXY_PASSWORD).equals(proxyServer.getPassword());

    // Verify the snowflake connection service was created successfully
    Optional<SnowflakeConnectionService> optSfConnectionService = sinkTask.getSnowflakeConnection();
    Assert.assertTrue(optSfConnectionService.isPresent());

    // Cleanup
    sinkTask.stop();
  }
}
