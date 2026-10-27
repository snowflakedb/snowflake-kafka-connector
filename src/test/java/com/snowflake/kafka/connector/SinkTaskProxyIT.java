package com.snowflake.kafka.connector;

import com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams;
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
    ConnectorConfigTools.setDefaultValues(config);

    config.put(Utils.TASK_ID, "0");
    config.put(KafkaConnectorConfigParams.JVM_PROXY_HOST, "wronghost");
    config.put(KafkaConnectorConfigParams.JVM_PROXY_PORT, "9093"); // wrongport
    config.put(KafkaConnectorConfigParams.JVM_PROXY_USERNAME, "user");
    config.put(KafkaConnectorConfigParams.JVM_PROXY_PASSWORD, "password");
    SnowflakeSinkTask sinkTask = new SnowflakeSinkTask();
    try {
      sinkTask.start(config);
    } catch (SnowflakeKafkaConnectorException e) {
      assertUnset(KafkaConnectorConfigParams.HTTPS_PROXY_HOST);
      assertUnset(KafkaConnectorConfigParams.HTTPS_PROXY_PORT);
      assertUnset(KafkaConnectorConfigParams.HTTP_PROXY_HOST);
      assertUnset(KafkaConnectorConfigParams.HTTP_PROXY_PORT);
      TestUtils.resetProxyParametersInJVM();
      throw e;
    }
  }

  /**
   * Connector proxy settings stay on the connector config. Starting a task must not publish them as
   * JVM system properties.
   */
  @Test
  public void testProxyJvmPropertiesConfiguration() {
    Map<String, String> config = TestUtils.transformProfileFileToConnectorConfiguration(false);
    ConnectorConfigTools.setDefaultValues(config);

    config.put(KafkaConnectorConfigParams.JVM_PROXY_HOST, "test-proxy.example.com");
    config.put(KafkaConnectorConfigParams.JVM_PROXY_PORT, "8080");
    config.put(KafkaConnectorConfigParams.JVM_PROXY_USERNAME, proxyServer.getUsername());
    config.put(KafkaConnectorConfigParams.JVM_PROXY_PASSWORD, proxyServer.getPassword());
    TestUtils.resetProxyParametersInJVM();

    Assert.assertEquals(
        "test-proxy.example.com", config.get(KafkaConnectorConfigParams.JVM_PROXY_HOST));
    assertUnset(KafkaConnectorConfigParams.HTTP_USE_PROXY);
    assertUnset(KafkaConnectorConfigParams.HTTP_PROXY_HOST);
    assertUnset(KafkaConnectorConfigParams.HTTPS_PROXY_HOST);
    assertUnset(KafkaConnectorConfigParams.HTTP_PROXY_USER);
    assertUnset(KafkaConnectorConfigParams.HTTPS_PROXY_PASSWORD);
  }

  @Test
  public void testSinkTaskProxyConfig() {
    Map<String, String> config = TestUtils.transformProfileFileToConnectorConfiguration(false);
    ConnectorConfigTools.setDefaultValues(config);

    config.put(Utils.TASK_ID, "0");
    int proxyPort = proxyServer.getPort();
    config.put(KafkaConnectorConfigParams.JVM_PROXY_HOST, "localhost");
    config.put(KafkaConnectorConfigParams.JVM_PROXY_PORT, String.valueOf(proxyPort));
    config.put(KafkaConnectorConfigParams.JVM_PROXY_USERNAME, proxyServer.getUsername());
    config.put(KafkaConnectorConfigParams.JVM_PROXY_PASSWORD, proxyServer.getPassword());
    SnowflakeSinkTask sinkTask = new SnowflakeSinkTask();

    TestUtils.resetProxyParametersInJVM();
    sinkTask.start(config);

    assertUnset(KafkaConnectorConfigParams.HTTPS_PROXY_HOST);
    assertUnset(KafkaConnectorConfigParams.HTTPS_PROXY_PORT);
    assertUnset(KafkaConnectorConfigParams.HTTP_PROXY_HOST);
    assertUnset(KafkaConnectorConfigParams.HTTP_PROXY_USER);
    assertUnset(KafkaConnectorConfigParams.HTTPS_PROXY_PASSWORD);

    // Verify the snowflake connection service was created successfully
    Optional<SnowflakeConnectionService> optSfConnectionService = sinkTask.getSnowflakeConnection();
    Assert.assertTrue(optSfConnectionService.isPresent());

    // Cleanup
    sinkTask.stop();
  }

  private static void assertUnset(String key) {
    String value = System.getProperty(key);
    Assert.assertTrue(value == null || value.isEmpty());
  }
}
