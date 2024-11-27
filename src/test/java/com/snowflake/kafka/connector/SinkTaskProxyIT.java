package com.snowflake.kafka.connector;

import com.snowflake.kafka.connector.internal.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.After;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

public class SinkTaskProxyIT {

  @After
  public void testCleanup() {
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
   * To run this test, spin up a http/https proxy at 127.0.0.1:3128 and set authentication as
   * required.
   *
   * <p>For instructions on how to setup proxy server take a look at
   * .github/workflows/IntegrationTest.yml
   */
  @Test
  public void testSinkTaskProxyConfig() {
    Map<String, String> config = TestUtils.getConf();
    SnowflakeSinkConnectorConfig.setDefaultValues(config);

    config.put(SnowflakeSinkConnectorConfig.JVM_PROXY_HOST, "localhost");
    config.put(SnowflakeSinkConnectorConfig.JVM_PROXY_PORT, "3128");
    config.put(SnowflakeSinkConnectorConfig.JVM_PROXY_USERNAME, "admin");
    config.put(SnowflakeSinkConnectorConfig.JVM_PROXY_PASSWORD, "test");
    SnowflakeSinkTask sinkTask = new SnowflakeSinkTask();

    sinkTask.start(config);

    assert System.getProperty(Utils.HTTP_USE_PROXY).equals("true");
    assert System.getProperty(Utils.HTTP_PROXY_HOST).equals("localhost");
    assert System.getProperty(Utils.HTTP_PROXY_PORT).equals("3128");
    assert System.getProperty(Utils.HTTPS_PROXY_HOST).equals("localhost");
    assert System.getProperty(Utils.HTTPS_PROXY_PORT).equals("3128");
    assert System.getProperty(Utils.JDK_HTTP_AUTH_TUNNELING).isEmpty();
    assert System.getProperty(Utils.HTTP_PROXY_USER).equals("admin");
    assert System.getProperty(Utils.HTTP_PROXY_PASSWORD).equals("test");
    assert System.getProperty(Utils.HTTPS_PROXY_USER).equals("admin");
    assert System.getProperty(Utils.HTTPS_PROXY_PASSWORD).equals("test");

    // get the snowflakeconnection service which was made during sinkTask

    Optional<SnowflakeConnectionService> optSfConnectionService = sinkTask.getSnowflakeConnection();

    Assert.assertTrue(optSfConnectionService.isPresent());

    SnowflakeConnectionService connectionService = optSfConnectionService.get();

    String stage = TestUtils.randomStageName();
    String pipe = TestUtils.randomPipeName();
    String table = TestUtils.randomTableName();

    connectionService.createStage(stage);
    connectionService.createTable(table);
    connectionService.createPipe(table, stage, pipe);

    SnowflakeIngestionService ingestionService = connectionService.buildIngestService(stage, pipe);

    String file = "{\"aa\":123}";
    String fileName =
        FileNameTestUtils.fileName(TestUtils.TEST_CONNECTOR_NAME, table, null, 0, 0, 1);

    connectionService.putWithCache(stage, fileName, file);
    ingestionService.ingestFile(fileName);
    List<String> names = new ArrayList<>(1);
    names.add(fileName);
  }
}
