package com.snowflake.kafka.connector.internal;

import com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig;
import net.snowflake.client.core.OCSPMode;
import net.snowflake.client.core.SFSessionProperty;
import net.snowflake.client.core.SFStatement;
import net.snowflake.client.jdbc.SnowflakeConnectionV1;
import net.snowflake.client.jdbc.SnowflakeFileTransferAgent;
import net.snowflake.client.jdbc.SnowflakeFileTransferConfig;
import net.snowflake.client.jdbc.SnowflakeFileTransferMetadataV1;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import net.snowflake.client.jdbc.cloud.storage.StageInfo;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class InternalStageIT {

  private final SnowflakeConnectionService service = TestUtils.getConnectionService();

  private final String stageName1 = TestUtils.randomStageName();
  private final String stageName2 = TestUtils.randomStageName();
  private final String stageName3 = TestUtils.randomStageName();
  private final String stageName4 = TestUtils.randomStageName();
  private final String proxyStage = TestUtils.randomStageName();
  private final String stageNameExpire= "kafka_connector_test_stage_credential_cache_expire";

  @After
  public void afterAll()
  {
    service.dropStage(stageName1);
    service.dropStage(stageName2);
    service.dropStage(stageName3);
    service.dropStage(stageName4);
  }

  @Test
  public void testInternalStage() throws Exception
  {
    // create stage
    service.createStage(stageName1);
    service.createStage(stageName2);
    service.createStage(stageName3);
    long startTime, fileNumber = 50;
    SnowflakeInternalStage agent = new SnowflakeInternalStage((SnowflakeConnectionV1) service.getConnection(),
      30 * 60 * 1000L, null);

    // we are using putwithcache API in s3 and azure.
    // TODO:: Once we move GCS to putWithCache, we can remove this check.
    if (usePutWithCacheApi(agent, stageName1))
    {
      // PUT two files to stageName1
      startTime = System.currentTimeMillis();
      agent.putWithCache(stageName1, "testCacheFileName1", "Any cache");
      agent.putWithCache(stageName1, "testCacheFileName2", "Any cache");
      List<String> files1 = service.listStage(stageName1, "testCache");
      assert files1.size() == 2;
      System.out.println(Logging.logMessage("Time: {} ms",
              (System.currentTimeMillis() - startTime)));

      // PUT 50 files to stageName2
      startTime = System.currentTimeMillis();
      fileNumber = 50;
      for (int i = 0; i < fileNumber; i++)
      {
        agent.putWithCache(stageName2, "appName/tableName/partition/testCacheFileName" + i, "Any cache");
      }
      List<String> files2 = service.listStage(stageName2, "appName/tableName/partition/testCache");
      assert files2.size() == fileNumber;
      System.out.println(Logging.logMessage("Time: {} ms",
              (System.currentTimeMillis() - startTime)));
    }

    // When stage is GCS
    if (!usePutWithCacheApi(agent, stageName1))
    {
      // PUT 50 files to stageName3
      startTime = System.currentTimeMillis();
      for (int i = 0; i < fileNumber; i++)
      {
        service.put(stageName3, "appName/tableName/partition/testNoCacheFileName" + i, "Any cache");
      }
      List<String> files3 = service.listStage(stageName3, "appName/tableName/partition/testNoCache");
      assert files3.size() == fileNumber;
      System.out.println(Logging.logMessage("Time: {} ms",
              (System.currentTimeMillis() - startTime)));
    }

  }

  /**
   * Tests internal put with cache API with proxy parameters set.
   * Please take a look at workflow file for configuration of proxy.
   */
  @Test
  public void testInternalStageWithProxy() throws SnowflakeSQLException {
    // create properties for proxy
    Properties proxyProperties = new Properties();

    proxyProperties.put(SFSessionProperty.USE_PROXY.getPropertyKey(), "true");
    proxyProperties.put(SFSessionProperty.PROXY_HOST.getPropertyKey(), "localhost");
    proxyProperties.put(SFSessionProperty.PROXY_PORT.getPropertyKey(), "3128");
    proxyProperties.put(SFSessionProperty.PROXY_USER.getPropertyKey(), "admin");
    proxyProperties.put(SFSessionProperty.PROXY_PASSWORD.getPropertyKey(), "test");

    // Create new snowflake connection service
    Map<String, String> config = TestUtils.getConf();

    config.put(SnowflakeSinkConnectorConfig.JVM_PROXY_HOST, "localhost");
    config.put(SnowflakeSinkConnectorConfig.JVM_PROXY_PORT, "3128");
    config.put(SnowflakeSinkConnectorConfig.JVM_PROXY_USERNAME, "admin");
    config.put(SnowflakeSinkConnectorConfig.JVM_PROXY_PASSWORD, "test");

    SnowflakeConnectionService proxyConnectionService = TestUtils.getConnectionService(config);
    // create stage
    proxyConnectionService.createStage(proxyStage);

    SnowflakeInternalStage agent = new SnowflakeInternalStage(
            (SnowflakeConnectionV1) proxyConnectionService.getConnection(),
            30 * 60 * 1000L, // cache expiration time
            proxyProperties); // proxy parameters used in JDBC

    // PUT two files to proxyStage
    long startTime = System.currentTimeMillis();
    if (usePutWithCacheApi(agent, proxyStage))
    {
      agent.putWithCache(proxyStage, "testInternalStageWithProxy1", "Any cache");
      agent.putWithCache(proxyStage, "testInternalStageWithProxy2", "Any cache");
    }
    else
    {
      proxyConnectionService.put(proxyStage, "testInternalStageWithProxy1", "Any cache");
      proxyConnectionService.put(proxyStage, "testInternalStageWithProxy2", "Any cache");
    }

    List<String> files1 = proxyConnectionService.listStage(proxyStage, "testInternalStage");
    assert files1.size() == 2;
    System.out.println(Logging.logMessage("Time: {} ms",
            (System.currentTimeMillis() - startTime)));
    proxyConnectionService.dropStage(proxyStage);

    // Reset proxy configuration
    TestUtils.resetProxyParametersInJDBC();
  }

  // Only runs in AWS and Azure until we move to putwithcache in gcs.
  @Test
  public void testCredentialRefresh() throws Exception
  {

    // create stage
    service.createStage(stageName4);

    // credential expires in 30 seconds
    SnowflakeInternalStage agent = new SnowflakeInternalStage((SnowflakeConnectionV1) service.getConnection(),
      30 * 1000L, null);

    if (usePutWithCacheApi(agent, stageName4))
    {
      // PUT two files to stageName1
      agent.putWithCache(stageName4, "testCacheFileName1", "Any cache");
      agent.putWithCache(stageName4, "testCacheFileName2", "Any cache");
      List<String> files1 = service.listStage(stageName4, "testCache");
      assert files1.size() == 2;

      // wait until the credential expires
      Thread.sleep(60 * 1000);

      agent.putWithCache(stageName4, "testCacheFileName3", "Any cache");
      agent.putWithCache(stageName4, "testCacheFileName4", "Any cache");
      List<String> files2 = service.listStage(stageName4, "testCache");
      assert files2.size() == 4;
    }
  }

  @Ignore
  @Test
  /**
   * This test is manually tested as it takes around 2 hours
   */
  public void testCredentialExpire() throws Exception
  {
    service.createStage(stageNameExpire);
    List<String> filesToDelete = service.listStage(stageNameExpire, "testExpire");
    service.purgeStage(stageNameExpire, filesToDelete);
    SnowflakeConnectionV1 conn = (SnowflakeConnectionV1) service.getConnection();

    String fullFilePath = "testExpire1";
    String data = "Any cache";

    String command = SnowflakeInternalStage.dummyPutCommandTemplate + stageNameExpire;

    SnowflakeFileTransferAgent agent = new SnowflakeFileTransferAgent(
      command,
      conn.getSfSession(),
      new SFStatement(conn.getSfSession())
    );

    SnowflakeFileTransferMetadataV1 fileTransferMetadata =
      (SnowflakeFileTransferMetadataV1) agent.getFileTransferMetadatas().get(0);

    // Set filename to be uploaded
    fileTransferMetadata.setPresignedUrlFileName(fullFilePath);

    byte[] dataBytes = data.getBytes(StandardCharsets.UTF_8);
    InputStream inStream = new ByteArrayInputStream(dataBytes);

    // Sleep until it expire
    Thread.sleep(2 * 60 * 60 * 1000);

    SnowflakeFileTransferAgent.uploadWithoutConnection(
      SnowflakeFileTransferConfig.Builder.newInstance()
        .setSnowflakeFileTransferMetadata(fileTransferMetadata)
        .setUploadStream(inStream)
        .setRequireCompress(true)
        .setOcspMode(OCSPMode.FAIL_OPEN)
        .build());

    List<String> filesExpire = service.listStage(stageNameExpire, "testExpire");
    assert filesExpire.size() == 1;
  }

  private boolean usePutWithCacheApi(final SnowflakeInternalStage agent,
                                     final String stageName)
  {
    if (agent.getStageType(stageName) == StageInfo.StageType.S3 ||
            agent.getStageType(stageName) == StageInfo.StageType.AZURE)
    {
      System.out.println("Using Put with cache since stage is in:" + agent.getStageType(stageName));
      return true;
    }
    return false;
  }
}