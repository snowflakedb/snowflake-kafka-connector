package com.snowflake.kafka.connector.internal;

import com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig;
import com.snowflake.kafka.connector.Utils;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import net.snowflake.client.core.OCSPMode;
import net.snowflake.client.core.SFSessionProperty;
import net.snowflake.client.core.SFStatement;
import net.snowflake.client.jdbc.SnowflakeConnectionV1;
import net.snowflake.client.jdbc.SnowflakeFileTransferAgent;
import net.snowflake.client.jdbc.SnowflakeFileTransferConfig;
import net.snowflake.client.jdbc.SnowflakeFileTransferMetadataV1;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import net.snowflake.client.jdbc.cloud.storage.StageInfo;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

public class InternalStageIT {

  private static SnowflakeConnectionService service;
  private static final String stageName1 = TestUtils.randomStageName();
  private static final String stageName2 = TestUtils.randomStageName();
  private static final String stageNameGCSPut = TestUtils.randomStageName();
  private static final String stageNameGCSPutWithCache = TestUtils.randomStageName();
  private static final String stageName4 = TestUtils.randomStageName();
  private static final String proxyStage = TestUtils.randomStageName();
  private static final String stageNameExpire =
      "kafka_connector_test_stage_credential_cache_expire";

  /* Stage Type will be same for all this tests since at once we only run it against one cloud */
  private static StageInfo.StageType stageType;

  @BeforeClass
  public static void beforeAllClasses() {
    service = TestUtils.getConnectionService();
    service.createStage(stageName1);
    service.createStage(stageName2);
    service.createStage(stageNameGCSPut);
    service.createStage(stageNameGCSPutWithCache);
    service.createStage(stageName4);
    service.createStage(proxyStage);
    service.createStage(stageNameExpire);
    stageType =
        ((SnowflakeConnectionServiceV1) service).getInternalStage().getStageType(stageName1);
  }

  @AfterClass
  public static void afterAllClasses() {
    service.dropStage(stageName1);
    service.dropStage(stageName2);
    service.dropStage(stageNameGCSPut);
    //    service.dropStage(stageNameGCSPutWithCache);
    service.dropStage(stageName4);
    service.dropStage(proxyStage);
    service.dropStage(stageNameExpire);
  }

  @Test
  public void testInternalStage() throws Exception {
    // create stage
    long startTime, fileNumber = 50;
    SnowflakeInternalStage agent =
        new SnowflakeInternalStage(
            (SnowflakeConnectionV1) service.getConnection(), 30 * 60 * 1000L, null);

    // PUT two files to stageName1
    startTime = System.currentTimeMillis();
    agent.putWithCache(stageName1, "testCacheFileName1", "Any cache", stageType);
    agent.putWithCache(stageName1, "testCacheFileName2", "Any cache", stageType);
    List<String> files1 = service.listStage(stageName1, "testCache");
    assert files1.size() == 2;
    System.out.println(
        Utils.formatLogMessage("Time: {} ms", (System.currentTimeMillis() - startTime)));

    // PUT 50 files to stageName2
    startTime = System.currentTimeMillis();
    for (int i = 0; i < fileNumber; i++) {
      agent.putWithCache(
          stageName2, "appName/tableName/partition/testCacheFileName" + i, "Any cache", stageType);
    }
    List<String> files2 = service.listStage(stageName2, "appName/tableName/partition/testCache");
    assert files2.size() == fileNumber;
    System.out.println(
        Utils.formatLogMessage("Time: {} ms", (System.currentTimeMillis() - startTime)));
  }

  @Test
  public void testComparePutAPIVersionsForGCS() {

    // PUT 500 files to stageName3
    final int numberOfFiles = 50;
    long startTime = System.currentTimeMillis();
    for (int i = 0; i < numberOfFiles; i++) {
      service.put(
          stageNameGCSPut, "appName/tableName/partition/testNoCacheFileName" + i, "Any data");
    }
    System.out.println(
        Utils.formatLogMessage(
            "Time for putting {} files in GCS is:{} ms",
            numberOfFiles,
            (System.currentTimeMillis() - startTime)));

    List<String> files =
        service.listStage(stageNameGCSPut, "appName/tableName/partition/testNoCacheFileName");
    assert files.size() == numberOfFiles;

    System.out.println("Same test but with uploadWithoutConnection API");
    startTime = System.currentTimeMillis();
    for (int i = 0; i < numberOfFiles; i++) {
      service.putWithCache(
          stageNameGCSPutWithCache,
          "appName/tableName/partition/testCacheFileName" + i,
          "Any data");
    }
    System.out.println(
        Utils.formatLogMessage(
            "Time for putting {} files in GCS(UploadWithoutConnection) is:{} ms",
            numberOfFiles,
            (System.currentTimeMillis() - startTime)));
    List<String> filesWithCache =
        service.listStage(
            stageNameGCSPutWithCache, "appName/tableName/partition/testCacheFileName");
    assert filesWithCache.size() == numberOfFiles;
  }

  /**
   * Tests internal put with cache API with proxy parameters set. Please take a look at workflow
   * file for configuration of proxy.
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

    SnowflakeInternalStage agent =
        new SnowflakeInternalStage(
            (SnowflakeConnectionV1) proxyConnectionService.getConnection(),
            30 * 60 * 1000L, // cache expiration time
            proxyProperties); // proxy parameters used in JDBC

    // PUT two files to proxyStage
    long startTime = System.currentTimeMillis();
    agent.putWithCache(proxyStage, "testInternalStageWithProxy1", "Any cache", stageType);
    agent.putWithCache(proxyStage, "testInternalStageWithProxy2", "Any cache", stageType);

    List<String> files1 = proxyConnectionService.listStage(proxyStage, "testInternalStage");
    assert files1.size() == 2;
    System.out.println(
        Utils.formatLogMessage("Time: {} ms", (System.currentTimeMillis() - startTime)));
    proxyConnectionService.dropStage(proxyStage);
  }

  @Test
  public void testCredentialRefresh() throws Exception {
    // credential expires in 30 seconds
    SnowflakeInternalStage agent =
        new SnowflakeInternalStage(
            (SnowflakeConnectionV1) service.getConnection(), 30 * 1000L, null);

    // PUT two files to stageName1
    agent.putWithCache(stageName4, "testCacheFileName1", "Any cache", stageType);
    agent.putWithCache(stageName4, "testCacheFileName2", "Any cache", stageType);
    List<String> files1 = service.listStage(stageName4, "testCache");
    assert files1.size() == 2;

    // wait until the credential expires
    Thread.sleep(60 * 1000);

    agent.putWithCache(stageName4, "testCacheFileName3", "Any cache", stageType);
    agent.putWithCache(stageName4, "testCacheFileName4", "Any cache", stageType);
    List<String> files2 = service.listStage(stageName4, "testCache");
    assert files2.size() == 4;
  }

  @Ignore
  @Test
  /** This test is manually tested as it takes around 2 hours */
  public void testCredentialExpire() throws Exception {
    List<String> filesToDelete = service.listStage(stageNameExpire, "testExpire");
    service.purgeStage(stageNameExpire, filesToDelete);
    SnowflakeConnectionV1 conn = (SnowflakeConnectionV1) service.getConnection();

    String fullFilePath = "testExpire1";
    String data = "Any cache";

    String command =
        String.format(SnowflakeInternalStage.dummyPutCommandTemplateAWSAndAzure, stageNameExpire);

    SnowflakeFileTransferAgent agent =
        new SnowflakeFileTransferAgent(
            command, conn.getSfSession(), new SFStatement(conn.getSfSession()));

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
}
