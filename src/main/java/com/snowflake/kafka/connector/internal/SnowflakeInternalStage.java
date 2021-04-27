package com.snowflake.kafka.connector.internal;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import net.snowflake.client.core.OCSPMode;
import net.snowflake.client.core.SFStatement;
import net.snowflake.client.jdbc.SnowflakeConnectionV1;
import net.snowflake.client.jdbc.SnowflakeFileTransferAgent;
import net.snowflake.client.jdbc.SnowflakeFileTransferConfig;
import net.snowflake.client.jdbc.SnowflakeFileTransferMetadataV1;
import net.snowflake.client.jdbc.cloud.storage.StageInfo;

public class SnowflakeInternalStage extends Logging {

  private static class SnowflakeMetadataWithExpiration {
    SnowflakeFileTransferMetadataV1 fileTransferMetadata;
    long timestamp;

    SnowflakeMetadataWithExpiration(
        SnowflakeFileTransferMetadataV1 fileTransferMetadata, long timestamp) {
      this.fileTransferMetadata = fileTransferMetadata;
      this.timestamp = timestamp;
    }
  }

  // Any operation on the map should be atomic
  private final ConcurrentMap<String, SnowflakeMetadataWithExpiration> storageInfoCache =
      new ConcurrentHashMap<>();

  public static String dummyPutCommandTemplate =
      "PUT file:///tmp/dummy_location_kakfa_connector_tmp/ @";
  private final SnowflakeConnectionV1 conn;
  private final long expirationTime;
  // Proxy parameters that we set while calling the snowflake JDBC.
  // Also required to pass in the uploadWithoutConnection API in the SnowflakeFileTransferConfig
  // It may not necessarily just contain proxy parameters, JDBC client filters all other properties.
  private final Properties proxyProperties;

  public SnowflakeInternalStage(
      SnowflakeConnectionV1 conn, long expirationTime, Properties proxyProperties) {
    this.conn = conn;
    this.expirationTime = expirationTime;
    this.proxyProperties = proxyProperties;
  }

  /**
   * Get the backend stage type, S3, Azure or GCS. Involves one GS call.
   *
   * @param stage name of the stage
   * @return stage type
   */
  public StageInfo.StageType getStageType(String stage) {
    try {
      String command = dummyPutCommandTemplate + stage;
      SnowflakeFileTransferAgent agent =
          new SnowflakeFileTransferAgent(
              command, conn.getSfSession(), new SFStatement(conn.getSfSession()));
      return agent.getStageInfo().getStageType();
    } catch (Exception e) {
      throw SnowflakeErrors.ERROR_5019.getException(e);
    }
  }

  /**
   * Upload file to internal stage with previously cached credentials. Refresh credential every 30
   * minutes
   *
   * @param stage Stage name
   * @param fullFilePath Full file name to be uploaded
   * @param data Data string to be uploaded
   */
  public void putWithCache(String stage, String fullFilePath, String data) {
    String command = dummyPutCommandTemplate + stage;

    try {
      SnowflakeMetadataWithExpiration credential = storageInfoCache.getOrDefault(stage, null);
      if (!isCredentialValid(credential)) {
        logDebug("Query credential for stage:{}, filePath:{}", stage, fullFilePath);
        SnowflakeFileTransferAgent agent =
            new SnowflakeFileTransferAgent(
                command, conn.getSfSession(), new SFStatement(conn.getSfSession()));
        // If the backend is not GCP, we cache the credential. Otherwise throw error.
        // transfer metadata list must only have one element
        SnowflakeFileTransferMetadataV1 fileTransferMetadata =
            (SnowflakeFileTransferMetadataV1) agent.getFileTransferMetadatas().get(0);
        if (fileTransferMetadata.getStageInfo().getStageType() != StageInfo.StageType.GCS) {
          // Overwrite the credential to be used
          credential =
              new SnowflakeMetadataWithExpiration(fileTransferMetadata, System.currentTimeMillis());
          storageInfoCache.put(stage, credential);
        } else {
          throw SnowflakeErrors.ERROR_5017.getException();
        }
      }

      SnowflakeFileTransferMetadataV1 fileTransferMetadata = credential.fileTransferMetadata;
      // Set filename to be uploaded
      fileTransferMetadata.setPresignedUrlFileName(fullFilePath);

      byte[] dataBytes = data.getBytes(StandardCharsets.UTF_8);
      InputStream inStream = new ByteArrayInputStream(dataBytes);

      // This uploadWithoutConnection api cannot handle expired credentials very well.
      // Need to prevent passing expired credential to it.
      try {
        SnowflakeFileTransferAgent.uploadWithoutConnection(
            SnowflakeFileTransferConfig.Builder.newInstance()
                .setSnowflakeFileTransferMetadata(fileTransferMetadata)
                .setUploadStream(inStream)
                .setRequireCompress(true)
                .setOcspMode(OCSPMode.FAIL_OPEN)
                .setProxyProperties(proxyProperties)
                .build());
      } catch (Throwable t) {
        // If this api encounters error, invalid the cached credentials
        // Caller will retry this function
        logWarn("uploadWithoutConnection encountered an error for fileName:{}", fullFilePath);
        storageInfoCache.remove(stage);
        throw t;
      }

    } catch (Exception e) {
      logWarn("Caught exception in putWithCache for fileName:{}", e.getMessage(), fullFilePath);
      throw SnowflakeErrors.ERROR_5018.getException(e);
    }
  }

  private boolean isCredentialValid(SnowflakeMetadataWithExpiration credential) {
    // Kay is cached and not expired
    return credential != null && System.currentTimeMillis() - credential.timestamp < expirationTime;
  }
}
