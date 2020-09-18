package com.snowflake.kafka.connector.internal;

import net.snowflake.client.core.OCSPMode;
import net.snowflake.client.core.SFStatement;
import net.snowflake.client.jdbc.*;
import net.snowflake.client.jdbc.cloud.storage.StageInfo;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class SnowflakeInternalStage {

  private class SnowflakeMetadataWithExpiration {
    SnowflakeFileTransferMetadataV1 fileTransferMetadata;
    long timestamp;

    SnowflakeMetadataWithExpiration(SnowflakeFileTransferMetadataV1 fileTransferMetadata, long timestamp)
    {
      this.fileTransferMetadata = fileTransferMetadata;
      this.timestamp = timestamp;
    }
  }

  public static String dummyPutCommandTemplate = "PUT file:///tmp/dummy_location_kakfa_connector_tmp/ @";

  // Lock to gard the credential map
  private final Lock credentialLock;
  private Map<String, SnowflakeMetadataWithExpiration> storageInfoCache = new HashMap<>();

  private SnowflakeConnectionV1 conn;
  private long expirationTime;

  public SnowflakeInternalStage(SnowflakeConnectionV1 conn, long expirationTime)
  {
    this.conn = conn;
    this.credentialLock = new ReentrantLock();
    this.expirationTime = expirationTime;
  }

  /**
   * Get the backend stage type, S3, Azure or GCS. Involves one GS call.
   * @param stage name of the stage
   * @return stage type
   */
  public StageInfo.StageType getStageType(String stage)
  {
    try {
      String command = dummyPutCommandTemplate + stage;
      SnowflakeFileTransferAgent agent = new SnowflakeFileTransferAgent(
        command,
        conn.getSfSession(),
        new SFStatement(conn.getSfSession())
      );
      return agent.getStageInfo().getStageType();
    } catch (Exception e)
    {
      throw SnowflakeErrors.ERROR_5019.getException(e);
    }
  }

  /**
   * Upload file to internal stage with previously cached credentials. Refresh credential every 30 minutes
   * @param stage        Stage name
   * @param fullFilePath Full file name to be uploaded
   * @param data         Data string to be uploaded
   */
  public void putWithCache(String stage, String fullFilePath, String data)
  {
    String command = dummyPutCommandTemplate + stage;

    try
    {
      if (!isCredentialValid(stage))
      {
        SnowflakeFileTransferAgent agent = new SnowflakeFileTransferAgent(
          command,
          conn.getSfSession(),
          new SFStatement(conn.getSfSession())
        );
        // If the backend is not GCP, we cache the credential. Otherwise throw error.
        // transfer metadata list must only have one element
        SnowflakeFileTransferMetadataV1 fileTransferMetadata =
          (SnowflakeFileTransferMetadataV1) agent.getFileTransferMetadatas().get(0);
        if (fileTransferMetadata.getStageInfo().getStageType() != StageInfo.StageType.GCS)
        {
          storageInfoCache.put(stage,
            new SnowflakeMetadataWithExpiration(fileTransferMetadata, System.currentTimeMillis()));
        }
        else
        {
          throw SnowflakeErrors.ERROR_5017.getException();
        }
      }

      // Get credential from cache
      SnowflakeFileTransferMetadataV1 fileTransferMetadata = storageInfoCache.get(stage).fileTransferMetadata;
      // Set filename to be uploaded
      fileTransferMetadata.setPresignedUrlFileName(fullFilePath);

      byte[] dataBytes = data.getBytes(StandardCharsets.UTF_8);
      InputStream inStream = new ByteArrayInputStream(dataBytes);

      // This uploadWithoutConnection is more like a hack.
      SnowflakeFileTransferAgent.uploadWithoutConnection(
        SnowflakeFileTransferConfig.Builder.newInstance()
          .setSnowflakeFileTransferMetadata(fileTransferMetadata)
          .setUploadStream(inStream)
          .setRequireCompress(true)
          .setOcspMode(OCSPMode.FAIL_OPEN)
          .build());

    } catch (Exception e)
    {
      throw SnowflakeErrors.ERROR_5018.getException(e);
    }
  }

  private boolean isCredentialValid(String stage)
  {
    // Kay is cached and not expired
    return storageInfoCache.containsKey(stage) &&
          System.currentTimeMillis() - storageInfoCache.get(stage).timestamp < expirationTime;
  }
}
