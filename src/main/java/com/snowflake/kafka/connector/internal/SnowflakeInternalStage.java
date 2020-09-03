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

public class SnowflakeInternalStage {

  private Map<String, SnowflakeFileTransferMetadataV1> storageInfoCache = new HashMap<>();

  public static String dummyPutCommandTemplate = "PUT file:///tmp/dummy_location_kakfa_connector_tmp/ @";

  private SnowflakeConnectionV1 conn;

  public SnowflakeInternalStage(SnowflakeConnectionV1 conn)
  {
    this.conn = conn;
  }

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

  public void putWithCache(String stage, String fullFilePath, String data)
  {
    String command = dummyPutCommandTemplate + stage;

    try
    {
      if (!storageInfoCache.containsKey(stage))
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
          storageInfoCache.put(stage, fileTransferMetadata);
        }
        else
        {
          throw SnowflakeErrors.ERROR_5017.getException();
        }
      }

      // Get credential from cache
      SnowflakeFileTransferMetadataV1 fileTransferMetadata = storageInfoCache.get(stage);
      // Set filename to be uploaded
      fileTransferMetadata.setPresignedUrlFileName(fullFilePath);

      byte[] dataBytes = data.getBytes(StandardCharsets.UTF_8);
      InputStream inStream = new ByteArrayInputStream(dataBytes);

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
}
