package com.snowflake.kafka.connector.internal;

import com.google.common.annotations.VisibleForTesting;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import net.snowflake.client.core.OCSPMode;
import net.snowflake.client.core.SFStatement;
import net.snowflake.client.jdbc.SnowflakeConnectionV1;
import net.snowflake.client.jdbc.SnowflakeFileTransferAgent;
import net.snowflake.client.jdbc.SnowflakeFileTransferConfig;
import net.snowflake.client.jdbc.SnowflakeFileTransferMetadataV1;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import net.snowflake.client.jdbc.cloud.storage.StageInfo;
import net.snowflake.client.jdbc.internal.apache.commons.io.FilenameUtils;

/**
 * Implementation of put API through JDBC's API uploadWithoutConnection.
 *
 * <p>We fetch the credentials and cache it for AWS and Azure. We will refresh if cache hits 30 mins
 * (Cache Eviction)
 *
 * <p>For GCS, we dont have any cache, we will make a call to GS for every put API since we require
 * presignedURL
 */
public class SnowflakeInternalStage {

  private static class SnowflakeMetadataWithExpiration {

    /* File transfer metadata when fetched from GS call */
    private final SnowflakeFileTransferMetadataV1 fileTransferMetadata;

    /* Timestamp when GS call happened */
    private final long timestampLastCredentialFetchMillis;

    /* StageType, can be fetched from fileTransferMetadata */
    private final StageInfo.StageType stageType;

    SnowflakeMetadataWithExpiration(
        SnowflakeFileTransferMetadataV1 fileTransferMetadata, long timestamp) {
      this.fileTransferMetadata = fileTransferMetadata;
      this.timestampLastCredentialFetchMillis = timestamp;
      this.stageType = this.fileTransferMetadata.getStageInfo().getStageType();
    }

    public StageInfo.StageType getStageType() {
      return this.stageType;
    }
  }

  private final KCLogger LOGGER = new KCLogger(SnowflakeInternalStage.class.getName());

  // Any operation on the map should be atomic
  private final ConcurrentMap<String, SnowflakeMetadataWithExpiration> storageInfoCache =
      new ConcurrentHashMap<>();

  // GCS Put version requires the dummy command to have filename and entire filePath including
  // stageName after "@"
  // For example: PUT file:///fileName @stageName/app/table/partition
  // Here are all the replacements required until JDBC bug is fixed: SNOW-350676
  // fileName, stageName and full file path (excluding the fileName)
  public static String dummyPutCommandTemplateGCS = "PUT file:///%s @%s/%s";

  // Aws and Azure Put version requires fullFilePath to be set as presignedURL
  public static String dummyPutCommandTemplateAWSAndAzure = "PUT file:///tmp/ @%s";

  // This will work on all three clouds since we only care about fetching stageType
  public static String dummyPutCommandToGetStageType = "PUT file:///tmp/dummyFileName @%s";

  private final SnowflakeConnectionV1 conn;
  private final long expirationTimeMillis;
  // Proxy parameters that we set while calling the snowflake JDBC.
  // Also required to pass in the uploadWithoutConnection API in the SnowflakeFileTransferConfig
  // It may not necessarily just contain proxy parameters, JDBC client filters all other properties.
  private final Properties proxyProperties;

  public SnowflakeInternalStage(
      SnowflakeConnectionV1 conn, long expirationTimeMillis, Properties proxyProperties) {
    this.conn = conn;
    this.expirationTimeMillis = expirationTimeMillis;
    this.proxyProperties = proxyProperties;
  }

  /**
   * Get the backend stage type, S3, Azure or GCS. Involves one GS only when storageInfoCache has
   * stage name missing.
   *
   * @param stageName name of the stage
   * @return stage type
   */
  public StageInfo.StageType getStageType(String stageName) {

    Optional<StageInfo.StageType> existingStageTypeFromCache = getStageTypeFromCache(stageName);
    if (existingStageTypeFromCache.isPresent()) {
      return existingStageTypeFromCache.get();
    }
    // Lets try to fetch the stageType by making a GS call.
    try {
      String command = String.format(dummyPutCommandToGetStageType, stageName);
      SnowflakeFileTransferAgent agent =
          new SnowflakeFileTransferAgent(
              command, conn.getSfSession(), new SFStatement(conn.getSfSession()));
      return agent.getStageInfo().getStageType();
    } catch (Exception e) {
      throw SnowflakeErrors.ERROR_5019.getException(e);
    }
  }

  /**
   * Helper function which fetches the stageType from Cache. Cache Hit -> Return StageType Cache
   * Miss -> Optional.empty
   *
   * @param stageName to search inside cache
   * @return stage if present in cache
   */
  private Optional<StageInfo.StageType> getStageTypeFromCache(final String stageName) {
    if (storageInfoCache.containsKey(stageName)) {
      return Optional.of(storageInfoCache.get(stageName).getStageType());
    }
    return Optional.empty();
  }

  /**
   * Upload file to internal stage with previously cached credentials. Refresh credential every 30
   * minutes for AWS and Azure. We do cache for GCS but we always refresh it for every put(every
   * file upload)
   *
   * <p>If we pass in expired credentials, we will get expired credentials error from cloud.
   *
   * <p>We are already doing a retry for this failure and renew the credentials from our end by
   * calling tradition put API with connection.
   *
   * @param stageName Stage name
   * @param fullFilePath Full file name to be uploaded
   * @param data Data string to be uploaded
   * @param stageType GCS, Azure or AWS
   */
  public void putWithCache(
      String stageName, String fullFilePath, String data, final StageInfo.StageType stageType) {
    try {
      SnowflakeMetadataWithExpiration credential = storageInfoCache.getOrDefault(stageName, null);

      if (!isCredentialValid(credential, stageType)) {
        // This should always be executed in GCS
        LOGGER.debug(
            "Query credential(Refreshing Credentials) for stageName:{}, filePath:{}",
            stageName,
            fullFilePath);
        refreshCredentials(stageName, stageType, fullFilePath);
      }
    } catch (Exception e) {
      LOGGER.warn(
          "Failed to refresh Credentials for stageName:{}, filePath:{}", stageName, fullFilePath);
      throw SnowflakeErrors.ERROR_5018.getException(e.getMessage());
    }

    // Lets fetch the file transfer metadata from cache again. (Because we might have refreshed
    // them)
    SnowflakeFileTransferMetadataV1 fileTransferMetadata =
        storageInfoCache.get(stageName).fileTransferMetadata;
    // Set filename to be uploaded
    // This set is not useful in GCS since there is a bug in JDBC which doesnt use destFileName.
    // TODO: https://snowflakecomputing.atlassian.net/browse/SNOW-350676
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
              // Setting a destinationFileName is a no-op for AWS and Azure since it still uses
              // presignedUrlFileName
              // Setting destFileName is useful for GCS and downscope URL
              .setDestFileName(FilenameUtils.getName(fullFilePath))
              .setOcspMode(OCSPMode.FAIL_OPEN)
              .setProxyProperties(proxyProperties)
              .build());
      LOGGER.info(
          "uploadWithoutConnection successful for stageName:{}, filePath:{}",
          stageName,
          fullFilePath,
          fullFilePath);
    } catch (Exception e) {
      // If this api encounters error, invalidate the cached credentials
      // Caller will retry this function
      LOGGER.warn(
          "uploadWithoutConnection encountered an exception:{} for filePath:{} in Storage:{}",
          e.getMessage(),
          fullFilePath,
          stageType);
      storageInfoCache.remove(stageName);
      throw SnowflakeErrors.ERROR_5018.getException(e.getMessage());
    }
  }

  /**
   * Check if credentials are valid before calling uploadWithoutConnection API. Valid if they are
   * not null and last fetch time was within {@link #expirationTimeMillis} For GCS, this will be
   * false and we always refresh the credentials.
   *
   * @param credential to check the validity for
   * @param stageType stageType of the stage fetched from JDBC and at least one GS call
   * @return true if we can reuse the credentials, false in case of GCS
   */
  private boolean isCredentialValid(
      SnowflakeMetadataWithExpiration credential, final StageInfo.StageType stageType) {

    if (stageType == StageInfo.StageType.GCS) {
      return false;
    }
    // Key is cached and not expired
    return credential != null
        && System.currentTimeMillis() - credential.timestampLastCredentialFetchMillis
            < expirationTimeMillis;
  }

  @VisibleForTesting
  protected void refreshCredentials(
      final String stageName, final StageInfo.StageType stageType, final String fullFilePath)
      throws SnowflakeSQLException {
    String putCommandToFetchMetadata =
        getDummyPutCommandTemplateForFileTransferMetadata(stageName, stageType, fullFilePath);

    // This should always be executed in GCS
    SnowflakeFileTransferAgent agent =
        new SnowflakeFileTransferAgent(
            putCommandToFetchMetadata, conn.getSfSession(), new SFStatement(conn.getSfSession()));
    // transfer metadata list must only have one element
    SnowflakeFileTransferMetadataV1 fileTransferMetadata =
        (SnowflakeFileTransferMetadataV1) agent.getFileTransferMetadatas().get(0);
    if (fileTransferMetadata.getStageInfo().getStageType() == StageInfo.StageType.LOCAL_FS) {
      LOGGER.error(
          "StageName:{} is not a valid stageType:{}",
          stageName,
          fileTransferMetadata.getStageInfo().getStageType());
      throw SnowflakeErrors.ERROR_5017.getException();
    } else {
      // Overwrite the credential to be used
      SnowflakeMetadataWithExpiration credential =
          new SnowflakeMetadataWithExpiration(fileTransferMetadata, System.currentTimeMillis());
      // Caching it here since we require to fetch the credential(Metadata) in the caller function
      // again.
      storageInfoCache.put(stageName, credential);
      LOGGER.debug("Caching credential successful for stage:{}", stageName);
    }
  }

  private String getDummyPutCommandTemplateForFileTransferMetadata(
      final String stageName, final StageInfo.StageType stageType, final String fullFilePath) {
    if (stageType == StageInfo.StageType.GCS) {
      return String.format(
          dummyPutCommandTemplateGCS,
          FilenameUtils.getName(fullFilePath), // Gets just the fileName
          stageName,
          FilenameUtils.getFullPathNoEndSeparator(
              fullFilePath)); // Gets everything leading up to the file
    } else {
      // AWS and Azure
      return String.format(dummyPutCommandTemplateAWSAndAzure, stageName);
    }
  }
}
