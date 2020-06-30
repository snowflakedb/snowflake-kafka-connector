package com.snowflake.kafka.connector.internal;

import net.snowflake.client.core.OCSPMode;
import net.snowflake.client.core.SFStatement;
import net.snowflake.client.jdbc.*;
import net.snowflake.client.jdbc.cloud.storage.StageInfo;
import net.snowflake.client.jdbc.internal.amazonaws.ClientConfiguration;
import net.snowflake.client.jdbc.internal.amazonaws.auth.BasicSessionCredentials;
import net.snowflake.client.jdbc.internal.amazonaws.retry.PredefinedRetryPolicies;
import net.snowflake.client.jdbc.internal.amazonaws.retry.RetryPolicy;
import net.snowflake.client.jdbc.internal.amazonaws.services.s3.AmazonS3Client;
import net.snowflake.client.jdbc.internal.amazonaws.services.s3.model.ObjectMetadata;
import net.snowflake.client.jdbc.internal.amazonaws.services.s3.model.PutObjectResult;
import net.snowflake.client.jdbc.internal.amazonaws.util.Base64;
import net.snowflake.client.jdbc.internal.snowflake.common.core.RemoteStoreFileEncryptionMaterial;

import javax.crypto.Cipher;
import javax.crypto.CipherOutputStream;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.*;
import java.lang.reflect.Array;
import java.security.SecureRandom;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPOutputStream;

public class SnowflakeInternalStage {
  private Map<String, Map<String, String>> storageInfoCache = new HashMap<>();

  private final int S3_MAX_RETRIES = 3;
  private final int S3_MAX_TIMEOUT_MS = 30000;
  private final String BUCKET_NAME = "bucketName";
  private final String AWS_ID = "awsId";
  private final String AWS_KEY = "awsKey";
  private final String AWS_TOKEN = "awsToken";
  private final String MASTER_KEY = "masterKey";
  private final String QUERY_ID = "queryId";
  private final String SMK_ID = "smkId";
  private final String PREFIX = "prefix";

  private final String AES = "AES";
  private final String AMZ_KEY = "x-amz-key";
  private final String AMZ_IV = "x-amz-iv";
  private final String DATA_CIPHER = "AES/CBC/PKCS5Padding";
  private final String KEY_CIPHER = "AES/ECB/PKCS5Padding";
  private final String AMZ_MATDESC = "x-amz-matdesc";

  public void putWithCache(SnowflakeConnectionV1 conn, String stage, String fileName, byte[] data)
  {
    try
    {
      if (!storageInfoCache.containsKey(stage))
      {
        storageInfoCache.put(stage, getStageStorageInfo(conn, stage));
      }

      OutputStream uploadStream = createUploadStream(fileName, storageInfoCache.get(stage));
      uploadStream.write(data);
      uploadStream.close();
    } catch (Exception e)
    {
      e.printStackTrace();
    }
  }

  public Map<String, String> getStageStorageInfo(SnowflakeConnectionV1 conn, String stage) throws Exception
  {
    String command = "PUT file:///tmp/dummy_location_kakfa_connector_tmp/ @" + stage;
    // This line will have one GS call
    SnowflakeFileTransferAgent agent = new SnowflakeFileTransferAgent(
      command,
      conn.getSfSession(),
      new SFStatement(conn.getSfSession())
    );
    System.out.println("One GS call");
    Map<?, ?> credentials = agent.getStageCredentials();

    RemoteStoreFileEncryptionMaterial encryptMaterials = agent.getEncryptionMaterial().get(0);

    String queryId = encryptMaterials.getQueryId();
    String smkId = encryptMaterials.getSmkId().toString();

    Map<String, String> storageInfo = new HashMap<>();

    storageInfo.put(QUERY_ID, queryId);
    storageInfo.put(SMK_ID, smkId);
    storageInfo.put(MASTER_KEY, encryptMaterials.getQueryStageMasterKey());

    String stageLocation = agent.getStageLocation();
    String url = "([^/]+)/?(.*)";
    Pattern pattern = Pattern.compile(url);
    Matcher matcher = pattern.matcher(stageLocation);
    matcher.find();
    String bucket = matcher.group(1);
    String path = matcher.group(2);

    storageInfo.put(BUCKET_NAME, bucket);
    storageInfo.put(AWS_ID, (String) credentials.get("AWS_ID"));
    storageInfo.put(AWS_KEY, (String) credentials.get("AWS_KEY"));
    storageInfo.put(AWS_TOKEN, (String) credentials.get("AWS_TOKEN"));

    String prefix;

    if (path.isEmpty()) prefix = path;
    else if (path.endsWith("/")) prefix = path;
    else prefix = path + "/";
    storageInfo.put(PREFIX, prefix);

    return storageInfo;
  }

  public OutputStream createUploadStream(String fileName, Map<String, String> storageInfo) throws Exception {

    AmazonS3Client s3Client = createS3Client(
        storageInfo.get(AWS_ID),
        storageInfo.get(AWS_KEY),
        storageInfo.get(AWS_TOKEN),
        1
      );

    CipherAndMetadata cipherAndMetadata =
      getCipherAndMetadata(
        storageInfo.get(MASTER_KEY),
        storageInfo.get(QUERY_ID),
        storageInfo.get(SMK_ID));

    Cipher fileCipher = cipherAndMetadata.fileCipher;
    String matDesc = cipherAndMetadata.matDesc;
    String encKeK = cipherAndMetadata.encKeK;
    String ivData = cipherAndMetadata.ivData;

    ObjectMetadata meta = new ObjectMetadata();
    meta.addUserMetadata(AMZ_MATDESC, matDesc);
    meta.addUserMetadata(AMZ_KEY, encKeK);
    meta.addUserMetadata(AMZ_IV, ivData);

    meta.setContentEncoding("GZIP");

    OutputStream outputStream = new SnowflakeOutputStream(meta, s3Client, fileName, storageInfo);

    outputStream = new CipherOutputStream(outputStream, fileCipher);

    return new GZIPOutputStream(outputStream);
  }

  public AmazonS3Client createS3Client(String awsId, String awsKey, String awsToken, int parallelism) {
    BasicSessionCredentials awsCredentials = new BasicSessionCredentials(awsId, awsKey, awsToken);

    ClientConfiguration clientConfig = new ClientConfiguration();
    clientConfig.setMaxConnections(parallelism);
    clientConfig.setMaxErrorRetry(S3_MAX_RETRIES);

    clientConfig.setRetryPolicy(
      new RetryPolicy(
        PredefinedRetryPolicies.DEFAULT_RETRY_CONDITION,
        PredefinedRetryPolicies.DEFAULT_BACKOFF_STRATEGY,
        S3_MAX_RETRIES,
        true
      )
    );

    clientConfig.setConnectionTimeout(S3_MAX_TIMEOUT_MS);


    return new AmazonS3Client(awsCredentials, clientConfig);
  }

  public CipherAndMetadata getCipherAndMetadata(String masterKey, String queryId, String smkId) throws Exception {

    byte[] decodedKey = Base64.decode(masterKey);
    int keySize = decodedKey.length;
    byte[] fileKeyBytes = new byte[keySize];
    Cipher fileCipher = Cipher.getInstance(DATA_CIPHER);
    int blockSz = fileCipher.getBlockSize();
    byte[] ivData = new byte[blockSz];

    SecureRandom secRnd = SecureRandom.getInstance("SHA1PRNG", "SUN");
    secRnd.nextBytes(new byte[10]);

    secRnd.nextBytes(ivData);
    IvParameterSpec iv = new IvParameterSpec(ivData);

    secRnd.nextBytes(fileKeyBytes);
    SecretKeySpec fileKey = new SecretKeySpec(fileKeyBytes, 0, keySize, AES);

    fileCipher.init(Cipher.ENCRYPT_MODE, fileKey, iv);

    Cipher keyCipher = Cipher.getInstance(KEY_CIPHER);
    SecretKeySpec queryStageMasterKey = new SecretKeySpec(decodedKey, 0, keySize, AES);

    // Init cipher
    keyCipher.init(Cipher.ENCRYPT_MODE, queryStageMasterKey);
    byte[] encKeK = keyCipher.doFinal(fileKeyBytes);

    MatDesc matDesc = new MatDesc(Long.parseLong(smkId), queryId, keySize * 8);

    return new CipherAndMetadata(
      fileCipher,
      matDesc.toString(),
      Base64.encodeAsString(encKeK),
      Base64.encodeAsString(ivData)
    );
  }

  class CipherAndMetadata
  {
    public Cipher fileCipher;
    public String matDesc;
    public String encKeK;
    public String ivData;

    public CipherAndMetadata(Cipher fileCipher, String matDesc, String encKeK, String ivData)
    {
      this.fileCipher = fileCipher;
      this.matDesc = matDesc;
      this.encKeK = encKeK;
      this.ivData = ivData;
    }
  }

  class SnowflakeOutputStream extends OutputStream
  {
    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    ObjectMetadata meta;
    AmazonS3Client s3Client;
    String file;
    Map<String, String> storageInfo;

    SnowflakeOutputStream(ObjectMetadata meta, AmazonS3Client s3Client, String file, Map<String, String> storageInfo) {
      this.meta = meta;
      this.s3Client = s3Client;
      this.file = file;
      this.storageInfo = storageInfo;
    }

    @Override
    public void write(int b)
    {
      buffer.write(b);
    }

    @Override
    public void close() throws IOException
    {
      buffer.close();
      byte[] resultByteArray = buffer.toByteArray();
      // Set up length to avoid S3 client API to raise warning message.
      meta.setContentLength(resultByteArray.length);
      InputStream inputStream = new ByteArrayInputStream(resultByteArray);
      PutObjectResult res = s3Client.putObject(
        storageInfo.get(BUCKET_NAME),
        storageInfo.get(PREFIX).concat(file),
        inputStream,
        meta
      );
      System.out.println(res);
    }
  }
}
