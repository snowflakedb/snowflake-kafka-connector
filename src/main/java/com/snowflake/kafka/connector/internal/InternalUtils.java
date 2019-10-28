package com.snowflake.kafka.connector.internal;

import com.snowflake.kafka.connector.Utils;
import net.snowflake.ingest.connection.IngestStatus;
import org.bouncycastle.asn1.pkcs.PrivateKeyInfo;
import org.bouncycastle.jcajce.provider.BouncyCastleFipsProvider;
import org.bouncycastle.openssl.PEMKeyPair;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;
import org.bouncycastle.openssl.jcajce.JceOpenSSLPKCS8DecryptorProviderBuilder;
import org.bouncycastle.operator.InputDecryptorProvider;
import org.bouncycastle.pkcs.PKCS8EncryptedPrivateKeyInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.StringReader;
import java.security.KeyFactory;
import java.security.PrivateKey;
import java.security.Security;
import java.security.spec.PKCS8EncodedKeySpec;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;

class InternalUtils
{
  //JDBC parameter list
  static final String JDBC_DATABASE = "db";
  static final String JDBC_SCHEMA = "schema";
  static final String JDBC_USER = "user";
  static final String JDBC_PRIVATE_KEY = "privateKey";
  static final String JDBC_SSL = "ssl";
  static final String JDBC_SESSION_KEEP_ALIVE =
    "client_session_keep_alive";
  static final String JDBC_WAREHOUSE = "warehouse"; //for test only

  //internal parameters
  static final long MAX_RECOVERY_TIME = 10 * 24 * 3600 * 1000; //10 days

  private static final Logger LOGGER =
    LoggerFactory.getLogger(InternalUtils.class.getName());

  /**
   * count the size of result set
   *
   * @param resultSet sql result set
   * @return size
   * @throws SQLException when failed to read result set
   */
  static int resultSize(ResultSet resultSet) throws SQLException
  {
    int size = 0;
    while (resultSet.next())
    {
      size++;
    }
    return size;
  }

  static void assertNotEmpty(String name, Object value)
  {
    if (value == null || (value instanceof String && value.toString().isEmpty()))
    {
      switch (name.toLowerCase())
      {
        case "tablename":
          throw SnowflakeErrors.ERROR_0005.getException();
        case "stagename":
          throw SnowflakeErrors.ERROR_0004.getException();
        case "pipename":
          throw SnowflakeErrors.ERROR_0006.getException();
        case "conf":
          throw SnowflakeErrors.ERROR_0001.getException();
        default:
          throw SnowflakeErrors.ERROR_0003.getException("parameter name: " + name);
      }
    }
  }

  static PrivateKey parseEncryptedPrivateKey(String key, String passphrase)
  {
    //remove header, footer, and line breaks
    key = key.replaceAll("-+[A-Za-z ]+-+", "");
    key = key.replaceAll("\\s", "");

    StringBuilder builder = new StringBuilder();
    builder.append("-----BEGIN ENCRYPTED PRIVATE KEY-----");
    for (int i = 0; i < key.length(); i++)
    {
      if (i % 64 == 0)
      {
        builder.append("\n");
      }
      builder.append(key.charAt(i));
    }
    builder.append("\n-----END ENCRYPTED PRIVATE KEY-----");
    key = builder.toString();
    Security.addProvider(new BouncyCastleFipsProvider());
    try
    {
      PEMParser pemParser = new PEMParser(new StringReader(key));
      PKCS8EncryptedPrivateKeyInfo encryptedPrivateKeyInfo =
        (PKCS8EncryptedPrivateKeyInfo) pemParser.readObject();
      pemParser.close();
      InputDecryptorProvider pkcs8Prov =
        new JceOpenSSLPKCS8DecryptorProviderBuilder().build(passphrase.toCharArray());
      JcaPEMKeyConverter converter =
        new JcaPEMKeyConverter().setProvider(BouncyCastleFipsProvider.PROVIDER_NAME);
      PrivateKeyInfo decryptedPrivateKeyInfo =
        encryptedPrivateKeyInfo.decryptPrivateKeyInfo(pkcs8Prov);
      return converter.getPrivateKey(decryptedPrivateKeyInfo);
    } catch (Exception e)
    {
      throw SnowflakeErrors.ERROR_0018.getException(e);
    }
  }

  static PrivateKey parsePrivateKey(String key)
  {
    //remove header, footer, and line breaks
    key = key.replaceAll("-+[A-Za-z ]+-+", "");
    key = key.replaceAll("\\s", "");

    StringBuilder builder = new StringBuilder();
    builder.append("-----BEGIN RSA PRIVATE KEY-----");
    for (int i = 0; i < key.length(); i++)
    {
      if (i % 64 == 0)
      {
        builder.append("\n");
      }
      builder.append(key.charAt(i));
    }
    builder.append("\n-----END RSA PRIVATE KEY-----");
    key = builder.toString();
    try
    {
      PEMParser pemParser = new PEMParser(new StringReader(key));
      PEMKeyPair pemKeyPair = (PEMKeyPair) pemParser.readObject();
      PKCS8EncodedKeySpec keySpec =
        new PKCS8EncodedKeySpec(pemKeyPair.getPrivateKeyInfo().getEncoded());
      KeyFactory keyFactory = KeyFactory.getInstance("RSA");
      return keyFactory.generatePrivate(keySpec);
    } catch (Exception e)
    {
      throw SnowflakeErrors.ERROR_0002.getException(e);
    }
  }

  /**
   * convert a timestamp to Date String
   *
   * @param time a long integer representing timestamp
   * @return date string
   */
  static String timestampToDate(long time)
  {
    TimeZone tz = TimeZone.getTimeZone("UTC");

    DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");

    df.setTimeZone(tz);

    String date = df.format(new Date(time));

    LOGGER.debug(Logging.logMessage("converted date: {}", date));

    return date;
  }

  /**
   * create a properties for snowflake connection
   *
   * @param conf a map contains all parameters
   * @return a Properties instance
   */
  static Properties createProperties(Map<String, String> conf)
  {
    Properties properties = new Properties();

    //decrypt rsa key
    String privateKey = "";
    String privateKeyPassphrase = "";

    for (Map.Entry<String, String> entry : conf.entrySet())
    {
      //case insensitive
      switch (entry.getKey().toLowerCase())
      {
        case Utils.SF_DATABASE:
          properties.put(JDBC_DATABASE, entry.getValue());
          break;
        case Utils.SF_PRIVATE_KEY:
          privateKey = entry.getValue();
          break;
        case Utils.SF_SCHEMA:
          properties.put(JDBC_SCHEMA, entry.getValue());
          break;
        case Utils.SF_USER:
          properties.put(JDBC_USER, entry.getValue());
          break;
        case Utils.SF_WAREHOUSE:
          properties.put(JDBC_WAREHOUSE, entry.getValue());
          break;
        case Utils.PRIVATE_KEY_PASSPHRASE:
          privateKeyPassphrase = entry.getValue();
          break;
        default:
          //ignore unrecognized keys
      }
    }

    if (!privateKeyPassphrase.isEmpty())
    {
      properties.put(JDBC_PRIVATE_KEY, parseEncryptedPrivateKey(privateKey,
        privateKeyPassphrase));
    }
    else if (!privateKey.isEmpty())
    {
      properties.put(JDBC_PRIVATE_KEY, parsePrivateKey(privateKey));
    }

    //put values for optional parameters
    properties.put(JDBC_SSL, "on");
    properties.put(JDBC_SESSION_KEEP_ALIVE, "true");

    //required parameter check
    if (!properties.containsKey(JDBC_PRIVATE_KEY))
    {
      throw SnowflakeErrors.ERROR_0013.getException();
    }

    if (!properties.containsKey(JDBC_SCHEMA))
    {
      throw SnowflakeErrors.ERROR_0014.getException();
    }

    if (!properties.containsKey(JDBC_DATABASE))
    {
      throw SnowflakeErrors.ERROR_0015.getException();
    }

    if (!properties.containsKey(JDBC_USER))
    {
      throw SnowflakeErrors.ERROR_0016.getException();
    }

    return properties;
  }

  /**
   * convert ingest status to ingested file status
   *
   * @param status an ingest status
   * @return an ingest file status
   */
  static IngestedFileStatus convertIngestStatus(IngestStatus status)
  {
    switch (status)
    {
      case LOADED:
        return IngestedFileStatus.LOADED;

      case LOAD_IN_PROGRESS:
        return IngestedFileStatus.LOAD_IN_PROGRESS;

      case PARTIALLY_LOADED:
        return IngestedFileStatus.PARTIALLY_LOADED;

      case LOAD_FAILED:

      default:
        return IngestedFileStatus.FAILED;
    }

  }

  /**
   * ingested file status
   * some status are grouped as 'finalized' status (LOADED, PARTIALLY_LOADED,
   * FAILED) -- we can purge these files
   * others are grouped as 'not_finalized'
   */
  enum IngestedFileStatus    // for ingest sdk
  {
    LOADED,
    PARTIALLY_LOADED,
    FAILED,
    // partially_loaded, or failed
    LOAD_IN_PROGRESS,
    NOT_FOUND,
  }
}
