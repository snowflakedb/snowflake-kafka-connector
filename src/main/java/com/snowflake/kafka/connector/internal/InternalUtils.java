package com.snowflake.kafka.connector.internal;

import com.snowflake.kafka.connector.Utils;
import net.snowflake.client.jdbc.internal.apache.commons.codec.binary.Base64;
import net.snowflake.client.jdbc.internal.org.bouncycastle.jce.provider.BouncyCastleProvider;
import net.snowflake.ingest.connection.IngestStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.KeyFactory;
import java.security.PrivateKey;
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

  // backoff with 1, 2, 4, 8 seconds
  public static final int backoffSec[] = {0, 1, 2, 4, 8};

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

  static PrivateKey parsePrivateKey(String key)
  {
    //remove header, footer, and line breaks
    key = key.replaceAll("-+[A-Za-z ]+-+", "");
    key = key.replaceAll("\\s", "");

    java.security.Security.addProvider(new BouncyCastleProvider());
    byte[] encoded = Base64.decodeBase64(key);
    try
    {
      KeyFactory kf = KeyFactory.getInstance("RSA");
      PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(encoded);
      return kf.generatePrivate(keySpec);
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
   * @param sslEnabled if ssl is enabled
   * @return a Properties instance
   */
  static Properties createProperties(Map<String, String> conf, boolean sslEnabled)
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
      properties.put(JDBC_PRIVATE_KEY,
        EncryptionUtils.parseEncryptedPrivateKey(privateKey,
          privateKeyPassphrase));
    }
    else if (!privateKey.isEmpty())
    {
      properties.put(JDBC_PRIVATE_KEY, parsePrivateKey(privateKey));
    }

    // set ssl
    if (sslEnabled)
    {
      properties.put(JDBC_SSL, "on");
    }
    else
    {
      properties.put(JDBC_SSL, "off");
    }
    //put values for optional parameters
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

  /**
   * Interfaces to define the lambda function to be used by backoffAndRetry
   */
  interface backoffFunction
  {
    Object apply() throws Exception;
  }

  /**
   * Backoff logic
   * @param telemetry telemetry service
   * @param runnable the lambda function itself
   * @return the object that the function returns
   * @throws Exception
   */
  public static Object backoffAndRetry(final SnowflakeTelemetryService telemetry, final backoffFunction runnable) throws Exception
  {
    for (final int iteration : backoffSec)
    {
      if (iteration != 0)
      {
        Thread.sleep(iteration * 1000);
      }
      try
      {
        return runnable.apply();
      }
      catch (Exception e)
      {
        LOGGER.error(e.getMessage());
        telemetry.reportKafkaSnowflakeThrottle(e.getMessage(), iteration);
      }
    }
    throw SnowflakeErrors.ERROR_2010.getException();
  }
}
