package com.snowflake.jdbc;

import java.io.File;
import java.io.IOException;
import java.security.KeyFactory;
import java.security.PrivateKey;
import java.security.spec.PKCS8EncodedKeySpec;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import com.snowflake.Utils;

import org.bouncycastle.jce.provider.BouncyCastleProvider;

import net.snowflake.client.jdbc.SnowflakeConnectionV1;
import net.snowflake.client.jdbc.internal.apache.commons.codec.binary.Base64;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.core.JsonProcessingException;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.JsonNode;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.ObjectMapper;

public class ConnectionUtils
{
  private static final String CONF_FILE_NAME = "config/snowflake.json";

  public static final String URL = "url";
  private static final String WAREHOUSE = "warehouse";
  public static final String DATABASE = "database";
  private static final String DB = "db";
  public static final String SCHEMA = "schema";
  public static final String USER = "user";
  public static final String PRIVATE_KEY = "private_key";
  private static final String P_KEY = "privateKey";
  private static final String PASSWORD = "password";
  private static final String RESULT_STAGE = "KAFKA_PERF_TEST_STAGE";
  private static final String RESULT_TABLE = "DO_NOT_DELETE_KAFKA_PERF_TEST_RESULT_TABLE";

  private static ObjectMapper MAPPER = new ObjectMapper();

  private static Connection testConn = null;
//  private static Connection snowhouseConn = null;

  private static Connection getTestServerConnection()
  {
    if (testConn == null)
    {
      initConnection();
    }
    return testConn;
  }

//  private static Connection getSnowhouseConnection()
//  {
//    if (snowhouseConn == null)
//    {
//      initConnection();
//    }
//    return snowhouseConn;
//  }

  public static void uploadReport()
  {
    runQuery(
      "create temp stage " + RESULT_STAGE
    );

    runQuery(
      "create table if not exists " + RESULT_TABLE + " ( KAFKA_PERF_TEST_RESULT VARIANT )"
    );

    runQuery(
      "put file://" + Utils.REPORT_FILE_PATH + " @" + RESULT_STAGE
    );

    runQuery(
      "copy into " + RESULT_TABLE + " from @" + RESULT_STAGE + " file_format = (type = json)"
    );

  }

  private static void initConnection()
  {
    Properties testProps = getServerProps("test");
//    Properties snowhouseProps = getServerProps("snowhouse");
    try
    {
      testConn = new SnowflakeConnectionV1(testProps.getProperty(URL),
        testProps);
//      snowhouseConn =
//        new SnowflakeConnectionV1(snowhouseProps.getProperty(URL),
//          snowhouseProps);
    } catch (SQLException e)
    {
      e.printStackTrace();
      System.exit(1);
    }

  }

  private static Properties getServerProps(String serverName)
  {
    Properties props = new Properties();
    JsonNode config;
    try
    {
      config = getConfig().get(serverName);
    } catch (Exception e)
    {
      e.printStackTrace();
      System.exit(1);
      config = MAPPER.createObjectNode();
    }

    props.put(DB, config.get(DATABASE).asText());
    props.put(SCHEMA, config.get(SCHEMA).asText());
    props.put(USER, config.get(USER).asText());
    props.put(WAREHOUSE, config.get(WAREHOUSE).asText());
    props.put(URL, "jdbc:snowflake://" + config.get(URL).asText() + ":443");
    if (config.has(PRIVATE_KEY))
    {
      java.security.Security.addProvider(new BouncyCastleProvider());
      byte[] encoded = Base64.decodeBase64(config.get(PRIVATE_KEY).asText());
      PrivateKey key;
      try
      {
        KeyFactory kf = KeyFactory.getInstance("RSA");
        PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(encoded);
        key = kf.generatePrivate(keySpec);

      } catch (Exception e)
      {
        e.printStackTrace();
        System.exit(1);
        key = null;
      }

      props.put(P_KEY, key);
    }
    if (config.has(PASSWORD))
    {
      props.put(PASSWORD, config.get(PASSWORD).asText());
    }

    return props;
  }

  private static JsonNode getConfig() throws IOException
  {
    File file = new File(CONF_FILE_NAME);
    return MAPPER.readTree(file);
  }

  public static void dropTestTable()
  {
    runQuery(
      "drop table if exists " + Utils.TEST_TOPIC
    );
  }

  public static void dropTestStage()
  {
    runQuery(
      "drop stage if exists SNOWFLAKE_KAFKA_CONNECTOR_" +
        Utils.CONNECTOR_NAME + "_STAGE_" + Utils.TEST_TOPIC
    );
  }

  public static void dropTestPipe()
  {
    runQuery(
      "drop pipe if exists SNOWFLAKE_KAFKA_CONNECTOR_" +
        Utils.CONNECTOR_NAME + "_PIPE_" + Utils.TEST_TOPIC + "_0"
    );
  }

  public static int tableSize()
  {
    ResultSet result = runQuery(
      "show tables like '" + Utils.TEST_TOPIC + "'"
    );
    try
    {
      if (result.next())
      {
        return result.getInt("rows");
      }
    } catch (SQLException e)
    {
      e.printStackTrace();
      System.exit(1);
    }

    return 0;
  }

  private static ResultSet runQuery(String query)
  {
    return runQuery(query, getTestServerConnection());
  }

  private static ResultSet runQuery(String query, Connection conn)
  {
    try
    {
      return conn.createStatement().executeQuery(query);
    } catch (SQLException e)
    {
      e.printStackTrace();
      System.exit(1);
      return null;
    }
  }

  public static void close()
  {
    try
    {
      getTestServerConnection().close();
//      getSnowhouseConnection().close();
    } catch (SQLException e)
    {
      e.printStackTrace();
    }

  }


}