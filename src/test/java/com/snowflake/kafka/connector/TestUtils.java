/*
 * Copyright (c) 2019 Snowflake Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.snowflake.kafka.connector;

import com.snowflake.client.jdbc.SnowflakeDriver;
import com.snowflake.kafka.connector.internal.SnowflakeJDBCWrapper;
import com.snowflake.kafka.connector.internal.SnowflakeURL;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind
    .ObjectMapper;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.node
    .ObjectNode;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import net.snowflake.client.jdbc.internal.apache.commons.codec.binary.Base64;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyFactory;
import java.security.PrivateKey;
import java.security.spec.PKCS8EncodedKeySpec;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;


public class TestUtils
{
  //test profile properties
  static String USER = "user";

  static String DATABASE = "database";

  static String SCHEMA = "schema";

  static String HOST = "host";

  static String SSL = "ssl";

  static String WAREHOUSE = "warehouse";

  static String PRIVATE_KEY = "private_key";


  //profile path
  private final static String PROFILE_PATH = "profile.json";

  private final static ObjectMapper mapper = new ObjectMapper();

  private static Connection conn = null;

  private static Map<String, String> conf = null;

  private static SnowflakeURL url = null;


  /**
   * load all login info from profile
   *
   * @throws IOException if can't read profile
   */
  private static void init() throws Exception
  {

    ObjectNode profile = (ObjectNode) mapper.readTree(
        new String(Files.readAllBytes(Paths.get(PROFILE_PATH)))
    );

    conf = new HashMap<>();

    conf.put(Utils.SF_USER, profile.get(USER).asText());
    conf.put(Utils.SF_DATABASE, profile.get(DATABASE).asText());
    conf.put(Utils.SF_SCHEMA, profile.get(SCHEMA).asText());
    conf.put(Utils.SF_URL, profile.get(HOST).asText());
    conf.put(Utils.SF_WAREHOUSE, profile.get(WAREHOUSE).asText());
    conf.put(Utils.SF_PRIVATE_KEY, profile.get(PRIVATE_KEY).asText());

    //enable test query mark
    conf.put(Utils.TASK_ID, "");
  }


  /**
   * Create snowflake jdbc connection
   *
   * @return jdbc connection
   * @throws Exception
   */
  public static Connection getConnection() throws Exception
  {
    if (conn != null)
    {
      return conn;
    }

    Properties properties = SnowflakeJDBCWrapper.createProperties(getConf());

    SnowflakeURL url = (SnowflakeURL) properties.get("url");

    properties.remove("url");

    conn = new SnowflakeDriver().connect(url.getJdbcUrl(), properties);

    return conn;
  }

  /**
   * read conf file
   *
   * @return a map of parameters
   * @throws Exception
   */
  public static Map<String, String> getConf() throws Exception
  {
    if (conf == null)
    {
      init();
    }
    return new HashMap<>(conf);
  }

  /**
   * execute sql query
   *
   * @param query sql query string
   * @return result set
   */
  public static ResultSet executeQuery(String query)
  {
    try
    {
      Statement statement = getConnection().createStatement();

      return statement.executeQuery(query);
    }
    //if ANY exceptions occur, an illegal state has been reached
    catch (Exception e)
    {
      throw new IllegalStateException(e);
    }
  }

  /**
   * drop a table
   *
   * @param tableName table name
   */
  public static void dropTable(String tableName)
  {
    String query = "drop table if exists " + tableName;

    executeQuery(query);
  }

  /**
   * Select * from table
   */
  public static ResultSet showTable(String tableName)
  {
    String query = "select * from " + tableName;

    return executeQuery(query);
  }

  /**
   * create a random name for test
   *
   * @param objectName e.g. table, stage, pipe
   * @return kafka_connector_test_objectName_randomNum
   */
  private static String randomName(String objectName)
  {
    long num = Math.abs(new Random().nextLong());

    String name = "kafka_connector_test_" + objectName + "_" + num;

    return name;
  }

  /**
   * @return a random table name
   */
  public static String randomTableName()
  {
    return randomName("table");
  }

  /**
   * @return a random stage name
   */
  public static String randomStageName()
  {
    return randomName("stage");
  }

  /**
   * @return a random pipe name
   */
  public static String randomPipeName()
  {
    return randomName("pipe");
  }

  /**
   * retrieve one properties
   *
   * @param name property name
   * @return property value
   * @throws Exception
   */
  private static String get(String name) throws Exception
  {
    Map<String, String> properties = getConf();

    return properties.get(name);
  }

  public static String getUser() throws Exception
  {
    return get(Utils.SF_USER);
  }

  public static String getDatabase() throws Exception
  {
    return get(Utils.SF_DATABASE);
  }

  public static String getSchema() throws Exception
  {
    return get(Utils.SF_SCHEMA);
  }

  public static boolean sslEnabled() throws Exception
  {
    return get(Utils.SF_SSL).equals("on");
  }

  public static SnowflakeURL getUrl() throws Exception
  {
    if (url == null)
    {
      url = new SnowflakeURL(get(Utils.SF_URL));
    }
    return url;
  }

  public static PrivateKey getPrivateKey() throws Exception
  {
    java.security.Security.addProvider(
        new net.snowflake.client.jdbc.internal.org.bouncycastle.jce
            .provider.BouncyCastleProvider()
    );

    byte[] encoded = Base64.decodeBase64(get(Utils.SF_PRIVATE_KEY));

    KeyFactory kf = KeyFactory.getInstance("RSA");

    PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(encoded);

    return kf.generatePrivate(keySpec);
  }

  public static String getFullPipeName(String pipeName) throws Exception
  {
    return TestUtils.getDatabase() + "." + TestUtils.getSchema() + "." +
        pipeName;
  }

  public static byte[] jsonToAvro(String json, String schemaStr) throws
      IOException
  {
    InputStream input = null;
    GenericDatumWriter writer = null;
    Encoder encoder = null;
    ByteArrayOutputStream output = null;

    try
    {
      Schema sc = new Schema.Parser().parse(schemaStr);
      DatumReader<GenericRecord> reader = new
          GenericDatumReader<GenericRecord>(sc);
      input = new ByteArrayInputStream(json.getBytes());
      output = new ByteArrayOutputStream();
      writer = new GenericDatumWriter<GenericRecord>(sc);
      DataInputStream din = new DataInputStream(input);
      Decoder decoder = DecoderFactory.get().jsonDecoder(sc, din);
      encoder = EncoderFactory.get().binaryEncoder(output, null);
      GenericRecord datum;
      while (true)
      {
        try
        {
          datum = reader.read(null, decoder);
        } catch (EOFException eof)
        {
          break;
        }
        writer.write(datum, encoder);
      }
      encoder.flush();
      return output.toByteArray();
    } finally
    {
      try
      {
        input.close();
      } catch (IOException e)
      {
        e.printStackTrace();
      }
    }
  }
}

