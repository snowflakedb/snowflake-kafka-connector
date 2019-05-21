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

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.rmi.UnexpectedException;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import net.snowflake.client.jdbc.SnowflakeConnectionV1;
import org.apache.commons.codec.binary.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Snowflake JDBC helper functions
 */
class SnowflakeJDBCWrapper
{

  private Connection conn;
  private Properties properties;
  private boolean closed;
  private final SnowflakeURL url;

  private static final Logger LOGGER =
    LoggerFactory.getLogger(SnowflakeJDBCWrapper.class.getName());

  //pipe manager
  private final HashMap<String, SnowflakeIngestService> pipes;

  //telemetry
  private final SnowflakeTelemetry telemetry;

  //JDBC parameter list
  private static final String JDBC_DATABASE = "db";
  private static final String JDBC_SCHEMA = "schema";
  private static final String JDBC_USER = "user";
  private static final String JDBC_PRIVATE_KEY = "privateKey";
  private static final String JDBC_SSL = "ssl";
  private static final String JDBC_ACCOUNT = "account";
  private static final String JDBC_ROLE = "role";
  private static final String JDBC_SESSION_KEEP_ALIVE =
    "client_session_keep_alive";
  private static final String JDBC_WAREHOUSE = "warehouse"; //for test only
  private static final String URL = "url";


  private final String queryMark;

  /**
   * Create a Snowflake connection
   *
   * @param conf a map contains all configurations
   * @throws ClassNotFoundException if snowflake jdbc class not exists
   */
  SnowflakeJDBCWrapper(Map<String, String> conf)
    throws SQLException
  {
    if (conf == null)
    {
      LOGGER.error("input conf is null");
      throw new IllegalArgumentException("input conf is null");
    }

    //init query mark
    if (conf.containsKey(Utils.TASK_ID))
    {
      if (conf.get(Utils.TASK_ID).isEmpty())
      {
        queryMark = Utils.getTestQueryMark();
      }
      else
      {
        queryMark = Utils.getTaskQueryMark(conf.get(Utils.TASK_ID));
      }
    }
    else
    {
      queryMark = Utils.getConnectorQueryMark();
    }

    properties = createProperties(conf);

    url = (SnowflakeURL) properties.get(URL);

    properties.remove(URL);

    this.conn = new SnowflakeDriver().connect(url.getJdbcUrl(), properties);

    this.closed = false;

    pipes = new HashMap<>();

    telemetry = new SnowflakeTelemetry((SnowflakeConnectionV1) this.conn);

    LOGGER.info("initialized the snowflake JDBC");
  }

  /**
   * create a jdbc properties for LOGGER in
   *
   * @param conf a map contains all parameters
   * @return a Properties instance
   */
  static Properties createProperties(Map<String, String> conf)
  {
    Properties properties = new Properties();

    for (Map.Entry<String, String> entry : conf.entrySet())
    {
      //case insensitive
      switch (entry.getKey().toLowerCase())
      {
        case Utils.SF_DATABASE:
          properties.put(JDBC_DATABASE, entry.getValue());
          break;

        case Utils.SF_PRIVATE_KEY:
          java.security.Security.addProvider(
            new net.snowflake.client.jdbc.internal.org.bouncycastle.jce
              .provider.BouncyCastleProvider()
          );
          byte[] encoded = Base64.decodeBase64(entry.getValue());
          try
          {
            KeyFactory kf = KeyFactory.getInstance("RSA");
            PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(encoded);
            properties.put(JDBC_PRIVATE_KEY, kf.generatePrivate(keySpec));
          } catch (Exception e)
          {
            throw
              new IllegalArgumentException(
                "Input PEM private key is invalid\n" + e.toString());
          }
          break;

        case Utils.SF_ROLE:
          properties.put(JDBC_ROLE, entry.getValue());
          break;

        case Utils.SF_SCHEMA:
          properties.put(JDBC_SCHEMA, entry.getValue());
          break;

        case Utils.SF_URL:

          SnowflakeURL url = new SnowflakeURL(entry.getValue());

          properties.put(JDBC_ACCOUNT, url.getAccount());

          properties.put(URL, url);

          break;

        case Utils.SF_USER:
          properties.put(JDBC_USER, entry.getValue());
          break;

        case Utils.SF_WAREHOUSE:
          properties.put(JDBC_WAREHOUSE, entry.getValue());
          break;
      }
    }

    //put values for optional parameters
    properties.put(JDBC_SSL, "on");
    properties.put(JDBC_SESSION_KEEP_ALIVE, "true");

    //required parameter check
    if (!properties.containsKey(JDBC_PRIVATE_KEY))
    {
      LOGGER.error("config doesn't contain private key");
      throw new IllegalArgumentException(
        "A PEM private key must be provided with " + Utils.SF_PRIVATE_KEY +
          " parameter");
    }

    if (!properties.containsKey(JDBC_SCHEMA))
    {
      LOGGER.error("config doesn't contain schema name");
      throw new IllegalArgumentException(
        "A snowflake schema must be provided with " + Utils.SF_SCHEMA +
          " parameter");
    }

    if (!properties.containsKey(JDBC_DATABASE))
    {
      LOGGER.error("config doesn't contain database name");
      throw new IllegalArgumentException(
        "A snowflake database must be provided with "
          + Utils.SF_DATABASE + " parameter");
    }

    if (!properties.containsKey(JDBC_DATABASE))
    {
      LOGGER.error("config doesn't contain database name");
      throw new IllegalArgumentException(
        "A snowflake database must be provided with "
          + Utils.SF_DATABASE + " parameter");
    }


    if (!properties.containsKey(JDBC_USER))
    {
      LOGGER.error("config doesn't contain user name");
      throw new IllegalArgumentException(
        "A snowflake user name must be provided with "
          + Utils.SF_DATABASE + " parameter");
    }

    if (!properties.containsKey(URL))
    {
      LOGGER.error("config doesn't contain database name");
      throw new IllegalArgumentException(
        "A snowflake URL must be provided with " + Utils.SF_URL +
          " parameter, e.g. 'accountname.snoflakecomputing.com'");
    }

    return properties;
  }

  /**
   * Create a table with two variant columns: RECORD_METADATA and RECORD_CONTENT
   *
   * @param tableName a string represents table name
   * @param overwrite if true, execute "create or replace table" query;
   *                  otherwise, run "create table if not exists"
   * @return table name
   */
  String createTable(String tableName, boolean overwrite)
    throws SQLException
  {
    checkConnection();

    if (tableName == null || tableName.isEmpty())
    {
      LOGGER.error("Empty table name");
      throw new IllegalArgumentException("table name must not be null or " +
        "empty");
    }

    String query;

    if (overwrite)
    {
      query = "create or replace table identifier(?) (record_metadata " +
        "variant, record_content variant)";
    }
    else
    {
      query = "create table if not exists identifier(?) (record_metadata " +
        "variant, record_content variant)";
    }

    query += queryMark;

    PreparedStatement stmt = conn.prepareStatement(query);

    stmt.setString(1, tableName);

    stmt.execute();

    LOGGER.info("create table {}", tableName);

    return tableName;
  }

  /**
   * create table is not exists
   *
   * @param tableName table name
   * @return table name
   * @throws SQLException
   */
  String createTable(String tableName) throws SQLException
  {
    return createTable(tableName, false);
  }

  /**
   * create a snowpipe
   *
   * @param pipeName  pipe name
   * @param tableName table name
   * @param stageName stage name
   * @param overwrite if true, execute "create or replace pipe" statement,
   *                  otherwise, run "create pipe if not exists"
   * @return pipe name
   * @throws SQLException
   */
  String createPipe(String pipeName, String tableName, String stageName,
                    boolean overwrite) throws
    SQLException
  {
    checkConnection();

    if (pipeName == null || pipeName.isEmpty())
    {
      LOGGER.error("pipe name is empty");
      throw new IllegalArgumentException("pipe name must not be null or empty");
    }

    if (tableName == null || tableName.isEmpty())
    {
      LOGGER.error("table name is empty");
      throw new IllegalArgumentException("table name must not be null or " +
        "empty");
    }

    if (stageName == null || stageName.isEmpty())
    {
      LOGGER.error("stage name is empty");
      throw new IllegalArgumentException("stage name must not be null or " +
        "empty");
    }

    String query;

    if (overwrite)
    {
      query = "create or replace pipe identifier(?) ";
    }
    else
    {
      query = "create pipe if not exists identifier(?) ";
    }

    query += "as " + pipeDefinition(tableName, stageName);

    query += queryMark;

    PreparedStatement stmt = conn.prepareStatement(query);

    stmt.setString(1, pipeName);

    stmt.execute();

    //create ingest service
    String pipeFullName = properties.get(JDBC_DATABASE) + "." +
      properties.get(JDBC_SCHEMA) + "." + pipeName;

    try
    {
      pipes.put(pipeName, createIngestConnection(pipeFullName, stageName));
    } catch (Exception e)
    {
      LOGGER.error("Invalid PEM private key");

      throw new IllegalArgumentException("Invalid PEM private key");
    }

    LOGGER.info("create pipe " + pipeName);

    return pipeName;
  }

  /**
   * create a stage
   *
   * @param stageName stage name
   * @param overwrite if true, execute "create or replace stage" statement;
   *                  otherwise, run "create stage if not exists"
   * @return stage name
   * @throws SQLException
   */
  String createStage(String stageName, boolean overwrite)
    throws SQLException
  {
    checkConnection();

    if (stageName == null || stageName.isEmpty())
    {
      LOGGER.error("stage name is empty");
      throw new IllegalArgumentException("stage name must not be null or " +
        "empty");
    }

    String query;

    if (overwrite)
    {
      query = "create or replace stage identifier(?)";
    }
    else
    {
      query = "create stage if not exists identifier(?)";
    }

    query += queryMark;

    PreparedStatement stmt = conn.prepareStatement(query);

    stmt.setString(1, stageName);

    stmt.execute();

    LOGGER.info("create stage {}", stageName);

    return stageName;

  }

  /**
   * create stage if not exists
   *
   * @param stageName stage name
   * @return stage name
   * @throws SQLException
   */
  String createStage(String stageName) throws SQLException
  {
    return createStage(stageName, false);
  }

  /**
   * check table existence
   *
   * @param tableName table name
   * @return true if table exists, false otherwise
   * @throws SQLException
   */
  boolean tableExist(String tableName) throws SQLException
  {
    checkConnection();

    if (tableName == null || tableName.isEmpty())
    {
      LOGGER.error("table name is empty");
      throw new IllegalArgumentException("table name must not be null or " +
        "empty");
    }

    String query = "desc table identifier(?)";

    query += queryMark;

    PreparedStatement stmt = conn.prepareStatement(query);

    stmt.setString(1, tableName);

    try
    {
      stmt.execute();

      return true;
    } catch (SQLException e)
    {
      LOGGER.debug("table {} doesn't exist");

      return false;
    }
  }

  /**
   * Check the given table has correct schema
   * correct schema: (record_metadata variant, record_content variant)
   *
   * @param tableName table name
   * @return true if schema is correct, false is schema is incorrect or
   * table does not exist
   * @throws SQLException
   */
  boolean tableIsCompatible(String tableName) throws SQLException
  {
    checkConnection();

    if (tableName == null || tableName.isEmpty())
    {
      LOGGER.error("table name is empty");
      throw new IllegalArgumentException("table name must not be null or " +
        "empty");
    }

    String query = "desc table identifier(?)";

    query += queryMark;

    PreparedStatement stmt = conn.prepareStatement(query);

    stmt.setString(1, tableName);

    ResultSet result;

    try
    {
      result = stmt.executeQuery();

      //first column is RECORD_METADATA
      if ((!result.next()) ||
        (!result.getString(1).equals("RECORD_METADATA")) ||
        (!result.getString(2).equals("VARIANT")))
      {
        LOGGER.debug("the first column of table {} is not RECORD_METADATA : " +
            "VARIANT"
          , tableName);

        return false;
      }

      //second column is RECORD_CONTENT
      if ((!result.next()) ||
        (!result.getString(1).equals("RECORD_CONTENT")) ||
        (!result.getString(2).equals("VARIANT")))
      {
        LOGGER.debug("the second column of table {} is not RECORD_CONTENT : " +
            "VARIANT"
          , tableName);

        return false;
      }

      //only two columns
      if (result.next())
      {
        LOGGER.debug("table {} contains more than two columns", tableName);

        return false;
      }


    } catch (SQLException e)
    {
      LOGGER.debug("table {} doesn't exist", tableName);

      return false;
    }

    return true;
  }

  /**
   * check pipe existence
   *
   * @param pipeName pipe name
   * @return true if pipe exists, false otherwise
   * @throws SQLException
   */
  boolean pipeExist(String pipeName) throws SQLException
  {
    checkConnection();

    if (pipeName == null || pipeName.isEmpty())
    {
      LOGGER.error("pipe name is empty");
      throw new IllegalArgumentException("pipeName must not be null or empty");
    }

    String query = "desc pipe identifier(?)";

    query += queryMark;

    PreparedStatement stmt = conn.prepareStatement(query);

    stmt.setString(1, pipeName);

    try
    {
      stmt.execute();

      return true;
    } catch (SQLException e)
    {
      LOGGER.debug("pipe {} doesn't exist", pipeName);

      return false;
    }
  }

  /**
   * check snowpipe definition
   *
   * @param pipeName  pipe name
   * @param tableName table name
   * @param stageName stage name
   * @return true if definition is correct, false if it is incorrect
   * or pipe does not exists
   * @throws SQLException
   */
  boolean pipeIsCompatible(String pipeName, String tableName, String stageName)
    throws SQLException
  {
    checkConnection();

    if (pipeName == null || pipeName.isEmpty())
    {
      LOGGER.error("pipe name is empty");
      throw new IllegalArgumentException("pipeName must not be null or empty");
    }

    if (tableName == null || tableName.isEmpty())
    {
      LOGGER.error("table name is empty");
      throw new IllegalArgumentException("tableName must not be null or empty");
    }

    if (stageName == null || stageName.isEmpty())
    {
      LOGGER.error("stage name is empty");
      throw new IllegalArgumentException("stageName must not be null or empty");
    }

    if (!pipeExist(pipeName))
    {
      return false;
    }

    String query = "desc pipe identifier(?)";

    query += queryMark;

    PreparedStatement stmt = conn.prepareStatement(query);

    stmt.setString(1, pipeName);

    try
    {
      ResultSet result = stmt.executeQuery();

      if (!result.next())
      {
        return false;
      }

      String definition = result.getString("definition");

      LOGGER.debug("pipe {} definition: {}", pipeName, definition);

      return definition.equalsIgnoreCase(pipeDefinition(tableName, stageName));

    } catch (SQLException e)
    {
      LOGGER.debug("pipe {} doesn't exists ", pipeName);

      return false;
    }
  }

  /**
   * check stage existence
   *
   * @param stageName stage name
   * @return true if stage exists, false otherwise
   * @throws SQLException
   */
  boolean stageExist(String stageName) throws SQLException
  {
    checkConnection();

    if (stageName == null || stageName.isEmpty())
    {
      LOGGER.error("stage name is empty");
      throw new IllegalArgumentException("stageName must not be null or empty");
    }

    String query = "desc stage identifier(?)";

    query += queryMark;

    PreparedStatement stmt = conn.prepareStatement(query);

    stmt.setString(1, stageName);

    try
    {
      stmt.execute();

      return true;
    } catch (SQLException e)
    {
      LOGGER.debug("stage {} doesn't exists", stageName);

      return false;
    }
  }

  /**
   * Examine all file names matches our pattern
   *
   * @param stageName stage name
   * @return true is stage is compatible,
   * false if stage does not exist or file name invalid
   * @throws SQLException
   */
  boolean stageIsCompatible(String stageName) throws SQLException
  {
    checkConnection();

    if (stageName == null || stageName.isEmpty())
    {
      LOGGER.error("stage name is empty");
      throw new IllegalArgumentException("stageName must not be null or empty");
    }

    if (!stageExist(stageName))
    {
      LOGGER.debug("stage {} doesn't exists", stageName);

      return false;
    }

    List<String> files = listStage(stageName, "");

    for (String name : files)
    {
      if (!Utils.verifyFileName(name))
      {
        LOGGER.debug("file name {} in stage {} is not valid", name, stageName);

        return false;
      }
    }

    return true;
  }

  /**
   * drop snowpipe
   *
   * @param pipeName pipe name
   * @throws SQLException
   */
  void dropPipe(String pipeName) throws SQLException
  {
    checkConnection();

    if (pipeName == null || pipeName.isEmpty())
    {
      LOGGER.error("pipe name is empty");
      throw new IllegalArgumentException("pipeName must not be null or empty");
    }

    String query = "drop pipe if exists identifier(?)";

    query += queryMark;

    PreparedStatement stmt = conn.prepareStatement(query);

    stmt.setString(1, pipeName);

    stmt.execute();

    //remove from pipe list
    if (pipes.containsKey(pipeName))
    {
      pipes.get(pipeName).close();

      pipes.remove(pipeName);
    }

    LOGGER.info("pipe {} dropped", pipeName);
  }

  /**
   * drop stage if the given stage is empty
   *
   * @param stageName stage name
   * @return true if stage dropped, otherwise false
   */
  boolean dropStageIfEmpty(String stageName) throws SQLException
  {
    checkConnection();

    if (stageName == null || stageName.isEmpty())
    {
      LOGGER.error("stage name is empty");
      throw new IllegalArgumentException("stageName must not be null or empty");
    }

    if (!stageExist(stageName))
    {
      return false;
    }

    String query = "list @" + stageName;

    query += queryMark;

    PreparedStatement stmt = conn.prepareStatement(query);

    ResultSet resultSet = stmt.executeQuery();

    if (Utils.resultSize(resultSet) == 0)
    {
      dropStage(stageName);

      return true;
    }

    LOGGER.info("stage {} can't be dropped because it is not empty", stageName);

    return false;
  }

  /**
   * drop stage
   *
   * @param stageName stage name
   * @throws SQLException
   */
  void dropStage(String stageName) throws SQLException
  {
    checkConnection();

    if (stageName == null || stageName.isEmpty())
    {
      LOGGER.error("stage name is empty");
      throw new IllegalArgumentException("stageName must not be null or empty");
    }

    String query = "drop stage if exists identifier(?)";

    query += queryMark;

    PreparedStatement stmt = conn.prepareStatement(query);

    stmt.setString(1, stageName);

    stmt.execute();

    LOGGER.info("stage {} dropped", stageName);
  }

  /**
   * put file to stage and then ingest it
   *
   * @param pipeName pipe name
   * @param content  file content
   * @param fileName file Name
   * @throws UnexpectedException if the given pipe is not in the pipe list.
   *                             All pipes should be created by this
   *                             JDBCWrapper.
   * @throws Exception           errors in putting file to stage and
   *                             ingesting file
   */
  void ingestFile(String pipeName, String fileName, String content) throws
    Exception
  {
    if (pipeName == null || pipeName.isEmpty())
    {
      LOGGER.error("pipe name is empty");
      throw new IllegalArgumentException("pipeName must not be null or empty");
    }

    if (!pipes.containsKey(pipeName))
    {
      LOGGER.error("pipe {} is not created", pipeName);
      throw new IllegalArgumentException("pipe " + pipeName + " is not " +
        "created");
    }

    //put file to stage
    put(fileName, content, pipes.get(pipeName).getStageName());

    //ingest
    ingestFile(pipeName, fileName);

  }

  /**
   * Ingest given file
   *
   * @param pipeName pipe name
   * @param fileName file name
   * @throws Exception errors in puting file to stage and ingesting file
   */
  void ingestFile(String pipeName, String fileName) throws Exception
  {
    if (pipeName == null || pipeName.isEmpty())
    {
      LOGGER.error("pipe name is empty");
      throw new IllegalArgumentException("pipeName must not be null or empty");
    }

    SnowflakeIngestService ingestService = pipes.get(pipeName);

    ingestService.ingestFile(fileName);

  }

  /**
   * purge files from given stage
   *
   * @param stageName stage name
   * @param files     list of file names
   * @throws SQLException
   */
  void purge(String stageName, List<String> files) throws SQLException
  {
    if (stageName == null || stageName.isEmpty())
    {
      LOGGER.error("stage name is empty");
      throw new IllegalArgumentException("stageName must not be null or empty");
    }

    for (String fileName : files)
    {
      removeFile(stageName, fileName);
    }
  }


  /**
   * check file status from ingest report
   *
   * @param pipeName pipe name
   * @param files    a list of file names
   * @return a map of file status
   * @throws UnexpectedException if the given pipe is not in the pipe list.
   *                             All pipes should be created by this
   *                             JDBCWrapper.
   * @throws Exception           error in checking history
   */
  Map<String, Utils.IngestedFileStatus> verifyFromIngestReport(
    String pipeName, List<String> files) throws Exception
  {
    if (pipeName == null || pipeName.isEmpty())
    {
      LOGGER.error("pipe name is empty");
      throw new IllegalArgumentException("pipeName must not be null or empty");
    }

    if (!pipes.containsKey(pipeName))
    {
      LOGGER.error("pipe {} is not created", pipeName);

      throw new IllegalArgumentException("Pipe " + pipeName + " is not " +
        "created");
    }

    return pipes.get(pipeName).checkIngestReport(files);
  }

  /**
   * check file status from load history
   *
   * @param pipeName pipe name
   * @param files    a list of file names
   * @return a map of file status
   * @throws UnexpectedException if the given pipe is not in the pipe list.
   *                             All pipes should be created by this
   *                             JDBCWrapper.
   * @throws Exception           if can't access ingest service
   */
  Map<String, Utils.IngestedFileStatus> verifyFromLoadHistory(
    String pipeName, List<String> files) throws Exception
  {
    if (pipeName == null || pipeName.isEmpty())
    {
      LOGGER.error("pipe name is empty");
      throw new IllegalArgumentException("pipeName must not be null or empty");
    }

    if (!pipes.containsKey(pipeName))
    {
      LOGGER.error("pipe {} is not created", pipeName);

      throw new IllegalArgumentException("Pipe " + pipeName + " is not " +
        "created");
    }

    return pipes.get(pipeName).checkLoadHistory(files);
  }

  /**
   * move files from stage to table stage
   *
   * @param stageName stage name
   * @param tableName table name
   * @param files     a list of file name
   * @return a list of file, which moved to the table stage. Empty list for
   * no error
   */
  void moveToTableStage(String stageName, String tableName, List<String>
    files) throws Exception
  {

    if (tableName == null || tableName.isEmpty())
    {
      LOGGER.error("table name is empty");
      throw new IllegalArgumentException("tableName must not be null or empty");
    }

    if (stageName == null || stageName.isEmpty())
    {
      LOGGER.error("stage name is empty");
      throw new IllegalArgumentException("stageName must not be null or empty");
    }

    SnowflakeConnectionV1 sfconn = (SnowflakeConnectionV1) conn;

    for (String name : files)
    {
      //get
      InputStream file = sfconn.downloadStream(stageName, name, true);

      //put
      sfconn.compressAndUploadStream("%" + tableName, null, file, name);

      LOGGER.info("moved file: {} from stage: {} to table stage: {}",
        name, stageName, tableName);
    }

  }

  /**
   * recover a pipe
   * // Get the status of all files that were sent to this pipe and are
   * visible in the stage.
   * // ASSUMPTIONS :
   * //      * Files that are visible in load history have the status set to
   * //        {LOAD_IN_PROGRESS, LOADED, PARTIALLY_LOADED, FAILED}
   * //      * Files that are not visible in load history and are too old
   * //        have status set to EXPIRED
   * //      * Files that are not visible in load history and is the latest
   * file is re-ingested
   * //        and status is set to NOT_FOUND
   *
   * @param pipeName     pipe name
   * @param stageName    stage name
   * @param subdirectory subdirectory name (partition number)
   * @return a list of file which in the given subdirectory
   * @throws Exception if failed to recover pipe
   */
  Map<String, Utils.IngestedFileStatus> recoverPipe(String pipeName,
                                                    String stageName,
                                                    String subdirectory)
    throws Exception
  {

    if (pipeName == null || pipeName.isEmpty())
    {
      LOGGER.error("pipe name is empty");
      throw new IllegalArgumentException("pipeName must not be null or empty");
    }

    if (stageName == null || stageName.isEmpty())
    {
      LOGGER.error("stage name is empty");
      throw new IllegalArgumentException("stageName must not be null or empty");
    }

    //create ingest service
    String pipeFullName = properties.get(JDBC_DATABASE) + "." +
      properties.get(JDBC_SCHEMA) + "." + pipeName;

    try
    {
      pipes.put(pipeName, createIngestConnection(pipeFullName, stageName));
    } catch (Exception e)
    {
      LOGGER.error("Invalid PEM private key");

      throw new IllegalArgumentException("Invalid PEM private key");
    }

    List<String> files = listStage(stageName, subdirectory);

    Map<String, Utils.IngestedFileStatus> fileStatus =
      verifyFromLoadHistory(pipeName, files);

    Map<String, Utils.IngestedFileStatus> result = new HashMap<>();

    int loadedFile = 0;

    int failedFile = 0;

    int notFoundFile = 0;

    int expiredFile = 0;

    int loadInProgressFile = 0;

    String lastNotFoundFileName = null;

    for (Map.Entry<String, Utils.IngestedFileStatus> entry : fileStatus
      .entrySet())
    {
      switch (entry.getValue())
      {
        case NOT_FOUND:

          if (Utils.isFileExpired(entry.getKey()))
          {
            result.put(entry.getKey(), Utils.IngestedFileStatus.EXPIRED);

            expiredFile++;
          }
          else
          {
            result.put(entry.getKey(), Utils.IngestedFileStatus.NOT_FOUND);

            if ((lastNotFoundFileName == null) ||
              (Utils.fileNameToTimeIngested(lastNotFoundFileName)
                < Utils.fileNameToTimeIngested(entry.getKey())))
            {
              lastNotFoundFileName = entry.getKey();
            }

            notFoundFile++;
          }

          break;

        case FAILED: //failed
        case PARTIALLY_LOADED: //failed

          result.put(entry.getKey(), entry.getValue());

          failedFile++;

          break;

        case LOAD_IN_PROGRESS:

          result.put(entry.getKey(), entry.getValue());

          loadInProgressFile++;

          break;

        case LOADED:

          result.put(entry.getKey(), entry.getValue());

          loadedFile++;

          break;

        default:

          LOGGER.error("Unexpected status {}", entry.getValue());

          //excepted status
          throw new UnexpectedException("Unexpected status: " + entry
            .getValue());

      }
    }

    if (lastNotFoundFileName != null)
    {
      ingestFile(pipeName, lastNotFoundFileName); // re-ingest last file
    }

    LOGGER.info("Recovered pipe: {}, found {} file(s) on stage {}", pipeName,
      result.size(), stageName);

    LOGGER.debug("{} loaded files, {} failed files, {} expired files, {} not " +
        "found files, {} load in progress files",
      loadedFile, failedFile, expiredFile, notFoundFile, loadInProgressFile);

    return result;
  }

  /**
   * list a table stage and return a list of file names
   *
   * @param tableName table name
   * @return a list of file names
   * @throws SQLException
   */
  List<String> listTableStage(String tableName) throws SQLException
  {
    return listStage(tableName, "", true);
  }


  /**
   * list a stage and return a list of file names contained in given
   * subdirectory
   *
   * @param stageName    stage name
   * @param subdirectory subdirectory name
   * @return a list of file names in given subdirectory, file name =
   * "subdirectory/filename"
   */
  List<String> listStage(String stageName, String subdirectory) throws
    SQLException
  {
    return listStage(stageName, subdirectory, false);
  }

  /**
   * list a stage and return a list of file names contained in given
   * subdirectory
   *
   * @param stageName    stage name
   * @param subdirectory subdirectory name
   * @return a list of file names in given subdirectory, file name =
   * "subdirectory/filename"
   */
  private List<String> listStage(String stageName, String subdirectory,
                                 boolean isTableStage) throws
    SQLException
  {
    if (stageName == null || stageName.isEmpty())
    {
      LOGGER.error("stage name is empty");
      throw new IllegalArgumentException("stageName must not be null or empty");
    }

    String query;

    int stageNameLength;

    if (isTableStage)
    {
      stageNameLength = 0;

      query = "ls @%" + stageName;
    }
    else
    {
      stageNameLength = stageName.length() + 1; //stage name + '/'

      query = "ls @" + stageName + "/" + subdirectory;
    }

    query += queryMark;

    PreparedStatement stmt = conn.prepareStatement(query);

    ResultSet resultSet = stmt.executeQuery();

    List<String> result = new LinkedList<>();


    while (resultSet.next())
    {
      result.add(resultSet.getString("name")
        .substring(stageNameLength));
    }

    LOGGER.info("list stage {} retrieved {} file names", stageName, result
      .size());

    return result;
  }


  /**
   * Close Connection
   *
   * @throws SQLException if meet any exceptions
   */
  void close() throws SQLException
  {
    if (!closed) //only close one time
    {
      try
      {
        conn.close();
        //todo: drop all pipe and stages
      } catch (SQLException e)
      {
        throw new SQLException("Can't close database connection: \n" + e);
      } finally
      {
        closed = true;

        LOGGER.info("jdbc connection closed");
      }
    }
  }

  /**
   * @return true if connection is closed
   */
  boolean isClosed()
  {
    return closed;
  }


  /**
   * put a file to stage
   *
   * @param fileName  file name
   * @param content   file content
   * @param stageName stage name
   */
  void put(String fileName, String content, String stageName)
    throws SQLException
  {
    if (stageName == null || stageName.isEmpty())
    {
      LOGGER.error("stage name is empty");
      throw new IllegalArgumentException("stageName must not be null or empty");
    }

    SnowflakeConnectionV1 sfconn = (SnowflakeConnectionV1) conn;

    InputStream input = new ByteArrayInputStream(content.getBytes());

    sfconn.compressAndUploadStream(stageName, null, input,
      Utils.removeGZFromFileName(fileName));

    LOGGER.debug("put file {} to stage {}", fileName, stageName);

  }

  /**
   * put one file to given table stage. used for bad records only
   *
   * @param fileName  file name
   * @param content   file content
   * @param tableName table name
   */
  void putToTableStage(String fileName, String content, String tableName)
    throws SQLException
  {
    if (tableName == null || tableName.isEmpty())
    {
      LOGGER.error("table name is empty");
      throw new IllegalArgumentException("tableName must not be null or empty");
    }

    put(fileName, content, "%" + tableName);
  }

  /**
   * @return telemetry connector
   */
  SnowflakeTelemetry getTelemetry()
  {
    return this.telemetry;
  }

  /**
   * generate pipe definition
   *
   * @param tableName table name
   * @param stageName stage name
   * @return pipe definition string
   */
  private String pipeDefinition(String tableName, String stageName) throws
    SQLException
  {
    if (tableName == null || tableName.isEmpty())
    {
      LOGGER.error("table name is empty");
      throw new IllegalArgumentException("tableName must not be null or empty");
    }

    if (stageName == null || stageName.isEmpty())
    {
      LOGGER.error("stage name is empty");
      throw new IllegalArgumentException("stageName must not be null or empty");
    }

    return "copy into " + tableName +
      "(RECORD_METADATA, RECORD_CONTENT) from (select $1:meta, " +
      "$1:content from"
      + " @" + stageName + " t) file_format = (type = 'json')";

  }

  /**
   * Remove one file from given stage
   *
   * @param stageName stage name
   * @param fileName  file name
   * @throws SQLException
   */
  private void removeFile(String stageName, String fileName) throws
    SQLException
  {
    if (stageName == null || stageName.isEmpty())
    {
      LOGGER.error("stage name is empty");
      throw new IllegalArgumentException("stageName must not be null or empty");
    }

    String query = "rm @" + stageName + "/" + fileName;

    query += queryMark;

    PreparedStatement stmt = conn.prepareStatement(query);

    stmt.execute();

    LOGGER.debug("deleted {} from stage {}", fileName, stageName);
  }


  /**
   * make sure connection is not closed
   */
  private void checkConnection()
  {
    if (closed)
    {
      throw new IllegalStateException(
        "snowflake connection is closed"
      );
    }
  }

  /**
   * create an ingest sdk instance of given pipe
   *
   * @param pipeName  pipename format: database.schema.pipename
   * @param stageName stage name
   * @return an instance of ingest sdk
   * @throws InvalidKeySpecException
   * @throws NoSuchAlgorithmException
   */
  private SnowflakeIngestService createIngestConnection(String pipeName,
                                                        String stageName)
    throws InvalidKeySpecException, NoSuchAlgorithmException
  {
    if (pipeName == null || pipeName.isEmpty())
    {
      LOGGER.error("pipe name is empty");
      throw new IllegalArgumentException("pipeName must not be null or empty");
    }

    if (stageName == null || stageName.isEmpty())
    {
      LOGGER.error("stage name is empty");
      throw new IllegalArgumentException("stageName must not be null or empty");
    }

    String account = properties.get(JDBC_ACCOUNT).toString();

    String user = properties.get(JDBC_USER).toString();

    String ingestUrl = url.getFullUrl();

    PrivateKey privateKey = (PrivateKey) properties.get(JDBC_PRIVATE_KEY);

    return new SnowflakeIngestService(account, user, pipeName, ingestUrl,
      privateKey, stageName);
  }

}


