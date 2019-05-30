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
package com.snowflake.kafka.connector.internal;

import com.snowflake.client.jdbc.SnowflakeDriver;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.security.KeyFactory;
import java.security.PrivateKey;
import java.security.spec.PKCS8EncodedKeySpec;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.snowflake.kafka.connector.Utils;
import net.snowflake.client.jdbc.SnowflakeConnectionV1;
import net.snowflake.client.jdbc.internal.apache.commons.codec.binary.Base64;

/**
 * Snowflake JDBC helper functions
 */
public class SnowflakeJDBCWrapper extends Logging
{

  private Connection conn;
  private Properties properties;
  private boolean closed;
  private final SnowflakeURL url;

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
  private static final String JDBC_SESSION_KEEP_ALIVE =
    "client_session_keep_alive";
  private static final String JDBC_WAREHOUSE = "warehouse"; //for test only
  private static final String URL = "url";

  /**
   * Create a Snowflake connection
   *
   * @param conf a map contains all configurations
   */
  public SnowflakeJDBCWrapper(Map<String, String> conf)
  {
    if (conf == null)
    {
      throw SnowflakeErrors.ERROR_0001.getException("input conf is null");
    }

    properties = createProperties(conf);

    url = (SnowflakeURL) properties.get(URL);

    properties.remove(URL);

    try
    {
      this.conn = new SnowflakeDriver().connect(url.getJdbcUrl(), properties);
    } catch (SQLException e)
    {
      throw SnowflakeErrors.ERROR_1001.getException(e);
    }

    this.closed = false;

    pipes = new HashMap<>();

    telemetry = new SnowflakeTelemetry((SnowflakeConnectionV1) this.conn);

    logInfo("initialized the snowflake JDBC");
  }

  /**
   * create a jdbc properties for LOGGER in
   *
   * @param conf a map contains all parameters
   * @return a Properties instance
   */
  public static Properties createProperties(Map<String, String> conf)
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
            throw SnowflakeErrors.ERROR_0002.getException(e);
          }
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
      throw SnowflakeErrors.ERROR_0003.getException(
        "private key must be provided with " + Utils.SF_PRIVATE_KEY +
          " parameter");
    }

    if (!properties.containsKey(JDBC_SCHEMA))
    {
      throw SnowflakeErrors.ERROR_0003.getException(
        "snowflake schema name must be provided with " + Utils.SF_SCHEMA +
          " parameter"
      );
    }

    if (!properties.containsKey(JDBC_DATABASE))
    {
      throw SnowflakeErrors.ERROR_0003.getException(
        "snowflake database name must be provided with " + Utils.SF_DATABASE
          + " parameter"
      );
    }

    if (!properties.containsKey(JDBC_USER))
    {
      throw SnowflakeErrors.ERROR_0003.getException(
        "snowflake user name must be provided with " + Utils.SF_USER +
          " parameter"
      );
    }

    if (!properties.containsKey(URL))
    {
      throw SnowflakeErrors.ERROR_0003.getException(
        "snowflake URL must be provided with " + Utils.SF_URL +
          " parameter, e.g. 'accountname.snoflakecomputing.com'"
      );
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
  public String createTable(String tableName, boolean overwrite)
  {
    checkConnection();

    if (tableName == null || tableName.isEmpty())
    {
      throw SnowflakeErrors.ERROR_0005.getException();
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

    try
    {
      PreparedStatement stmt = conn.prepareStatement(query);
      stmt.setString(1, tableName);
      stmt.execute();
    } catch (SQLException e)
    {
      throw SnowflakeErrors.ERROR_2001.getException(e);
    }

    logInfo("create table {}", tableName);

    return tableName;
  }

  /**
   * create table is not exists
   *
   * @param tableName table name
   * @return table name
   */
  public String createTable(String tableName)
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
   */
  public String createPipe(String pipeName, String tableName, String stageName,
                           boolean overwrite)
  {
    checkConnection();

    if (pipeName == null || pipeName.isEmpty())
    {
      throw SnowflakeErrors.ERROR_0006.getException();
    }

    if (tableName == null || tableName.isEmpty())
    {
      throw SnowflakeErrors.ERROR_0005.getException();
    }

    if (stageName == null || stageName.isEmpty())
    {
      throw SnowflakeErrors.ERROR_0004.getException();
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

    try
    {
      query += "as " + pipeDefinition(tableName, stageName);

      PreparedStatement stmt = conn.prepareStatement(query);

      stmt.setString(1, pipeName);

      stmt.execute();
    } catch (SQLException e)
    {
      throw SnowflakeErrors.ERROR_2001.getException(e);
    }

    //create ingest service
    String pipeFullName = properties.get(JDBC_DATABASE) + "." + properties
      .get(JDBC_SCHEMA) + "." + pipeName;

    pipes.put(pipeName, createIngestConnection(pipeFullName, stageName));

    logInfo("create pipe: {}", pipeName);

    return pipeName;
  }

  /**
   * create a stage
   *
   * @param stageName stage name
   * @param overwrite if true, execute "create or replace stage" statement;
   *                  otherwise, run "create stage if not exists"
   * @return stage name
   */
  public String createStage(String stageName, boolean overwrite)
  {
    checkConnection();

    if (stageName == null || stageName.isEmpty())
    {
      SnowflakeErrors.ERROR_0004.getException();
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

    try
    {
      PreparedStatement stmt = conn.prepareStatement(query);
      stmt.setString(1, stageName);
      stmt.execute();
    } catch (SQLException e)
    {
      throw SnowflakeErrors.ERROR_2001.getException(e);
    }

    logInfo("create stage {}", stageName);

    return stageName;

  }

  /**
   * create stage if not exists
   *
   * @param stageName stage name
   * @return stage name
   */
  public String createStage(String stageName)
  {
    return createStage(stageName, false);
  }

  /**
   * check table existence
   *
   * @param tableName table name
   * @return true if table exists, false otherwise
   */
  public boolean tableExist(String tableName)
  {
    checkConnection();

    if (tableName == null || tableName.isEmpty())
    {
      throw SnowflakeErrors.ERROR_0005.getException();
    }

    String query = "desc table identifier(?)";

    PreparedStatement stmt;
    try
    {
      stmt = conn.prepareStatement(query);
      stmt.setString(1, tableName);
    } catch (SQLException e)
    {
      throw SnowflakeErrors.ERROR_2001.getException(e);
    }

    try
    {
      stmt.execute();

      return true;
    } catch (SQLException e)
    {
      logDebug("table {} doesn't exist", tableName);

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
   */
  public boolean tableIsCompatible(String tableName)
  {
    checkConnection();

    if (tableName == null || tableName.isEmpty())
    {
      throw SnowflakeErrors.ERROR_0005.getException();
    }

    String query = "desc table identifier(?)";

    PreparedStatement stmt;
    try
    {
      stmt = conn.prepareStatement(query);
      stmt.setString(1, tableName);
    } catch (SQLException e)
    {
      throw SnowflakeErrors.ERROR_2001.getException(e);
    }

    ResultSet result;

    try
    {
      result = stmt.executeQuery();

      //first column is RECORD_METADATA
      if ((!result.next()) ||
        (!result.getString(1).equals("RECORD_METADATA")) ||
        (!result.getString(2).equals("VARIANT")))
      {
        logDebug("the first column of table {} is not RECORD_METADATA : " +
          "VARIANT", tableName);
        ;

        return false;
      }

      //second column is RECORD_CONTENT
      if ((!result.next()) ||
        (!result.getString(1).equals("RECORD_CONTENT")) ||
        (!result.getString(2).equals("VARIANT")))
      {
        logDebug("the second column of table {} is not RECORD_CONTENT : " +
          "VARIANT", tableName);

        return false;
      }

      //only two columns
      if (result.next())
      {
        logDebug("table {} contains more than two columns", tableName);

        return false;
      }


    } catch (SQLException e)
    {
      logDebug("table {} doesn't exist", tableName);

      return false;
    }

    return true;
  }

  /**
   * check pipe existence
   *
   * @param pipeName pipe name
   * @return true if pipe exists, false otherwise
   */
  public boolean pipeExist(String pipeName)
  {
    checkConnection();

    if (pipeName == null || pipeName.isEmpty())
    {
      throw SnowflakeErrors.ERROR_0006.getException();
    }

    String query = "desc pipe identifier(?)";

    PreparedStatement stmt;
    try
    {
      stmt = conn.prepareStatement(query);
      stmt.setString(1, pipeName);
    } catch (SQLException e)
    {
      throw SnowflakeErrors.ERROR_2001.getException(e);
    }

    try
    {
      stmt.execute();

      return true;
    } catch (SQLException e)
    {
      logDebug("pipe {} doesn't exist", pipeName);

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
   */
  public boolean pipeIsCompatible(String pipeName, String tableName, String
    stageName)
  {
    checkConnection();

    if (pipeName == null || pipeName.isEmpty())
    {
      throw SnowflakeErrors.ERROR_0006.getException();
    }

    if (tableName == null || tableName.isEmpty())
    {
      throw SnowflakeErrors.ERROR_0005.getException();
    }

    if (stageName == null || stageName.isEmpty())
    {
      throw SnowflakeErrors.ERROR_0004.getException();
    }

    if (!pipeExist(pipeName))
    {
      return false;
    }

    String query = "desc pipe identifier(?)";

    PreparedStatement stmt;
    try
    {
      stmt = conn.prepareStatement(query);
      stmt.setString(1, pipeName);
    } catch (SQLException e)
    {
      throw SnowflakeErrors.ERROR_2001.getException(e);
    }

    try
    {
      ResultSet result = stmt.executeQuery();

      if (!result.next())
      {
        return false;
      }

      String definition = result.getString("definition");

      logDebug("pipe {} definition: {}", pipeName, definition);

      return definition.equalsIgnoreCase(pipeDefinition(tableName, stageName));

    } catch (SQLException e)
    {
      logDebug("pipe {} doesn't exists ", pipeName);

      return false;
    }
  }

  /**
   * check stage existence
   *
   * @param stageName stage name
   * @return true if stage exists, false otherwise
   */
  public boolean stageExist(String stageName)
  {
    checkConnection();

    if (stageName == null || stageName.isEmpty())
    {
      throw SnowflakeErrors.ERROR_0004.getException();
    }

    String query = "desc stage identifier(?)";

    PreparedStatement stmt;
    try
    {
      stmt = conn.prepareStatement(query);
      stmt.setString(1, stageName);
    } catch (SQLException e)
    {
      throw SnowflakeErrors.ERROR_2001.getException(e);
    }

    try
    {
      stmt.execute();

      return true;
    } catch (SQLException e)
    {
      logDebug("stage {} doesn't exists", stageName);

      return false;
    }
  }

  /**
   * Examine all file names matches our pattern
   *
   * @param stageName stage name
   * @return true is stage is compatible,
   * false if stage does not exist or file name invalid
   */
  public boolean stageIsCompatible(String stageName)
  {
    checkConnection();

    if (stageName == null || stageName.isEmpty())
    {
      throw SnowflakeErrors.ERROR_0004.getException();
    }

    if (!stageExist(stageName))
    {
      logDebug("stage {} doesn't exists", stageName);

      return false;
    }

    List<String> files = listStage(stageName, "");

    for (String name : files)
    {
      if (!Utils.verifyFileName(name))
      {
        logDebug("file name {} in stage {} is not valid", name, stageName);

        return false;
      }
    }

    return true;
  }

  /**
   * drop snowpipe
   *
   * @param pipeName pipe name
   */
  public void dropPipe(String pipeName)
  {
    checkConnection();

    if (pipeName == null || pipeName.isEmpty())
    {
      throw SnowflakeErrors.ERROR_0006.getException();
    }

    String query = "drop pipe if exists identifier(?)";

    try
    {
      PreparedStatement stmt = conn.prepareStatement(query);
      stmt.setString(1, pipeName);
      stmt.execute();
    } catch (SQLException e)
    {
      throw SnowflakeErrors.ERROR_2001.getException(e);
    }

    //remove from pipe list
    if (pipes.containsKey(pipeName))
    {
      pipes.get(pipeName).close();

      pipes.remove(pipeName);
    }

    logInfo("pipe {} dropped", pipeName);
  }

  /**
   * drop stage if the given stage is empty
   *
   * @param stageName stage name
   * @return true if stage dropped, otherwise false
   */
  public boolean dropStageIfEmpty(String stageName)
  {
    checkConnection();

    if (stageName == null || stageName.isEmpty())
    {
      throw SnowflakeErrors.ERROR_0004.getException();
    }

    if (!stageExist(stageName))
    {
      return false;
    }

    String query = "list @" + stageName;

    try
    {
      PreparedStatement stmt = conn.prepareStatement(query);
      ResultSet resultSet = stmt.executeQuery();
      if (Utils.resultSize(resultSet) == 0)
      {
        dropStage(stageName);

        return true;
      }
    } catch (SQLException e)
    {
      throw SnowflakeErrors.ERROR_2001.getException(e);
    }


    logInfo("stage {} can't be dropped because it is not empty", stageName);

    return false;
  }

  /**
   * drop stage
   *
   * @param stageName stage name
   */
  public void dropStage(String stageName)
  {
    checkConnection();

    if (stageName == null || stageName.isEmpty())
    {
      throw SnowflakeErrors.ERROR_0004.getException();
    }

    String query = "drop stage if exists identifier(?)";

    try
    {
      PreparedStatement stmt = conn.prepareStatement(query);

      stmt.setString(1, stageName);

      stmt.execute();
    } catch (SQLException e)
    {
      throw SnowflakeErrors.ERROR_2001.getException(e);
    }

    logInfo("stage {} dropped", stageName);
  }

  /**
   * put file to stage and then ingest it
   *
   * @param pipeName pipe name
   * @param content  file content
   * @param fileName file Name
   */
  public void ingestFile(String pipeName, String fileName, String content)
  {
    if (pipeName == null || pipeName.isEmpty())
    {
      throw SnowflakeErrors.ERROR_0006.getException();
    }

    if (!pipes.containsKey(pipeName))
    {
      throw SnowflakeErrors.ERROR_3004.getException();
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
   */
  public void ingestFile(String pipeName, String fileName)
  {
    if (pipeName == null || pipeName.isEmpty())
    {
      throw SnowflakeErrors.ERROR_0006.getException();
    }

    SnowflakeIngestService ingestService = pipes.get(pipeName);

    ingestService.ingestFile(fileName);

  }

  /**
   * purge files from given stage
   *
   * @param stageName stage name
   * @param files     list of file names
   */
  public void purge(String stageName, List<String> files)
  {
    if (stageName == null || stageName.isEmpty())
    {
      throw SnowflakeErrors.ERROR_0004.getException();
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
   */
  public Map<String, Utils.IngestedFileStatus> verifyFromIngestReport(
    String pipeName, List<String> files)
  {
    if (pipeName == null || pipeName.isEmpty())
    {
      throw SnowflakeErrors.ERROR_0006.getException();
    }

    if (!pipes.containsKey(pipeName))
    {
      throw SnowflakeErrors.ERROR_3004.getException();
    }

    return pipes.get(pipeName).checkIngestReport(files);
  }

  /**
   * check file status from one hour load history
   *
   * @param pipeName  pipe name
   * @param files     a list of file names
   * @param startTime start timestamp
   * @return a map of file status
   */
  public Map<String, Utils.IngestedFileStatus> verifyFromOneHourLoadHistory(
    String pipeName, List<String> files, long startTime)
  {

    if (pipeName == null || pipeName.isEmpty())
    {
      throw SnowflakeErrors.ERROR_0006.getException();
    }

    if (!pipes.containsKey(pipeName))
    {
      throw SnowflakeErrors.ERROR_3004.getException();
    }

    return pipes.get(pipeName).checkOneHourHistory(files, startTime);
  }

  /**
   * check file status from last one hour load history
   *
   * @param pipeName pipe name
   * @param files    a list of file names
   * @return a map of file status
   */
  public Map<String, Utils.IngestedFileStatus> verifyFromLastOnehourLoasHistory(
    String pipeName, List<String> files)
  {

    return verifyFromOneHourLoadHistory(pipeName, files, System
      .currentTimeMillis() - SnowflakeIngestService.ONE_HOUR);
  }


  /**
   * move files from stage to table stage
   *
   * @param stageName stage name
   * @param tableName table name
   * @param files     a list of file name
   */
  public void moveToTableStage(String stageName, String tableName,
                               List<String> files)
  {
    if (tableName == null || tableName.isEmpty())
    {
      throw SnowflakeErrors.ERROR_0005.getException();
    }

    if (stageName == null || stageName.isEmpty())
    {
      throw SnowflakeErrors.ERROR_0004.getException();
    }

    SnowflakeConnectionV1 sfconn = (SnowflakeConnectionV1) conn;

    for (String name : files)
    {
      //get
      InputStream file;
      try
      {
        file = sfconn.downloadStream(stageName, name, true);
      } catch (Exception e)
      {
        throw SnowflakeErrors.ERROR_2002.getException(e);
      }

      //put
      try
      {
        sfconn.compressAndUploadStream("%" + tableName, null, file, name);
      } catch (SQLException e)
      {
        throw SnowflakeErrors.ERROR_2003.getException(e);
      }

      //remove
      removeFile(stageName, name);

      logInfo("moved file: {} from stage: {} to table stage: {}", name,
        stageName, tableName);
    }

  }

  /**
   * move all files on stage related to given pipe to table stage
   *
   * @param stageName    stage name
   * @param tableName    table name
   * @param subdirectory subdirectory name
   */
  public void moveToTableStage(String stageName, String tableName, String
    subdirectory)
  {
    if (tableName == null || tableName.isEmpty())
    {
      throw SnowflakeErrors.ERROR_0005.getException();
    }

    if (stageName == null || stageName.isEmpty())
    {
      throw SnowflakeErrors.ERROR_0004.getException();
    }

    List<String> files = listStage(stageName, subdirectory);

    moveToTableStage(stageName, tableName, files);

  }


  /**
   * recover a pipe, move all failed file to table stage, purge all loaded files
   * and return a list of not processed files
   *
   * @param pipeName     pipe name
   * @param stageName    stage name
   * @param subdirectory subdirectory name
   * @return a map of file status
   */
  public Map<String, Utils.IngestedFileStatus> recoverPipe(String pipeName,
                                                           String stageName,
                                                           String subdirectory)
  {


    if (pipeName == null || pipeName.isEmpty())
    {
      throw SnowflakeErrors.ERROR_0006.getException();
    }

    if (stageName == null || stageName.isEmpty())
    {
      throw SnowflakeErrors.ERROR_0004.getException();
    }

    //create ingest service
    String pipeFullName = properties.get(JDBC_DATABASE) + "." +
      properties.get(JDBC_SCHEMA) + "." + pipeName;

    pipes.put(pipeName, createIngestConnection(pipeFullName, stageName));

    List<String> files = listStage(stageName, subdirectory);

    Map<String, Utils.IngestedFileStatus> result = new HashMap<>();

    //sort by time
    //may be an issue when continuously recovering
    // because this time is time when file uploaded.
    // if files ingested again, this time will not be
    // updated. So the real ingestion time maybe different
    // in the second time recovery.
    files.sort(Comparator.comparingLong(Utils::fileNameToTimeIngested));

    while (!files.isEmpty())
    {
      long startTime = Utils.fileNameToTimeIngested(files.get(0));
      long endTime = startTime + SnowflakeIngestService.TEN_MINUTES;

      verifyFromOneHourLoadHistory(pipeName, files, startTime).forEach(
        (name, status) ->
        {
          switch (status)
          {
            case NOT_FOUND:
              //if ingested time later than start time + 10 min,
              //then scan again, otherwise treat as not processed file
              if (Utils.fileNameToTimeIngested(name) > endTime)
              {
                break;
              }
              ingestFile(pipeName, name);

            case LOADED:
            case LOAD_IN_PROGRESS:
            case FAILED:
            default:
              files.remove(name);
              result.put(name, status);
          }
        }
      );
    }

    logInfo("Recovered {} files", result.size());
    return result;
  }

  /**
   * list a table stage and return a list of file names
   *
   * @param tableName table name
   * @return a list of file names
   */
  public List<String> listTableStage(String tableName)
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
  public List<String> listStage(String stageName, String subdirectory)
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
                                 boolean isTableStage)
  {
    if (stageName == null || stageName.isEmpty())
    {
      throw SnowflakeErrors.ERROR_0004.getException();
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

    List<String> result;
    try
    {
      PreparedStatement stmt = conn.prepareStatement(query);
      ResultSet resultSet = stmt.executeQuery();

      result = new LinkedList<>();
      while (resultSet.next())
      {
        result.add(resultSet.getString("name")
          .substring(stageNameLength));
      }
    } catch (SQLException e)
    {
      throw SnowflakeErrors.ERROR_2001.getException(e);
    }


    logInfo("list stage {} retrieved {} file names",
      stageName, result.size());

    return result;
  }


  /**
   * Close Connection
   */
  public void close()
  {
    if (!closed) //only close one time
    {
      try
      {
        conn.close();
      } catch (SQLException e)
      {
        throw SnowflakeErrors.ERROR_2001.getException(e);
      } finally
      {
        closed = true;

        logInfo("jdbc connection closed");
      }
    }
  }

  /**
   * @return true if connection is closed
   */
  public boolean isClosed()
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
  public void put(String fileName, String content, String stageName)
  {
    if (stageName == null || stageName.isEmpty())
    {
      throw SnowflakeErrors.ERROR_0004.getException();
    }

    SnowflakeConnectionV1 sfconn = (SnowflakeConnectionV1) conn;

    InputStream input = new ByteArrayInputStream(content.getBytes());

    try
    {
      sfconn.compressAndUploadStream(stageName, null, input,
        Utils.removeGZFromFileName(fileName));
    } catch (SQLException e)
    {
      throw SnowflakeErrors.ERROR_2003.getException(e);
    }

    logDebug("put file {} to stage {}", fileName, stageName);

  }

  /**
   * put one file to given table stage. used for bad records only
   *
   * @param fileName  file name
   * @param content   file content
   * @param tableName table name
   */
  public void putToTableStage(String fileName, String content, String tableName)
  {
    if (tableName == null || tableName.isEmpty())
    {
      throw SnowflakeErrors.ERROR_0005.getException();
    }

    put(fileName, content, "%" + tableName);
  }

  /**
   * @return telemetry connector
   */
  public SnowflakeTelemetry getTelemetry()
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
  private String pipeDefinition(String tableName, String stageName)
  {
    if (tableName == null || tableName.isEmpty())
    {
      throw SnowflakeErrors.ERROR_0005.getException();
    }

    if (stageName == null || stageName.isEmpty())
    {
      throw SnowflakeErrors.ERROR_0004.getException();
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
   */
  private void removeFile(String stageName, String fileName)
  {
    if (stageName == null || stageName.isEmpty())
    {
      throw SnowflakeErrors.ERROR_0004.getException();
    }

    String query = "rm @" + stageName + "/" + fileName;

    try
    {
      PreparedStatement stmt = conn.prepareStatement(query);
      stmt.execute();
    } catch (SQLException e)
    {
      throw SnowflakeErrors.ERROR_2001.getException(e);
    }

    logDebug("deleted {} from stage {}", fileName, stageName);
  }


  /**
   * make sure connection is not closed
   */
  private void checkConnection()
  {
    if (closed)
    {
      throw SnowflakeErrors.ERROR_1003.getException();
    }
  }

  /**
   * create an ingest sdk instance of given pipe
   *
   * @param pipeName  pipename format: database.schema.pipename
   * @param stageName stage name
   * @return an instance of ingest sdk
   */
  private SnowflakeIngestService createIngestConnection(String pipeName,
                                                        String stageName)
  {
    if (pipeName == null || pipeName.isEmpty())
    {
      throw SnowflakeErrors.ERROR_0006.getException();
    }

    if (stageName == null || stageName.isEmpty())
    {
      throw SnowflakeErrors.ERROR_0004.getException();
    }

    String account = properties.get(JDBC_ACCOUNT).toString();

    String user = properties.get(JDBC_USER).toString();

    String ingestUrl = url.getUrlWithoutPort();

    PrivateKey privateKey = (PrivateKey) properties.get(JDBC_PRIVATE_KEY);

    return new SnowflakeIngestService(account, user, pipeName, ingestUrl,
      privateKey, stageName);
  }

}


