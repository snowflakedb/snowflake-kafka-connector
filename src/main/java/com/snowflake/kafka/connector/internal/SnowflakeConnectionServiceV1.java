package com.snowflake.kafka.connector.internal;

import net.snowflake.client.jdbc.SnowflakeConnectionV1;
import net.snowflake.client.jdbc.SnowflakeDriver;
import net.snowflake.client.jdbc.SnowflakeStatement;
import net.snowflake.client.jdbc.cloud.storage.StageInfo;
import net.snowflake.ingest.connection.HistoryResponse;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.security.PrivateKey;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

public class SnowflakeConnectionServiceV1 extends Logging
  implements SnowflakeConnectionService
{
  private final Connection conn;
  private final SnowflakeTelemetryService telemetry;
  private final String connectorName;
  private final String taskID;
  private final Properties prop;

  // Placeholder for all proxy related properties set in the connector configuration
  private final Properties proxyProperties;
  private final SnowflakeURL url;
  private final SnowflakeInternalStage internalStage;
  private StageInfo.StageType stageType;

  SnowflakeConnectionServiceV1(Properties prop, SnowflakeURL url,
                               String connectorName, String taskID, Properties proxyProperties)
  {
    this.connectorName = connectorName;
    this.taskID = taskID;
    this.url = url;
    this.prop = prop;
    this.stageType = null;
    this.proxyProperties = proxyProperties;
    try
    {
      this.conn = new SnowflakeDriver().connect(url.getJdbcUrl(), prop);
    } catch (SQLException e)
    {
      throw SnowflakeErrors.ERROR_1001.getException(e);
    }
    long credentialExpireTime = 30 * 60 * 1000L;
    this.internalStage = new SnowflakeInternalStage((SnowflakeConnectionV1) this.conn, credentialExpireTime, proxyProperties);
    this.telemetry =
      SnowflakeTelemetryServiceFactory.builder(conn)
        .setAppName(this.connectorName)
        .setTaskID(this.taskID)
        .build();
    logInfo("initialized the snowflake connection");
  }

  @Override
  public void createTable(final String tableName, final boolean overwrite)
  {
    checkConnection();
    InternalUtils.assertNotEmpty("tableName", tableName);
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
      stmt.close();
    } catch (SQLException e)
    {
      throw SnowflakeErrors.ERROR_2007.getException(e);
    }

    logInfo("create table {}", tableName);
  }

  @Override
  public void createTable(final String tableName)
  {
    createTable(tableName, false);
  }

  @Override
  public void createPipe(final String tableName, final String stageName,
                         final String pipeName, final boolean overwrite)
  {
    checkConnection();
    InternalUtils.assertNotEmpty("tableName", tableName);
    InternalUtils.assertNotEmpty("stageName", stageName);
    InternalUtils.assertNotEmpty("pipeName", pipeName);

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
      stmt.close();
    } catch (SQLException e)
    {
      throw SnowflakeErrors.ERROR_2009.getException(e);
    }
    logInfo("create pipe: {}", pipeName);
  }

  @Override
  public void createPipe(final String tableName, final String stageName,
                         final String pipeName)
  {
    createPipe(tableName, stageName, pipeName, false);
  }

  @Override
  public void createStage(final String stageName, final boolean overwrite)
  {
    checkConnection();
    InternalUtils.assertNotEmpty("stageName", stageName);

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
      stmt.close();
    } catch (SQLException e)
    {
      throw SnowflakeErrors.ERROR_2008.getException(e);
    }
    logInfo("create stage {}", stageName);
  }

  @Override
  public void createStage(final String stageName)
  {
    createStage(stageName, false);
  }

  @Override
  public boolean tableExist(final String tableName)
  {
    checkConnection();
    InternalUtils.assertNotEmpty("tableName", tableName);
    String query = "desc table identifier(?)";
    PreparedStatement stmt = null;
    boolean exist;
    try
    {
      stmt = conn.prepareStatement(query);
      stmt.setString(1, tableName);
      stmt.execute();
      exist = true;
    }
    catch (Exception e)
    {
      logDebug("table {} doesn't exist", tableName);
      exist = false;
    }
    finally
    {
      if(stmt != null)
      {
        try
        {
          stmt.close();
        } catch (SQLException e)
        {
          e.printStackTrace();
        }
      }
    }
    return exist;
  }

  @Override
  public boolean stageExist(final String stageName)
  {
    checkConnection();
    InternalUtils.assertNotEmpty("stageName", stageName);
    String query = "desc stage identifier(?)";
    PreparedStatement stmt = null;
    boolean exist;
    try
    {
      stmt = conn.prepareStatement(query);
      stmt.setString(1, stageName);
      stmt.execute();
      exist = true;
    } catch (SQLException e)
    {
      logDebug("stage {} doesn't exists", stageName);
      exist = false;
    }
    finally
    {
      if(stmt != null)
      {
        try
        {
          stmt.close();
        } catch (SQLException e)
        {
          e.printStackTrace();
        }
      }
    }
    return exist;
  }

  @Override
  public boolean pipeExist(final String pipeName)
  {
    checkConnection();
    InternalUtils.assertNotEmpty("pipeName", pipeName);
    String query = "desc pipe identifier(?)";
    PreparedStatement stmt = null;
    boolean exist;
    try
    {
      stmt = conn.prepareStatement(query);
      stmt.setString(1, pipeName);
      stmt.execute();
      exist = true;
    } catch (SQLException e)
    {
      logDebug("pipe {} doesn't exist", pipeName);
      exist = false;
    }
    finally
    {
      if(stmt != null)
      {
        try
        {
          stmt.close();
        } catch (SQLException e)
        {
          e.printStackTrace();
        }
      }
    }
    return exist;
  }

  @Override
  public boolean isTableCompatible(final String tableName)
  {
    checkConnection();
    InternalUtils.assertNotEmpty("tableName", tableName);
    String query = "desc table identifier(?)";
    PreparedStatement stmt = null;
    ResultSet result = null;
    boolean compatible;
    try
    {
      stmt = conn.prepareStatement(query);
      stmt.setString(1, tableName);
      result = stmt.executeQuery();
      boolean hasMeta = false;
      boolean hasContent = false;
      boolean allNullable = true;
      while (result.next())
      {
        switch (result.getString(1))
        {
          case "RECORD_METADATA":
            if(result.getString(2).equals("VARIANT"))
            {
              hasMeta = true;
            }
            break;
          case "RECORD_CONTENT":
            if(result.getString(2).equals("VARIANT"))
            {
              hasContent = true;
            }
            break;
          default:
            if(result.getString(4).equals("N"))
            {
              allNullable = false;
            }

        }
      }
      compatible = hasMeta && hasContent && allNullable;
    } catch (SQLException e)
    {
      logDebug("table {} doesn't exist", tableName);
      compatible = false;
    }
    finally
    {
      try
      {
        if(result != null)
        {
          result.close();
        }
      }
      catch (Exception e)
      {
        e.printStackTrace();
      }

      try
      {
        if(stmt != null)
        {
          stmt.close();
        }
      }
      catch (Exception e)
      {
        e.printStackTrace();
      }
    }
    return compatible;
  }

  @Override
  public boolean isStageCompatible(final String stageName)
  {
    checkConnection();
    InternalUtils.assertNotEmpty("stageName", stageName);
    if (!stageExist(stageName))
    {
      logDebug("stage {} doesn't exists", stageName);
      return false;
    }
    List<String> files = listStage(stageName, "");
    for (String name : files)
    {
      if (!FileNameUtils.verifyFileName(name))
      {
        logDebug("file name {} in stage {} is not valid", name, stageName);
        return false;
      }
    }
    return true;
  }

  @Override
  public boolean isPipeCompatible(final String tableName,
                                  final String stageName, final String pipeName)
  {
    checkConnection();
    InternalUtils.assertNotEmpty("tableName", tableName);
    InternalUtils.assertNotEmpty("stageName", stageName);
    InternalUtils.assertNotEmpty("pipeName", pipeName);
    if (!pipeExist(pipeName))
    {
      return false;
    }

    String query = "desc pipe identifier(?)";
    PreparedStatement stmt = null;
    ResultSet result = null;
    boolean compatible;
    try
    {
      stmt = conn.prepareStatement(query);
      stmt.setString(1, pipeName);
      result = stmt.executeQuery();
      if (!result.next())
      {
        compatible = false;
      }
      else
      {
        String definition = result.getString("definition");
        logDebug("pipe {} definition: {}", pipeName, definition);
        compatible =  definition.equalsIgnoreCase(pipeDefinition(tableName, stageName));
      }

    } catch (SQLException e)
    {
      logDebug("pipe {} doesn't exists ", pipeName);
      compatible = false;
    }
    finally
    {
      try
      {
        if(stmt != null)
        {
          stmt.close();
        }
        if(result != null)
        {
          result.close();
        }
      }
      catch (Exception e)
      {
        e.printStackTrace();
      }
    }

    return compatible;
  }

  @Override
  public void databaseExists(String databaseName)
  {
    checkConnection();
    String query = "use database identifier(?)";
    try
    {
      PreparedStatement stmt = conn.prepareStatement(query);
      stmt.setString(1, databaseName);
      stmt.execute();
      stmt.close();
    } catch (SQLException e)
    {
      throw SnowflakeErrors.ERROR_2001.getException(e);
    }

    logInfo("database {} exists", databaseName);
  }

  @Override
  public void schemaExists(String schemaName)
  {
    checkConnection();
    String query = "use schema identifier(?)";
    boolean foundSchema = false;
    try
    {
      PreparedStatement stmt = conn.prepareStatement(query);
      stmt.setString(1, schemaName);
      stmt.execute();
      stmt.close();
    } catch (SQLException e)
    {
      throw SnowflakeErrors.ERROR_2001.getException(e);
    }

    logInfo("schema {} exists", schemaName);
  }

  @Override
  public void dropPipe(final String pipeName)
  {
    checkConnection();
    InternalUtils.assertNotEmpty("pipeName", pipeName);
    String query = "drop pipe if exists identifier(?)";

    try
    {
      PreparedStatement stmt = conn.prepareStatement(query);
      stmt.setString(1, pipeName);
      stmt.execute();
      stmt.close();
    } catch (SQLException e)
    {
      throw SnowflakeErrors.ERROR_2001.getException(e);
    }

    logInfo("pipe {} dropped", pipeName);
  }

  @Override
  public boolean dropStageIfEmpty(final String stageName)
  {
    checkConnection();
    InternalUtils.assertNotEmpty("stageName", stageName);
    if (!stageExist(stageName))
    {
      return false;
    }
    String query = "list @" + stageName;
    try
    {
      PreparedStatement stmt = conn.prepareStatement(query);
      ResultSet resultSet = stmt.executeQuery();
      if (InternalUtils.resultSize(resultSet) == 0)
      {
        dropStage(stageName);
        stmt.close();
        resultSet.close();
        return true;
      }
      resultSet.close();
      stmt.close();
    } catch (SQLException e)
    {
      throw SnowflakeErrors.ERROR_2001.getException(e);
    }
    logInfo("stage {} can't be dropped because it is not empty", stageName);
    return false;
  }

  @Override
  public void dropStage(final String stageName)
  {
    checkConnection();
    InternalUtils.assertNotEmpty("stageName", stageName);
    String query = "drop stage if exists identifier(?)";
    try
    {
      PreparedStatement stmt = conn.prepareStatement(query);
      stmt.setString(1, stageName);
      stmt.execute();
      stmt.close();
    } catch (SQLException e)
    {
      throw SnowflakeErrors.ERROR_2001.getException(e);
    }
    logInfo("stage {} dropped", stageName);
  }

  @Override
  public void purgeStage(final String stageName, final List<String> files)
  {
    InternalUtils.assertNotEmpty("stageName", stageName);
    for (String fileName : files)
    {
      removeFile(stageName, fileName);
    }
    logInfo("purge {} files from stage: {}", files.size(), stageName);
  }

  @Override
  public void moveToTableStage(final String tableName, final String stageName
    , final List<String> files)
  {
    InternalUtils.assertNotEmpty("tableName", tableName);
    InternalUtils.assertNotEmpty("stageName", stageName);
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
        sfconn.uploadStream("%" + tableName,
          FileNameUtils.getPrefixFromFileName(name), file,
          FileNameUtils.removePrefixAndGZFromFileName(name), true);
      } catch (SQLException e)
      {
        throw SnowflakeErrors.ERROR_2003.getException(e);
      }
      logInfo("moved file: {} from stage: {} to table stage: {}", name,
        stageName, tableName);
      //remove
      removeFile(stageName, name);
    }
  }

  @Override
  public void moveToTableStage(final String tableName, final String stageName
    , final String prefix)
  {
    InternalUtils.assertNotEmpty("tableName", tableName);
    InternalUtils.assertNotEmpty("stageName", stageName);
    List<String> files = listStage(stageName, prefix);
    moveToTableStage(tableName, stageName, files);
  }

  @Override
  public List<String> listStage(final String stageName, final String prefix,
                                final boolean isTableStage)
  {
    InternalUtils.assertNotEmpty("stageName", stageName);
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
      query = "ls @" + stageName + "/" + prefix;
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
      stmt.close();
      resultSet.close();
    } catch (SQLException e)
    {
      throw SnowflakeErrors.ERROR_2001.getException(e);
    }
    logInfo("list stage {} retrieved {} file names",
      stageName, result.size());
    return result;
  }

  @Override
  public List<String> listStage(final String stageName, final String prefix)
  {
    return listStage(stageName, prefix, false);
  }

  @Override
  public void put(final String stageName, final String fileName,
                  final String content)
  {
    InternalUtils.assertNotEmpty("stageName", stageName);
    SnowflakeConnectionV1 sfconn = (SnowflakeConnectionV1) conn;
    InputStream input = new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8));
    try
    {
      InternalUtils.backoffAndRetry(telemetry, SnowflakeInternalOperations.UPLOAD_FILE_TO_INTERNAL_STAGE,
          () ->
          {
            sfconn.uploadStream(stageName,
                                FileNameUtils.getPrefixFromFileName(fileName), input,
                                FileNameUtils.removePrefixAndGZFromFileName(fileName), true);
            return true;
          });
    } catch (Exception e)
    {
      throw SnowflakeErrors.ERROR_2003.getException(e);
    }
    logDebug("put file {} to stage {}", fileName, stageName);
  }

  @Override
  public void putWithCache(final String stageName, final String fileName,
                           final String content)
  {
    // If we don't know the stage type yet, query that first.
    if (stageType == null)
    {
      stageType = internalStage.getStageType(stageName);
    }
    // Normal upload for GCS, cached upload for Azure and S3.
    if (stageType == StageInfo.StageType.GCS)
    {
      put(stageName, fileName, content);
    }
    else if (stageType == StageInfo.StageType.AZURE || stageType == StageInfo.StageType.S3)
    {
      try
      {
        InternalUtils.backoffAndRetry(telemetry, SnowflakeInternalOperations.UPLOAD_FILE_TO_INTERNAL_STAGE_NO_CONNECTION,
          () ->
          {
            internalStage.putWithCache(stageName, fileName, content);
            return true;
          });
      } catch (Exception e)
      {
        throw SnowflakeErrors.ERROR_2011.getException(e);
      }
    }
  }

  @Override
  public void putToTableStage(final String tableName, final String fileName,
                              final byte[] content)
  {
    InternalUtils.assertNotEmpty("tableName", tableName);
    SnowflakeConnectionV1 sfconn = (SnowflakeConnectionV1) conn;
    InputStream input = new ByteArrayInputStream(content);

    try
    {
      InternalUtils.backoffAndRetry(telemetry, SnowflakeInternalOperations.UPLOAD_FILE_TO_TABLE_STAGE,
          () ->
          {
            sfconn.uploadStream("%" + tableName,
                                FileNameUtils.getPrefixFromFileName(fileName), input,
                                FileNameUtils.removePrefixAndGZFromFileName(fileName), true);
            return true;
          }
      );
    } catch (Exception e)
    {
      throw SnowflakeErrors.ERROR_2003.getException(e);
    }
    logInfo("put file: {} to table stage: {}", fileName, tableName);
  }

  @Override
  public SnowflakeTelemetryService getTelemetryClient()
  {
    return this.telemetry;
  }

  @Override
  public void close()
  {
    try
    {
      conn.close();
    } catch (SQLException e)
    {
      throw SnowflakeErrors.ERROR_2005.getException(e);
    }

    logInfo("snowflake connection closed");
  }

  @Override
  public boolean isClosed()
  {
    try
    {
      return conn.isClosed();
    } catch (SQLException e)
    {
      throw SnowflakeErrors.ERROR_2006.getException(e);
    }
  }

  @Override
  public String getConnectorName()
  {
    return this.connectorName;
  }

  @Override
  public SnowflakeIngestionService buildIngestService(final String stageName,
                                                      final String pipeName)
  {
    String account = url.getAccount();
    String user = prop.getProperty(InternalUtils.JDBC_USER);
    String host = url.getUrlWithoutPort();
    int port = url.getPort();
    String connectionScheme = url.getScheme();
    String fullPipeName = prop.getProperty(InternalUtils.JDBC_DATABASE) + "." +
      prop.getProperty(InternalUtils.JDBC_SCHEMA) + "." + pipeName;
    PrivateKey privateKey =
      (PrivateKey) prop.get(InternalUtils.JDBC_PRIVATE_KEY);
    return SnowflakeIngestionServiceFactory
        .builder(account, user, host, port, connectionScheme, stageName, fullPipeName, privateKey)
        .setTelemetry(this.telemetry)
        .build();
  }

  /**
   * make sure connection is not closed
   */
  private void checkConnection()
  {
    try
    {
      if (conn.isClosed())
      {
        throw SnowflakeErrors.ERROR_1003.getException();
      }
    } catch (SQLException e)
    {
      throw SnowflakeErrors.ERROR_1003.getException(e);
    }
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
    return "copy into " + tableName +
      "(RECORD_METADATA, RECORD_CONTENT) from (select $1:meta, $1:content from"
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
    InternalUtils.assertNotEmpty("stageName", stageName);
    String query = "rm @" + stageName + "/" + fileName;

    try
    {
      InternalUtils.backoffAndRetry(telemetry, SnowflakeInternalOperations.REMOVE_FILE_FROM_INTERNAL_STAGE,
        () ->
        {
          PreparedStatement stmt = conn.prepareStatement(query);
          stmt.execute();
          stmt.close();
          return true;
        });
    } catch (Exception e)
    {
      throw SnowflakeErrors.ERROR_2001.getException(e);
    }
    logDebug("deleted {} from stage {}", fileName, stageName);
  }

  @Override
  public Connection getConnection()
  {
    return this.conn;
  }
}
