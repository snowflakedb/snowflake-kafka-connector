package com.snowflake.kafka.connector.internal;

import static com.snowflake.kafka.connector.Utils.TABLE_COLUMN_CONTENT;
import static com.snowflake.kafka.connector.Utils.TABLE_COLUMN_METADATA;

import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.internal.streaming.IngestionMethodConfig;
import com.snowflake.kafka.connector.internal.streaming.SchematizationUtils;
import com.snowflake.kafka.connector.internal.telemetry.SnowflakeTelemetryService;
import com.snowflake.kafka.connector.internal.telemetry.SnowflakeTelemetryServiceFactory;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.PrivateKey;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import net.snowflake.client.jdbc.SnowflakeConnectionV1;
import net.snowflake.client.jdbc.SnowflakeDriver;
import net.snowflake.client.jdbc.cloud.storage.StageInfo;

/**
 * Implementation of Snowflake Connection Service interface which includes all handshake between KC
 * and SF through JDBC connection.
 */
public class SnowflakeConnectionServiceV1 implements SnowflakeConnectionService {

  private final KCLogger LOGGER = new KCLogger(SnowflakeConnectionServiceV1.class.getName());

  private final Connection conn;
  private final SnowflakeTelemetryService telemetry;
  private final String connectorName;
  private final String taskID;
  private final Properties prop;

  // Placeholder for all proxy related properties set in the connector configuration
  private final Properties proxyProperties;
  private final SnowflakeURL url;
  private final SnowflakeInternalStage internalStage;

  // This info is provided in the connector configuration
  // This property will be appeneded to user agent while calling snowpipe API in http request
  private final String kafkaProvider;

  private StageInfo.StageType stageType;

  private static final long CREDENTIAL_EXPIRY_TIMEOUT_MILLIS = TimeUnit.MINUTES.toMillis(30);

  // User agent suffix we want to pass in to ingest service
  public static final String USER_AGENT_SUFFIX_FORMAT = "SFKafkaConnector/%s provider/%s";

  SnowflakeConnectionServiceV1(
      Properties prop,
      SnowflakeURL url,
      String connectorName,
      String taskID,
      Properties proxyProperties,
      String kafkaProvider,
      IngestionMethodConfig ingestionMethodConfig) {
    this.connectorName = connectorName;
    this.taskID = taskID;
    this.url = url;
    this.prop = prop;
    this.stageType = null;
    this.proxyProperties = proxyProperties;
    this.kafkaProvider = kafkaProvider;
    try {
      if (proxyProperties != null && !proxyProperties.isEmpty()) {
        Properties combinedProperties =
            mergeProxyAndConnectionProperties(this.prop, this.proxyProperties);
        LOGGER.debug("Proxy properties are set, passing in JDBC while creating the connection");
        this.conn = new SnowflakeDriver().connect(url.getJdbcUrl(), combinedProperties);
      } else {
        LOGGER.info("Establishing a JDBC connection with url:{}", url.getJdbcUrl());
        this.conn = new SnowflakeDriver().connect(url.getJdbcUrl(), prop);
      }
    } catch (SQLException e) {
      throw SnowflakeErrors.ERROR_1001.getException(e);
    }
    long credentialExpireTimeMillis = CREDENTIAL_EXPIRY_TIMEOUT_MILLIS;
    this.internalStage =
        new SnowflakeInternalStage(
            (SnowflakeConnectionV1) this.conn, credentialExpireTimeMillis, proxyProperties);
    this.telemetry =
        SnowflakeTelemetryServiceFactory.builder(conn, ingestionMethodConfig)
            .setAppName(this.connectorName)
            .setTaskID(this.taskID)
            .build();
    LOGGER.info("initialized the snowflake connection");
  }

  /* Merges the two properties. */
  private static Properties mergeProxyAndConnectionProperties(
      Properties connectionProperties, Properties proxyProperties) {
    assert connectionProperties != null;
    assert proxyProperties != null;
    Properties mergedProperties = new Properties();
    mergedProperties.putAll(connectionProperties);
    mergedProperties.putAll(proxyProperties);
    return mergedProperties;
  }

  @Override
  public void createTable(final String tableName, final boolean overwrite) {
    checkConnection();
    InternalUtils.assertNotEmpty("tableName", tableName);
    String query;
    if (overwrite) {
      query =
          "create or replace table if not exists identifier(?) (record_metadata "
              + "variant, record_content variant, row_id number "
              + "autoincrement, inserted_at timestamp_ntz "
              + "default sysdate())";
    } else {
      query =
          "create table if not exists identifier(?) (record_metadata "
              + "variant, record_content variant, row_id number "
              + "autoincrement, inserted_at timestamp_ntz "
              + "default sysdate())";
    }
    try {
      PreparedStatement stmt = conn.prepareStatement(query);
      stmt.setString(1, tableName);
      stmt.execute();
      stmt.close();
    } catch (SQLException e) {
      throw SnowflakeErrors.ERROR_2007.getException(e);
    }

    LOGGER.info("create table {}", tableName);
  }

  @Override
  public void createTable(final String tableName) {
    createTable(tableName, false);
  }

  @Override
  public void createTableWithOnlyMetadataColumn(final String tableName) {
    checkConnection();
    InternalUtils.assertNotEmpty("tableName", tableName);
    String createTableQuery =
        "create table if not exists identifier(?) (record_metadata variant comment 'created by"
            + " automatic table creation from Snowflake Kafka Connector')";

    try {
      PreparedStatement stmt = conn.prepareStatement(createTableQuery);
      stmt.setString(1, tableName);
      stmt.execute();
      stmt.close();
    } catch (SQLException e) {
      throw SnowflakeErrors.ERROR_2007.getException(e);
    }

    // Enable schema evolution by default if the table is created by the connector
    String enableSchemaEvolutionQuery =
        "alter table identifier(?) set ENABLE_SCHEMA_EVOLUTION = true";
    try {
      PreparedStatement stmt = conn.prepareStatement(enableSchemaEvolutionQuery);
      stmt.setString(1, tableName);
      stmt.executeQuery();
    } catch (SQLException e) {
      // Skip the error given that schema evolution is still under PrPr
      LOGGER.warn(
          "Enable schema evolution failed on table: {}, message: {}", tableName, e.getMessage());
    }

    LOGGER.info("Created table {} with only RECORD_METADATA column", tableName);
  }

  @Override
  public void createPipe(
      final String tableName,
      final String stageName,
      final String pipeName,
      final boolean overwrite) {
    checkConnection();
    InternalUtils.assertNotEmpty("tableName", tableName);
    InternalUtils.assertNotEmpty("stageName", stageName);
    InternalUtils.assertNotEmpty("pipeName", pipeName);

    String query;
    if (overwrite) {
      query = "create or replace pipe identifier(?) ";
    } else {
      query = "create pipe if not exists identifier(?) ";
    }
    try {
      query += "as " + pipeDefinition(tableName, stageName);
      PreparedStatement stmt = conn.prepareStatement(query);
      stmt.setString(1, pipeName);
      stmt.execute();
      stmt.close();
    } catch (SQLException e) {
      throw SnowflakeErrors.ERROR_2009.getException(e);
    }
    LOGGER.info("create pipe: {}", pipeName);
  }

  @Override
  public void createPipe(final String tableName, final String stageName, final String pipeName) {
    createPipe(tableName, stageName, pipeName, false);
  }

  @Override
  public void createStage(final String stageName, final boolean overwrite) {
    checkConnection();
    InternalUtils.assertNotEmpty("stageName", stageName);

    String query;
    if (overwrite) {
      query = "create or replace stage identifier(?)";
    } else {
      query = "create stage if not exists identifier(?)";
    }
    try {
      PreparedStatement stmt = conn.prepareStatement(query);
      stmt.setString(1, stageName);
      stmt.execute();
      stmt.close();
    } catch (SQLException e) {
      throw SnowflakeErrors.ERROR_2008.getException(e);
    }
    LOGGER.info("create stage {}", stageName);
  }

  @Override
  public void createStage(final String stageName) {
    createStage(stageName, false);
  }

  @Override
  public boolean tableExist(final String tableName) {
    checkConnection();
    InternalUtils.assertNotEmpty("tableName", tableName);
    String query = "desc table identifier(?)";
    PreparedStatement stmt = null;
    boolean exist;
    try {
      stmt = conn.prepareStatement(query);
      stmt.setString(1, tableName);
      stmt.execute();
      exist = true;
    } catch (Exception e) {
      LOGGER.debug("table {} doesn't exist", tableName);
      exist = false;
    } finally {
      if (stmt != null) {
        try {
          stmt.close();
        } catch (SQLException e) {
          e.printStackTrace();
        }
      }
    }
    return exist;
  }

  @Override
  public boolean stageExist(final String stageName) {
    checkConnection();
    InternalUtils.assertNotEmpty("stageName", stageName);
    String query = "desc stage identifier(?)";
    PreparedStatement stmt = null;
    boolean exist;
    try {
      stmt = conn.prepareStatement(query);
      stmt.setString(1, stageName);
      stmt.execute();
      exist = true;
    } catch (SQLException e) {
      LOGGER.debug("stage {} doesn't exists", stageName);
      exist = false;
    } finally {
      if (stmt != null) {
        try {
          stmt.close();
        } catch (SQLException e) {
          e.printStackTrace();
        }
      }
    }
    return exist;
  }

  @Override
  public boolean pipeExist(final String pipeName) {
    checkConnection();
    InternalUtils.assertNotEmpty("pipeName", pipeName);
    String query = "desc pipe identifier(?)";
    PreparedStatement stmt = null;
    boolean exist;
    try {
      stmt = conn.prepareStatement(query);
      stmt.setString(1, pipeName);
      stmt.execute();
      exist = true;
    } catch (SQLException e) {
      LOGGER.debug("pipe {} doesn't exist", pipeName);
      exist = false;
    } finally {
      if (stmt != null) {
        try {
          stmt.close();
        } catch (SQLException e) {
          e.printStackTrace();
        }
      }
    }
    return exist;
  }

  @Override
  public boolean isTableCompatible(final String tableName) {
    checkConnection();
    InternalUtils.assertNotEmpty("tableName", tableName);
    String query = "desc table identifier(?)";
    PreparedStatement stmt = null;
    ResultSet result = null;
    boolean compatible;
    try {
      stmt = conn.prepareStatement(query);
      stmt.setString(1, tableName);
      result = stmt.executeQuery();
      boolean hasMeta = false;
      boolean hasContent = false;
      boolean allNullable = true;
      while (result.next()) {
        switch (result.getString(1)) {
          case TABLE_COLUMN_METADATA:
            if (result.getString(2).equals("VARIANT")) {
              hasMeta = true;
            }
            break;
          case TABLE_COLUMN_CONTENT:
            if (result.getString(2).equals("VARIANT")) {
              hasContent = true;
            }
            break;
          default:
            if (result.getString(4).equals("N")) {
              allNullable = false;
            }
        }
      }
      compatible = hasMeta && hasContent && allNullable;
    } catch (SQLException e) {
      LOGGER.debug("table {} doesn't exist", tableName);
      compatible = false;
    } finally {
      try {
        if (result != null) {
          result.close();
        }
      } catch (Exception e) {
        e.printStackTrace();
      }

      try {
        if (stmt != null) {
          stmt.close();
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    return compatible;
  }

  @Override
  public void appendMetaColIfNotExist(final String tableName) {
    checkConnection();
    InternalUtils.assertNotEmpty("tableName", tableName);
    String query = "desc table identifier(?)";
    PreparedStatement stmt = null;
    ResultSet result = null;
    boolean hasMeta = false;
    boolean isVariant = false;
    try {
      stmt = conn.prepareStatement(query);
      stmt.setString(1, tableName);
      result = stmt.executeQuery();
      while (result.next()) {
        // The result schema is row idx | column name | data type | kind | null? | ...
        if (result.getString(1).equals(TABLE_COLUMN_METADATA)) {
          hasMeta = true;
          if (result.getString(2).equals("VARIANT")) {
            isVariant = true;
          }
          break;
        }
      }
    } catch (SQLException e) {
      throw SnowflakeErrors.ERROR_2014.getException("table name: " + tableName);
    }
    try {
      if (!hasMeta) {
        String metaQuery = "alter table identifier(?) add RECORD_METADATA VARIANT";
        stmt = conn.prepareStatement(metaQuery);
        stmt.setString(1, tableName);
        stmt.executeQuery();
      } else {
        if (!isVariant) {
          throw SnowflakeErrors.ERROR_2012.getException("table name: " + tableName);
        }
      }
    } catch (SQLException e) {
      throw SnowflakeErrors.ERROR_2013.getException("table name: " + tableName);
    }
  }

  /**
   * Check whether the user has the role privilege to do schema evolution and whether the schema
   * evolution option is enabled on the table
   *
   * @param tableName the name of the table
   * @param role the role of the user
   * @return whether schema evolution has the required permission to be performed
   */
  @Override
  public boolean hasSchemaEvolutionPermission(String tableName, String role) {
    checkConnection();
    InternalUtils.assertNotEmpty("tableName", tableName);
    String query = "show grants on table identifier(?)";
    List<String> schemaEvolutionAllowedPrivilegeList =
        Arrays.asList("EVOLVE SCHEMA", "ALL", "OWNERSHIP");
    ResultSet result = null;
    // whether the role has the privilege to do schema evolution (EVOLVE SCHEMA / ALL / OWNERSHIP)
    boolean hasRolePrivilege = false;
    String myRole = SchematizationUtils.formatName(role);
    try {
      PreparedStatement stmt = conn.prepareStatement(query);
      stmt.setString(1, tableName);
      result = stmt.executeQuery();
      while (result.next()) {
        if (!result.getString("grantee_name").equals(myRole)) {
          continue;
        }
        if (schemaEvolutionAllowedPrivilegeList.contains(
            result.getString("privilege").toUpperCase())) {
          hasRolePrivilege = true;
        }
      }
      stmt.close();
    } catch (SQLException e) {
      throw SnowflakeErrors.ERROR_2017.getException(e);
    }

    // whether the table has ENABLE_SCHEMA_EVOLUTION option set to true on the table.
    boolean hasTableOptionEnabled = false;
    query = "show tables like '" + tableName + "' limit 1";
    try {
      PreparedStatement stmt = conn.prepareStatement(query);
      stmt.setString(1, tableName);
      result = stmt.executeQuery();
      while (result.next()) {
        String enableSchemaEvolution = "N";
        try {
          enableSchemaEvolution = result.getString("enable_schema_evolution");
        } catch (SQLException e) {
          // Do nothing since schema evolution is still in PrPr
        }
        if (enableSchemaEvolution.equals("Y")) {
          hasTableOptionEnabled = true;
        }
      }
      stmt.close();
    } catch (SQLException e) {
      throw SnowflakeErrors.ERROR_2017.getException(e);
    }

    boolean hasPermission = hasRolePrivilege && hasTableOptionEnabled;
    LOGGER.info(
        String.format("Table: %s has schema evolution permission: %s", tableName, hasPermission));
    return hasPermission;
  }

  /**
   * Alter table to add columns according to a map from columnNames to their types
   *
   * @param tableName the name of the table
   * @param columnToType the mapping from the columnNames to their types
   */
  @Override
  public void appendColumnsToTable(String tableName, Map<String, String> columnToType) {
    checkConnection();
    InternalUtils.assertNotEmpty("tableName", tableName);
    StringBuilder appendColumnQuery = new StringBuilder("alter table identifier(?) add column ");
    boolean first = true;
    StringBuilder logColumn = new StringBuilder("[");
    for (String columnName : columnToType.keySet()) {
      if (first) {
        first = false;
      } else {
        appendColumnQuery.append(", ");
        logColumn.append(",");
      }
      appendColumnQuery
          .append(columnName)
          .append(" ")
          .append(columnToType.get(columnName))
          .append(" comment 'column created by schema evolution from Snowflake Kafka Connector'");
      logColumn.append(columnName).append(" (").append(columnToType.get(columnName)).append(")");
    }
    try {
      LOGGER.info("Trying to run query: {}", appendColumnQuery.toString());
      PreparedStatement stmt = conn.prepareStatement(appendColumnQuery.toString());
      stmt.setString(1, tableName);
      stmt.execute();
      stmt.close();
    } catch (SQLException e) {
      throw SnowflakeErrors.ERROR_2015.getException(e);
    }

    logColumn.insert(0, "Following columns created for table {}:\n").append("]");
    LOGGER.info(logColumn.toString(), tableName);
  }

  /**
   * Alter table to drop non-nullability of a list of columns
   *
   * @param tableName the name of the table
   * @param columnNames the list of columnNames
   */
  @Override
  public void alterNonNullableColumns(String tableName, List<String> columnNames) {
    checkConnection();
    InternalUtils.assertNotEmpty("tableName", tableName);
    StringBuilder dropNotNullQuery = new StringBuilder("alter table identifier(?) alter ");
    boolean isFirstColumn = true;
    StringBuilder logColumn = new StringBuilder("[");
    for (String columnName : columnNames) {
      if (isFirstColumn) {
        isFirstColumn = false;
      } else {
        dropNotNullQuery.append(", ");
        logColumn.append(", ");
      }
      dropNotNullQuery
          .append(columnName)
          .append(" drop not null, ")
          .append(columnName)
          .append(
              " comment 'column altered to be nullable by schema evolution from Snowflake Kafka"
                  + " Connector'");
      logColumn.append(columnName);
    }
    try {
      LOGGER.info("Trying to run query: {}", dropNotNullQuery.toString());
      PreparedStatement stmt = conn.prepareStatement(dropNotNullQuery.toString());
      stmt.setString(1, tableName);
      stmt.execute();
      stmt.close();
    } catch (SQLException e) {
      throw SnowflakeErrors.ERROR_2016.getException(e);
    }

    logColumn
        .insert(0, "Following columns' non-nullabilty was dropped for table {}:\n")
        .append("]");
    LOGGER.info(logColumn.toString(), tableName);
  }

  @Override
  public boolean isStageCompatible(final String stageName) {
    checkConnection();
    InternalUtils.assertNotEmpty("stageName", stageName);
    if (!stageExist(stageName)) {
      LOGGER.debug("stage {} doesn't exists", stageName);
      return false;
    }
    List<String> files = listStage(stageName, "");
    for (String name : files) {
      if (!FileNameUtils.verifyFileName(name)) {
        LOGGER.debug("file name {} in stage {} is not valid", name, stageName);
        return false;
      }
    }
    return true;
  }

  @Override
  public boolean isPipeCompatible(
      final String tableName, final String stageName, final String pipeName) {
    checkConnection();
    InternalUtils.assertNotEmpty("tableName", tableName);
    InternalUtils.assertNotEmpty("stageName", stageName);
    InternalUtils.assertNotEmpty("pipeName", pipeName);
    if (!pipeExist(pipeName)) {
      return false;
    }

    String query = "desc pipe identifier(?)";
    PreparedStatement stmt = null;
    ResultSet result = null;
    boolean compatible;
    try {
      stmt = conn.prepareStatement(query);
      stmt.setString(1, pipeName);
      result = stmt.executeQuery();
      if (!result.next()) {
        compatible = false;
      } else {
        String definition = result.getString("definition");
        LOGGER.debug("pipe {} definition: {}", pipeName, definition);
        compatible = definition.equalsIgnoreCase(pipeDefinition(tableName, stageName));
      }

    } catch (SQLException e) {
      LOGGER.debug("pipe {} doesn't exists ", pipeName);
      compatible = false;
    } finally {
      try {
        if (stmt != null) {
          stmt.close();
        }
        if (result != null) {
          result.close();
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    return compatible;
  }

  @Override
  public void databaseExists(String databaseName) {
    checkConnection();
    String query = "use database identifier(?)";
    try {
      PreparedStatement stmt = conn.prepareStatement(query);
      stmt.setString(1, databaseName);
      stmt.execute();
      stmt.close();
    } catch (SQLException e) {
      throw SnowflakeErrors.ERROR_2001.getException(e);
    }

    LOGGER.info("database {} exists", databaseName);
  }

  @Override
  public void schemaExists(String schemaName) {
    checkConnection();
    String query = "use schema identifier(?)";
    boolean foundSchema = false;
    try {
      PreparedStatement stmt = conn.prepareStatement(query);
      stmt.setString(1, schemaName);
      stmt.execute();
      stmt.close();
    } catch (SQLException e) {
      throw SnowflakeErrors.ERROR_2001.getException(e);
    }

    LOGGER.info("schema {} exists", schemaName);
  }

  @Override
  public void dropPipe(final String pipeName) {
    checkConnection();
    InternalUtils.assertNotEmpty("pipeName", pipeName);
    String query = "drop pipe if exists identifier(?)";

    try {
      PreparedStatement stmt = conn.prepareStatement(query);
      stmt.setString(1, pipeName);
      stmt.execute();
      stmt.close();
    } catch (SQLException e) {
      throw SnowflakeErrors.ERROR_2001.getException(e);
    }

    LOGGER.info("pipe {} dropped", pipeName);
  }

  @Override
  public boolean dropStageIfEmpty(final String stageName) {
    checkConnection();
    InternalUtils.assertNotEmpty("stageName", stageName);
    if (!stageExist(stageName)) {
      return false;
    }
    String query = "list @" + stageName;
    try {
      PreparedStatement stmt = conn.prepareStatement(query);
      ResultSet resultSet = stmt.executeQuery();
      if (InternalUtils.resultSize(resultSet) == 0) {
        dropStage(stageName);
        stmt.close();
        resultSet.close();
        return true;
      }
      resultSet.close();
      stmt.close();
    } catch (SQLException e) {
      throw SnowflakeErrors.ERROR_2001.getException(e);
    }
    LOGGER.info("stage {} can't be dropped because it is not empty", stageName);
    return false;
  }

  @Override
  public void dropStage(final String stageName) {
    checkConnection();
    InternalUtils.assertNotEmpty("stageName", stageName);
    String query = "drop stage if exists identifier(?)";
    try {
      PreparedStatement stmt = conn.prepareStatement(query);
      stmt.setString(1, stageName);
      stmt.execute();
      stmt.close();
    } catch (SQLException e) {
      throw SnowflakeErrors.ERROR_2001.getException(e);
    }
    LOGGER.info("stage {} dropped", stageName);
  }

  @Override
  public void purgeStage(final String stageName, final List<String> files) {
    InternalUtils.assertNotEmpty("stageName", stageName);
    for (String fileName : files) {
      removeFile(stageName, fileName);
    }
    LOGGER.info("purge {} files from stage: {}", files.size(), stageName);
  }

  @Override
  public void moveToTableStage(
      final String tableName, final String stageName, final List<String> files) {
    InternalUtils.assertNotEmpty("tableName", tableName);
    InternalUtils.assertNotEmpty("stageName", stageName);
    SnowflakeConnectionV1 sfconn = (SnowflakeConnectionV1) conn;

    for (String name : files) {
      // get
      InputStream file;
      try {
        file = sfconn.downloadStream(stageName, name, true);
      } catch (Exception e) {
        throw SnowflakeErrors.ERROR_2002.getException(e, this.telemetry);
      }
      // put
      try {
        sfconn.uploadStream(
            "%" + tableName,
            FileNameUtils.getPrefixFromFileName(name),
            file,
            FileNameUtils.removePrefixAndGZFromFileName(name),
            true);
      } catch (SQLException e) {
        throw SnowflakeErrors.ERROR_2003.getException(e, this.telemetry);
      }
      LOGGER.info("moved file: {} from stage: {} to table stage: {}", name, stageName, tableName);
      // remove
      removeFile(stageName, name);
    }
  }

  @Override
  public void moveToTableStage(
      final String tableName, final String stageName, final String prefix) {
    InternalUtils.assertNotEmpty("tableName", tableName);
    InternalUtils.assertNotEmpty("stageName", stageName);
    List<String> files = listStage(stageName, prefix);
    moveToTableStage(tableName, stageName, files);
  }

  @Override
  public List<String> listStage(
      final String stageName, final String prefix, final boolean isTableStage) {
    InternalUtils.assertNotEmpty("stageName", stageName);
    String query;
    int stageNameLength;
    if (isTableStage) {
      stageNameLength = 0;
      query = "ls @%" + stageName;
    } else {
      stageNameLength = stageName.length() + 1; // stage name + '/'
      query = "ls @" + stageName + "/" + prefix;
    }
    List<String> result;
    try {
      PreparedStatement stmt = conn.prepareStatement(query);
      ResultSet resultSet = stmt.executeQuery();

      result = new LinkedList<>();
      while (resultSet.next()) {
        result.add(resultSet.getString("name").substring(stageNameLength));
      }
      stmt.close();
      resultSet.close();
    } catch (SQLException e) {
      throw SnowflakeErrors.ERROR_2001.getException(e, this.telemetry);
    }
    LOGGER.info("list stage {} retrieved {} file names", stageName, result.size());
    return result;
  }

  @Override
  public List<String> listStage(final String stageName, final String prefix) {
    return listStage(stageName, prefix, false);
  }

  @Override
  @Deprecated
  // Only using it in test for performance testing
  public void put(final String stageName, final String fileName, final String content) {
    InternalUtils.assertNotEmpty("stageName", stageName);
    SnowflakeConnectionV1 sfconn = (SnowflakeConnectionV1) conn;
    InputStream input = new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8));
    try {
      InternalUtils.backoffAndRetry(
          telemetry,
          SnowflakeInternalOperations.UPLOAD_FILE_TO_INTERNAL_STAGE,
          () -> {
            sfconn.uploadStream(
                stageName,
                FileNameUtils.getPrefixFromFileName(fileName),
                input,
                FileNameUtils.removePrefixAndGZFromFileName(fileName),
                true);
            return true;
          });
    } catch (Exception e) {
      throw SnowflakeErrors.ERROR_2003.getException(e);
    }
    LOGGER.debug("put file {} to stage {}", fileName, stageName);
  }

  @Override
  public void putWithCache(final String stageName, final String fileName, final String content) {
    // If we don't know the stage type yet, query that first.
    if (stageType == null) {
      stageType = internalStage.getStageType(stageName);
    }
    try {
      InternalUtils.backoffAndRetry(
          telemetry,
          SnowflakeInternalOperations.UPLOAD_FILE_TO_INTERNAL_STAGE_NO_CONNECTION,
          () -> {
            internalStage.putWithCache(stageName, fileName, content, stageType);
            return true;
          });
    } catch (Exception e) {
      LOGGER.error(
          "Put With Cache(uploadWithoutConnection) failed after multiple retries for stageName:{},"
              + " stageType:{}, fullFilePath:{}",
          stageName,
          stageType,
          fileName);
      throw SnowflakeErrors.ERROR_2011.getException(e, this.telemetry);
    }
  }

  @Override
  public void putToTableStage(final String tableName, final String fileName, final byte[] content) {
    InternalUtils.assertNotEmpty("tableName", tableName);
    SnowflakeConnectionV1 sfconn = (SnowflakeConnectionV1) conn;
    InputStream input = new ByteArrayInputStream(content);

    try {
      InternalUtils.backoffAndRetry(
          telemetry,
          SnowflakeInternalOperations.UPLOAD_FILE_TO_TABLE_STAGE,
          () -> {
            sfconn.uploadStream(
                "%" + tableName,
                FileNameUtils.getPrefixFromFileName(fileName),
                input,
                FileNameUtils.removePrefixAndGZFromFileName(fileName),
                true);
            return true;
          });
    } catch (Exception e) {
      throw SnowflakeErrors.ERROR_2003.getException(e, this.telemetry);
    }
    LOGGER.info("put file: {} to table stage: {}", fileName, tableName);
  }

  @Override
  public SnowflakeTelemetryService getTelemetryClient() {
    return this.telemetry;
  }

  @Override
  public void close() {
    try {
      conn.close();
    } catch (SQLException e) {
      throw SnowflakeErrors.ERROR_2005.getException(e, this.telemetry);
    }

    LOGGER.info("snowflake connection closed");
  }

  @Override
  public boolean isClosed() {
    try {
      return conn.isClosed();
    } catch (SQLException e) {
      throw SnowflakeErrors.ERROR_2006.getException(e, this.telemetry);
    }
  }

  @Override
  public String getConnectorName() {
    return this.connectorName;
  }

  @Override
  public SnowflakeIngestionService buildIngestService(
      final String stageName, final String pipeName) {
    String account = url.getAccount();
    String user = prop.getProperty(InternalUtils.JDBC_USER);
    String userAgentSuffixInHttpRequest =
        String.format(USER_AGENT_SUFFIX_FORMAT, Utils.VERSION, kafkaProvider);
    String host = url.getUrlWithoutPort();
    int port = url.getPort();
    String connectionScheme = url.getScheme();
    String fullPipeName =
        prop.getProperty(InternalUtils.JDBC_DATABASE)
            + "."
            + prop.getProperty(InternalUtils.JDBC_SCHEMA)
            + "."
            + pipeName;
    PrivateKey privateKey = (PrivateKey) prop.get(InternalUtils.JDBC_PRIVATE_KEY);
    return SnowflakeIngestionServiceFactory.builder(
            account,
            user,
            host,
            port,
            connectionScheme,
            stageName,
            fullPipeName,
            privateKey,
            userAgentSuffixInHttpRequest)
        .setTelemetry(this.telemetry)
        .build();
  }

  /** make sure connection is not closed */
  private void checkConnection() {
    try {
      if (conn.isClosed()) {
        throw SnowflakeErrors.ERROR_1003.getException();
      }
    } catch (SQLException e) {
      throw SnowflakeErrors.ERROR_1003.getException(e, this.telemetry);
    }
  }

  /**
   * generate pipe definition
   *
   * @param tableName table name
   * @param stageName stage name
   * @return pipe definition string
   */
  private String pipeDefinition(String tableName, String stageName) {
    return "copy into "
        + tableName
        + "(RECORD_METADATA, RECORD_CONTENT) from (select $1:meta, $1:content from"
        + " @"
        + stageName
        + " t) file_format = (type = 'json')";
  }

  /**
   * Remove one file from given stage
   *
   * @param stageName stage name
   * @param fileName file name
   */
  private void removeFile(String stageName, String fileName) {
    InternalUtils.assertNotEmpty("stageName", stageName);
    String query = "rm @" + stageName + "/" + fileName;

    try {
      InternalUtils.backoffAndRetry(
          telemetry,
          SnowflakeInternalOperations.REMOVE_FILE_FROM_INTERNAL_STAGE,
          () -> {
            PreparedStatement stmt = conn.prepareStatement(query);
            stmt.execute();
            stmt.close();
            return true;
          });
    } catch (Exception e) {
      throw SnowflakeErrors.ERROR_2001.getException(e, this.telemetry);
    }
    LOGGER.debug("deleted {} from stage {}", fileName, stageName);
  }

  @Override
  public Connection getConnection() {
    return this.conn;
  }

  public SnowflakeInternalStage getInternalStage() {
    return this.internalStage;
  }
}
