package com.snowflake.kafka.connector.internal;

import static com.snowflake.kafka.connector.Utils.TABLE_COLUMN_METADATA;
import static com.snowflake.kafka.connector.streaming.iceberg.IcebergDDLTypes.ICEBERG_METADATA_OBJECT_SCHEMA;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.snowflake.kafka.connector.internal.streaming.ChannelMigrateOffsetTokenResponseDTO;
import com.snowflake.kafka.connector.internal.streaming.schemaevolution.ColumnInfos;
import com.snowflake.kafka.connector.internal.telemetry.SnowflakeTelemetryService;
import com.snowflake.kafka.connector.internal.telemetry.SnowflakeTelemetryServiceFactory;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;
import net.snowflake.client.jdbc.SnowflakeDriver;

/**
 * Implementation of Snowflake Connection Service interface which includes all handshake between KC
 * and SF through JDBC connection.
 */
public class StandardSnowflakeConnectionService implements SnowflakeConnectionService {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private final KCLogger LOGGER = new KCLogger(StandardSnowflakeConnectionService.class.getName());
  private final Connection conn;
  private final SnowflakeTelemetryService telemetry;
  private final String connectorName;
  private final String taskID;

  StandardSnowflakeConnectionService(
      JdbcProperties jdbcProperties, SnowflakeURL url, String connectorName, String taskID) {
    this.connectorName = connectorName;
    this.taskID = taskID;
    Properties proxyProperties = jdbcProperties.getProxyProperties();
    Properties combinedProperties = jdbcProperties.getProperties();
    try {
      if (!proxyProperties.isEmpty()) {
        LOGGER.debug("Proxy properties are set, passing in JDBC while creating the connection");
      } else {
        LOGGER.info("Establishing a JDBC connection with url:{}", url.getJdbcUrl());
      }
      this.conn = new SnowflakeDriver().connect(url.getJdbcUrl(), combinedProperties);
    } catch (SQLException e) {
      throw SnowflakeErrors.ERROR_1001.getException(e);
    }
    this.telemetry =
        SnowflakeTelemetryServiceFactory.builder(conn)
            .setAppName(this.connectorName)
            .setTaskID(this.taskID)
            .build();
    LOGGER.info("initialized the snowflake connection");
  }

  @Override
  public void createTable(final String tableName, final boolean overwrite) {
    checkConnection();
    InternalUtils.assertNotEmpty("tableName", tableName);
    String query;
    if (overwrite) {
      query = "create or replace table identifier(?) (record_metadata variant)";
    } else {
      query = "create table if not exists identifier(?) (record_metadata variant)";
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
  public void addMetadataColumnForIcebergIfNotExists(String tableName) {
    checkConnection();
    InternalUtils.assertNotEmpty("tableName", tableName);
    String query =
        "ALTER ICEBERG TABLE identifier(?) ADD COLUMN IF NOT EXISTS RECORD_METADATA "
            + ICEBERG_METADATA_OBJECT_SCHEMA;
    try {
      PreparedStatement stmt = conn.prepareStatement(query);
      stmt.setString(1, tableName);
      stmt.execute();
      stmt.close();
    } catch (SQLException e) {
      LOGGER.error(
          "Couldn't alter table {} add RECORD_METADATA column to align with iceberg format",
          tableName);
      throw SnowflakeErrors.ERROR_2019.getException(e);
    }
    LOGGER.info(
        "alter table {} add RECORD_METADATA column to align with iceberg format", tableName);
  }

  @Override
  public void initializeMetadataColumnTypeForIceberg(String tableName) {
    checkConnection();
    InternalUtils.assertNotEmpty("tableName", tableName);
    String query =
        "ALTER ICEBERG TABLE identifier(?) ALTER COLUMN RECORD_METADATA SET DATA TYPE "
            + ICEBERG_METADATA_OBJECT_SCHEMA;
    try {
      PreparedStatement stmt = conn.prepareStatement(query);
      stmt.setString(1, tableName);
      stmt.execute();
      stmt.close();
    } catch (SQLException e) {
      LOGGER.error(
          "Couldn't alter table {} RECORD_METADATA column type to align with iceberg format",
          tableName);
      throw SnowflakeErrors.ERROR_2018.getException(e);
    }
    LOGGER.info(
        "alter table {} RECORD_METADATA column type to align with iceberg format", tableName);
  }

  @Override
  public boolean tableExist(final String tableName) {
    return describeTable(tableName).isPresent();
  }

  @Override
  public boolean pipeExist(final String pipeName) {
    LOGGER.info("Calling DESCRIBE PIPE {}", pipeName);
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
  // TODO - use describeTable()
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
      boolean allNullable = true;
      while (result.next()) {
        switch (result.getString(1)) {
          case TABLE_COLUMN_METADATA:
            if (result.getString(2).equals("VARIANT")) {
              hasMeta = true;
            }
            break;
          default:
            if (result.getString(4).equals("N")) {
              allNullable = false;
            }
        }
      }
      compatible = hasMeta && allNullable;
    } catch (SQLException e) {
      LOGGER.debug("Table {} doesn't exist. Exception {}", tableName, e.getStackTrace());
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
    LOGGER.info("Table {} compatibility is {}", tableName, compatible);
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
    LOGGER.info("Checking schema evolution permission for table {}", tableName);
    checkConnection();
    InternalUtils.assertNotEmpty("tableName", tableName);
    String query = "show grants on table identifier(?)";
    List<String> schemaEvolutionAllowedPrivilegeList =
        Arrays.asList("EVOLVE SCHEMA", "ALL", "OWNERSHIP");
    ResultSet result = null;
    // whether the role has the privilege to do schema evolution (EVOLVE SCHEMA / ALL / OWNERSHIP)
    boolean hasRolePrivilege = false;
    String myRole = FormattingUtils.formatName(role);
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
   * Alter iceberg table to modify columns datatype
   *
   * @param tableName the name of the table
   * @param columnInfosMap the mapping from the columnNames to their infos
   */
  @Override
  public void alterColumnsDataTypeIcebergTable(
      String tableName, Map<String, ColumnInfos> columnInfosMap) {
    LOGGER.debug("Modifying data types of iceberg table columns");
    String alterSetDatatypeQuery = generateAlterSetDataTypeQuery(columnInfosMap);
    executeStatement(tableName, alterSetDatatypeQuery);
  }

  private String generateAlterSetDataTypeQuery(Map<String, ColumnInfos> columnsToModify) {
    StringBuilder setDataTypeQuery = new StringBuilder("alter iceberg ");
    setDataTypeQuery.append("table identifier(?) alter column ");

    String columnsPart =
        columnsToModify.entrySet().stream()
            .map(
                column -> {
                  String columnName = column.getKey();
                  String dataType = column.getValue().getColumnType();
                  return columnName + " set data type " + dataType;
                })
            .collect(Collectors.joining(", "));

    setDataTypeQuery.append(columnsPart);

    return setDataTypeQuery.toString();
  }

  /**
   * Alter table to add columns according to a map from columnNames to their types
   *
   * @param tableName the name of the table
   * @param columnInfosMap the mapping from the columnNames to their infos
   */
  @Override
  public void appendColumnsToTable(String tableName, Map<String, ColumnInfos> columnInfosMap) {
    LOGGER.debug("Appending columns to snowflake table");
    appendColumnsToTable(tableName, columnInfosMap, false);
  }

  /**
   * Alter iceberg table to add columns according to a map from columnNames to their types
   *
   * @param tableName the name of the table
   * @param columnInfosMap the mapping from the columnNames to their infos
   */
  @Override
  public void appendColumnsToIcebergTable(
      String tableName, Map<String, ColumnInfos> columnInfosMap) {
    LOGGER.debug("Appending columns to iceberg table");
    appendColumnsToTable(tableName, columnInfosMap, true);
  }

  private void appendColumnsToTable(
      String tableName, Map<String, ColumnInfos> columnInfosMap, boolean isIcebergTable) {
    checkConnection();
    InternalUtils.assertNotEmpty("tableName", tableName);
    StringBuilder appendColumnQuery = new StringBuilder("alter ");
    if (isIcebergTable) {
      appendColumnQuery.append("iceberg ");
    }
    appendColumnQuery.append("table identifier(?) add column if not exists ");
    boolean first = true;
    StringBuilder logColumn = new StringBuilder("[");

    for (String columnName : columnInfosMap.keySet()) {
      if (first) {
        first = false;
      } else {
        appendColumnQuery.append(", if not exists ");
        logColumn.append(",");
      }
      ColumnInfos columnInfos = columnInfosMap.get(columnName);

      appendColumnQuery
          .append(columnName)
          .append(" ")
          .append(columnInfos.getColumnType())
          .append(columnInfos.getDdlComments());
      logColumn.append(columnName).append(" (").append(columnInfosMap.get(columnName)).append(")");
    }

    executeStatement(tableName, appendColumnQuery.toString());

    logColumn.insert(0, "Following columns created for table {}:\n").append("]");
    LOGGER.info(logColumn.toString(), tableName);
  }

  private void executeStatement(String tableName, String query) {
    try {
      LOGGER.info("Trying to run query: {}", query);
      PreparedStatement stmt = conn.prepareStatement(query);
      stmt.setString(1, tableName);
      stmt.execute();
      stmt.close();
    } catch (SQLException e) {
      throw SnowflakeErrors.ERROR_2015.getException(e);
    }
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

  @Override
  public Connection getConnection() {
    return this.conn;
  }

  @Override
  public Optional<List<DescribeTableRow>> describeTable(String tableName) {
    LOGGER.info("Calling DESCRIBE TABLE {}", tableName);
    checkConnection();
    String query = "desc table identifier(?)";
    PreparedStatement stmt = null;
    List<DescribeTableRow> rows = new ArrayList<>();

    try {
      stmt = conn.prepareStatement(query);
      stmt.setString(1, tableName);
      ResultSet result = stmt.executeQuery();

      while (result.next()) {
        String columnName = result.getString("name");
        String type = result.getString("type");
        String comment = result.getString("comment");
        rows.add(new DescribeTableRow(columnName, type, comment));
      }
      return Optional.of(rows);
    } catch (Exception e) {
      LOGGER.debug("table {} doesn't exist", tableName);
      return Optional.empty();
    } finally {
      if (stmt != null) {
        try {
          stmt.close();
        } catch (SQLException e) {
          e.printStackTrace();
        }
      }
    }
  }

  @Override
  public void executeQueryWithParameters(String query, String... parameters) {
    try {
      PreparedStatement stmt = conn.prepareStatement(query);
      for (int i = 0; i < parameters.length; i++) {
        stmt.setString(i + 1, parameters[i]);
      }
      stmt.execute();
      stmt.close();
    } catch (Exception e) {
      throw new RuntimeException("Error executing query: " + query, e);
    }
  }

  @VisibleForTesting
  protected ChannelMigrateOffsetTokenResponseDTO getChannelMigrateOffsetTokenResponseDTO(
      String migrateOffsetTokenResultFromSysFunc) throws JsonProcessingException {
    ChannelMigrateOffsetTokenResponseDTO channelMigrateOffsetTokenResponseDTO =
        OBJECT_MAPPER.readValue(
            migrateOffsetTokenResultFromSysFunc, ChannelMigrateOffsetTokenResponseDTO.class);
    return channelMigrateOffsetTokenResponseDTO;
  }

  public static class FormattingUtils {
    /**
     * Transform the objectName to uppercase unless it is enclosed in double quotes
     *
     * <p>In that case, drop the quotes and leave it as it is.
     *
     * @param objectName name of the snowflake object, could be tableName, columnName, roleName,
     *     etc.
     * @return Transformed objectName
     */
    public static String formatName(String objectName) {
      return (objectName.charAt(0) == '"' && objectName.charAt(objectName.length() - 1) == '"')
          ? objectName.substring(1, objectName.length() - 1)
          : objectName.toUpperCase();
    }
  }
}
