package com.snowflake.kafka.connector.internal;

import static com.snowflake.kafka.connector.Utils.TABLE_COLUMN_METADATA;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.internal.schemaevolution.ColumnInfos;
import com.snowflake.kafka.connector.internal.telemetry.SnowflakeTelemetryService;
import com.snowflake.kafka.connector.internal.telemetry.SnowflakeTelemetryServiceFactory;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
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
  public void createTableWithMetadataColumn(final String tableName, final boolean overwrite) {
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
  public void createTableWithMetadataColumn(final String tableName) {
    createTableWithMetadataColumn(tableName, false);
  }

  @Override
  public void createTableWithOnlyMetadataColumn(final String tableName) {
    checkConnection();
    InternalUtils.assertNotEmpty("tableName", tableName);
    String createTableQuery =
        "create table if not exists identifier(?) (record_metadata variant comment 'created by"
            + " automatic table creation from Snowflake Kafka Connector High Performance')"
            + " enable_schema_evolution = true";

    try {
      PreparedStatement stmt = conn.prepareStatement(createTableQuery);
      stmt.setString(1, tableName);
      stmt.execute();
      stmt.close();
    } catch (SQLException e) {
      throw SnowflakeErrors.ERROR_2007.getException(e);
    }

    LOGGER.info("Created table {} with only RECORD_METADATA column", tableName);
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
        String nullable = result.getString("null?");
        rows.add(new DescribeTableRow(columnName, type, comment, nullable));
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
  public boolean isSchemaEvolutionEnabled(String tableName) {
    checkConnection();
    InternalUtils.assertNotEmpty("tableName", tableName);
    String query = "SHOW TABLES LIKE '" + tableName + "' LIMIT 1";

    try (PreparedStatement stmt = conn.prepareStatement(query)) {
      ResultSet result = stmt.executeQuery();
      if (result.next()) {
        return "Y".equals(result.getString("enable_schema_evolution"));
      }
      return false;
    } catch (SQLException e) {
      LOGGER.warn(
          "Failed to check ENABLE_SCHEMA_EVOLUTION for table {}: {}", tableName, e.getMessage());
      return false;
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

  @Override
  public void appendColumnsToTable(String tableName, Map<String, ColumnInfos> columnInfosMap) {
    if (columnInfosMap == null || columnInfosMap.isEmpty()) {
      return;
    }
    checkConnection();
    InternalUtils.assertNotEmpty("tableName", tableName);

    StringBuilder query = new StringBuilder("alter table identifier(?) add column if not exists ");
    boolean first = true;
    for (Map.Entry<String, ColumnInfos> entry : columnInfosMap.entrySet()) {
      if (!first) {
        query.append(", if not exists ");
      }
      query.append(Utils.quoteNameIfNeeded(entry.getKey()));
      query.append(" ");
      query.append(entry.getValue().getColumnType());
      query.append(entry.getValue().getDdlComments());
      first = false;
    }

    try (PreparedStatement stmt = conn.prepareStatement(query.toString())) {
      stmt.setString(1, tableName);
      stmt.execute();
      LOGGER.info("Added columns to table {}: {}", tableName, columnInfosMap.keySet());
    } catch (SQLException e) {
      LOGGER.warn(
          "ALTER TABLE ADD COLUMN failed for table {} (may be concurrent race condition): {}",
          tableName,
          e.getMessage());
      throw SnowflakeErrors.ERROR_2015.getException(e);
    }
  }

  @Override
  public void alterNonNullableColumns(String tableName, List<String> columnNames) {
    if (columnNames == null || columnNames.isEmpty()) {
      return;
    }
    checkConnection();
    InternalUtils.assertNotEmpty("tableName", tableName);

    StringBuilder query = new StringBuilder("alter table identifier(?) alter ");
    boolean first = true;
    for (String columnName : columnNames) {
      if (!first) {
        query.append(", ");
      }
      String quotedName = Utils.quoteNameIfNeeded(columnName);
      query
          .append(quotedName)
          .append(" drop not null, ")
          .append(quotedName)
          .append(
              " comment 'column altered to be nullable by schema evolution from Snowflake Kafka"
                  + " Connector'");
      first = false;
    }

    try (PreparedStatement stmt = conn.prepareStatement(query.toString())) {
      stmt.setString(1, tableName);
      stmt.execute();
      LOGGER.info("Dropped NOT NULL constraints on table {}: {}", tableName, columnNames);
    } catch (SQLException e) {
      LOGGER.warn(
          "ALTER TABLE DROP NOT NULL failed for table {} (may be concurrent race condition): {}",
          tableName,
          e.getMessage());
      throw SnowflakeErrors.ERROR_2016.getException(e);
    }
  }
}
