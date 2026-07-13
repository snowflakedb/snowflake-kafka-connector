package com.snowflake.kafka.connector.internal;

import static com.snowflake.kafka.connector.Utils.TABLE_COLUMN_METADATA;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.snowflake.kafka.connector.internal.advisory.AdvisoryMessage;
import com.snowflake.kafka.connector.internal.advisory.KcAdvisoryResponse;
import com.snowflake.kafka.connector.internal.schemaevolution.ColumnInfos;
import com.snowflake.kafka.connector.internal.streaming.v2.migration.Ssv1MigrationResponse;
import com.snowflake.kafka.connector.internal.telemetry.SnowflakeTelemetryService;
import com.snowflake.kafka.connector.internal.telemetry.SnowflakeTelemetryServiceFactory;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import net.snowflake.client.api.driver.SnowflakeDriver;

/**
 * Implementation of Snowflake Connection Service interface which includes all handshake between KC
 * and SF through JDBC connection.
 */
public class StandardSnowflakeConnectionService implements SnowflakeConnectionService {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private static final String COLUMN_COMMENT =
      "created by automatic table creation from Snowflake Kafka Connector High Performance";

  private static final String SHOW_ICEBERG_TABLES_QUERY = "show iceberg tables like ? limit 1";
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
  public void createTableWithOnlyMetadataColumn(final String tableName) {
    checkConnection();
    InternalUtils.assertNotEmpty("tableName", tableName);
    String createTableQuery =
        "create table if not exists identifier(?) (record_metadata variant comment '"
            + COLUMN_COMMENT
            + "') enable_schema_evolution = true error_logging = true";

    try {
      PreparedStatement stmt = conn.prepareStatement(createTableQuery);
      stmt.setString(1, quoteIdentifier(tableName));
      stmt.execute();
      stmt.close();
    } catch (SQLException e) {
      // Snowflake rejects CREATE TABLE IF NOT EXISTS when the name is already taken by an
      // ICEBERG TABLE (cross-type conflict is not suppressed by IF NOT EXISTS). KCv4 only
      // supports pre-created Iceberg tables; error_logging is not available for them.
      // We match on the error message text because Snowflake does not provide a stable SQL
      // error code that distinguishes this cross-type conflict from other CREATE TABLE errors.
      if (e.getMessage() != null && e.getMessage().contains("already exists as ICEBERG_TABLE")) {
        LOGGER.warn(
            "Table '{}' is a pre-created Iceberg table. Skipping auto-creation."
                + " Error table functionality is not available for Iceberg tables.",
            tableName);
        return;
      }
      throw SnowflakeErrors.ERROR_2007.getException(e);
    }

    LOGGER.info(
        "Created table {} with RECORD_METADATA column and ERROR_LOGGING enabled", tableName);
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
  public boolean isTableCompatible(final String tableName) {
    checkConnection();
    InternalUtils.assertNotEmpty("tableName", tableName);
    String query = "desc table identifier(?)";
    PreparedStatement stmt = null;
    ResultSet result = null;
    boolean compatible;
    try {
      stmt = conn.prepareStatement(query);
      stmt.setString(1, quoteIdentifier(tableName));
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
      stmt.setString(1, quoteIdentifier(tableName));
      ResultSet result = stmt.executeQuery();

      while (result.next()) {
        String columnName = result.getString("name");
        String type = result.getString("type");
        String comment = result.getString("comment");
        String nullable = result.getString("null?");
        String defaultValue = null;
        String autoincrement = null;
        try {
          defaultValue = result.getString("default");
          autoincrement = result.getString("autoincrement");
        } catch (SQLException e) {
          LOGGER.debug(
              "default/autoincrement columns not available in DESCRIBE TABLE for {}", tableName);
        }
        rows.add(
            new DescribeTableRow(columnName, type, comment, nullable, defaultValue, autoincrement));
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
  public boolean shouldEvolveSchema(String tableName, String role) {
    LOGGER.info("Checking schema evolution permission for table {}", tableName);
    checkConnection();
    InternalUtils.assertNotEmpty("tableName", tableName);
    InternalUtils.assertNotEmpty("role", role);

    String query = "show grants on table identifier(?)";
    List<String> schemaEvolutionAllowedPrivilegeList =
        Arrays.asList("EVOLVE SCHEMA", "ALL", "OWNERSHIP");
    boolean hasRolePrivilege = false;
    String myRole =
        (role.charAt(0) == '"' && role.charAt(role.length() - 1) == '"')
            ? role.substring(1, role.length() - 1)
            : role.toUpperCase();
    try {
      PreparedStatement stmt = conn.prepareStatement(query);
      stmt.setString(1, quoteIdentifier(tableName));
      ResultSet result = stmt.executeQuery();
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
      throw SnowflakeErrors.ERROR_2001.getException(e);
    }

    boolean hasTableOptionEnabled = false;
    String escapedTableName =
        tableName.replace("\\", "\\\\").replace("_", "\\_").replace("%", "\\%");
    for (String showQuery :
        new String[] {"show tables like ? limit 1", SHOW_ICEBERG_TABLES_QUERY}) {
      if (hasTableOptionEnabled) break;
      try (PreparedStatement stmt = conn.prepareStatement(showQuery)) {
        stmt.setString(1, escapedTableName);
        try (ResultSet result = stmt.executeQuery()) {
          while (result.next()) {
            String enableSchemaEvolution = "N";
            try {
              enableSchemaEvolution = result.getString("enable_schema_evolution");
            } catch (SQLException e) {
              LOGGER.warn(
                  "enable_schema_evolution column not found in SHOW output for table {}: {}",
                  tableName,
                  e.getMessage());
            }
            if (enableSchemaEvolution.equals("Y")) {
              hasTableOptionEnabled = true;
            }
          }
        }
      } catch (SQLException e) {
        throw SnowflakeErrors.ERROR_2001.getException(e);
      }
    }

    boolean hasPermission = hasRolePrivilege && hasTableOptionEnabled;
    LOGGER.info(
        "Table: {} has schema evolution permission: {} (hasRolePrivilege={},"
            + " hasTableOptionEnabled={})",
        tableName,
        hasPermission,
        hasRolePrivilege,
        hasTableOptionEnabled);
    return hasPermission;
  }

  @Override
  public boolean isIcebergTable(String tableName) {
    checkConnection();
    InternalUtils.assertNotEmpty("tableName", tableName);
    try (PreparedStatement stmt = conn.prepareStatement(SHOW_ICEBERG_TABLES_QUERY)) {
      String escapedTableName =
          tableName.replace("\\", "\\\\").replace("_", "\\_").replace("%", "\\%");
      stmt.setString(1, escapedTableName);
      try (ResultSet result = stmt.executeQuery()) {
        boolean iceberg = result.next();
        LOGGER.info("Table {} isIcebergTable={}", tableName, iceberg);
        return iceberg;
      }
    } catch (SQLException e) {
      throw SnowflakeErrors.ERROR_2001.getException(e);
    }
  }

  @Override
  public boolean hasErrorLoggingEnabled(String tableName) {
    checkConnection();
    InternalUtils.assertNotEmpty("tableName", tableName);

    try (PreparedStatement stmt = conn.prepareStatement("show tables like ? limit 1")) {
      String escapedTableName =
          tableName.replace("\\", "\\\\").replace("_", "\\_").replace("%", "\\%");
      stmt.setString(1, escapedTableName);
      try (ResultSet result = stmt.executeQuery()) {
        if (result.next()) {
          try {
            if ("ON".equals(result.getString("error_logging"))) {
              LOGGER.debug("Table {} has ERROR_LOGGING enabled", tableName);
              return true;
            }
          } catch (SQLException e) {
            // error_logging column absent in result set — treat as disabled to surface a warning
            LOGGER.warn(
                "error_logging column not found in SHOW TABLES output for table {} —"
                    + " treating as disabled",
                tableName);
            return false;
          }
        }
      }
    } catch (SQLException e) {
      throw SnowflakeErrors.ERROR_2001.getException(e);
    }
    LOGGER.debug("Table {} does not have ERROR_LOGGING enabled", tableName);
    return false;
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

    // identifier(?) works for the table name but NOT for column names in ADD COLUMN.
    // Column names are quoted inline to preserve case (e.g. "age" vs "AGE").
    // Iceberg tables require ALTER ICEBERG TABLE instead of ALTER TABLE.
    String alterKeyword = isIcebergTable(tableName) ? "alter iceberg table" : "alter table";
    StringBuilder query =
        new StringBuilder(alterKeyword + " identifier(?) add column if not exists ");
    boolean first = true;
    for (Map.Entry<String, ColumnInfos> entry : columnInfosMap.entrySet()) {
      if (!first) {
        query.append(", if not exists ");
      }
      query.append(quoteIdentifier(entry.getKey()));
      query.append(" ");
      query.append(entry.getValue().getColumnType());
      query.append(entry.getValue().getDdlComments());
      first = false;
    }

    try (PreparedStatement stmt = conn.prepareStatement(query.toString())) {
      stmt.setString(1, quoteIdentifier(tableName));
      stmt.execute();
      LOGGER.info("Added columns to table {}: {}", tableName, columnInfosMap.keySet());
    } catch (SQLException e) {
      LOGGER.warn(
          "ALTER TABLE/ICEBERG TABLE ADD COLUMN failed for table {} (may be concurrent race"
              + " condition): {}",
          tableName,
          e.getMessage());
      throw SnowflakeErrors.ERROR_2015.getException(e);
    }
  }

  @Override
  public Ssv1MigrationResponse migrateSsv1ChannelOffset(
      String tableName, String ssv1ChannelName, String ssv2ChannelName, String pipeName) {
    checkConnection();
    LOGGER.info(
        "Calling SYSTEM$MIGRATE_SSV1_CHANNEL_OFFSET for table={}, ssv1Channel={}, "
            + "ssv2Channel={}, pipe={}",
        tableName,
        ssv1ChannelName,
        ssv2ChannelName,
        pipeName);

    String query = "SELECT SYSTEM$MIGRATE_SSV1_CHANNEL_OFFSET(?, ?, ?, ?)";
    try (PreparedStatement stmt = conn.prepareStatement(query)) {
      stmt.setString(1, quoteIdentifier(tableName));
      // The backend should unquote/uppercase the channel name, but that fix is not yet rolled out.
      // Uppercase here as a workaround
      // TODO(SNOW-3360048): Remove once the backend fix is rolled out.
      stmt.setString(2, ssv1ChannelName.toUpperCase(Locale.ROOT));
      stmt.setString(3, ssv2ChannelName);
      stmt.setString(4, pipeName);
      try (ResultSet rs = stmt.executeQuery()) {
        if (!rs.next()) {
          throw new RuntimeException(
              "SYSTEM$MIGRATE_SSV1_CHANNEL_OFFSET returned no result for table " + tableName);
        }
        String jsonResponse = rs.getString(1);
        try {
          return OBJECT_MAPPER.readValue(jsonResponse, Ssv1MigrationResponse.class);
        } catch (Exception e) {
          throw new RuntimeException(
              "Failed to parse SYSTEM$MIGRATE_SSV1_CHANNEL_OFFSET response for channel "
                  + ssv1ChannelName,
              e);
        }
      }
    } catch (SQLException e) {
      throw new RuntimeException(
          "SYSTEM$MIGRATE_SSV1_CHANNEL_OFFSET failed for ssv1Channel="
              + ssv1ChannelName
              + ", ssv2Channel="
              + ssv2ChannelName
              + ": "
              + e.getMessage(),
          e);
    }
  }

  @Override
  public List<AdvisoryMessage> getKcAdvisoryMessages(String requestJson) {
    try {
      checkConnection();
      String query = "SELECT SYSTEM$GET_KC_ADVISORY_MESSAGES(?)";
      try (PreparedStatement stmt = conn.prepareStatement(query)) {
        stmt.setString(1, requestJson);
        try (ResultSet rs = stmt.executeQuery()) {
          if (!rs.next()) {
            return Collections.emptyList();
          }
          KcAdvisoryResponse response =
              OBJECT_MAPPER.readValue(rs.getString(1), KcAdvisoryResponse.class);
          return response.getMessages();
        }
      }
    } catch (Exception e) {
      // Fail-safe: an old GS without the function, a disabled/empty policy, or a parse error
      // must never disrupt the connector. Log at DEBUG only (not customer-facing).
      LOGGER.debug("SYSTEM$GET_KC_ADVISORY_MESSAGES unavailable or failed: {}", e.getMessage());
      return Collections.emptyList();
    }
  }

  @Override
  public void alterNonNullableColumns(String tableName, List<String> columnNames) {
    if (columnNames == null || columnNames.isEmpty()) {
      return;
    }
    checkConnection();
    InternalUtils.assertNotEmpty("tableName", tableName);

    // identifier(?) works for the table name but NOT for column names in ALTER ... DROP NOT NULL.
    // Column names are quoted inline to preserve case.
    // Iceberg tables require ALTER ICEBERG TABLE instead of ALTER TABLE.
    String alterKeyword = isIcebergTable(tableName) ? "alter iceberg table" : "alter table";
    StringBuilder query = new StringBuilder(alterKeyword + " identifier(?) alter ");
    boolean first = true;
    for (String colName : columnNames) {
      if (!first) {
        query.append(", ");
      }
      String quoted = quoteIdentifier(colName);
      query
          .append(quoted)
          .append(" drop not null, ")
          .append(quoted)
          .append(
              " comment 'column altered to be nullable by schema evolution from"
                  + " Snowflake Kafka Connector'");
      first = false;
    }

    try (PreparedStatement stmt = conn.prepareStatement(query.toString())) {
      stmt.setString(1, quoteIdentifier(tableName));
      stmt.execute();
      LOGGER.info("Dropped NOT NULL constraints on table {}: {}", tableName, columnNames);
    } catch (SQLException e) {
      LOGGER.warn(
          "ALTER TABLE/ICEBERG TABLE DROP NOT NULL failed for table {} (may be concurrent race"
              + " condition): {}",
          tableName,
          e.getMessage());
      throw SnowflakeErrors.ERROR_2016.getException(e);
    }
  }

  /**
   * Wraps a raw column name in double quotes to preserve case in DDL statements. Snowflake treats
   * unquoted identifiers as case-insensitive (uppercased), so quoting is required for
   * case-sensitive column names like {@code "age"} vs {@code "AGE"}. Internal double quotes are
   * escaped per SQL standard.
   */
  private static String quoteIdentifier(String name) {
    return "\"" + name.replace("\"", "\"\"") + "\"";
  }
}
