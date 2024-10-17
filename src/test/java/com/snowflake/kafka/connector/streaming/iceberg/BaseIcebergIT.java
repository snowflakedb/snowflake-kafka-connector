package com.snowflake.kafka.connector.streaming.iceberg;

import static com.snowflake.kafka.connector.internal.TestUtils.executeQueryAndCollectResult;
import static com.snowflake.kafka.connector.internal.TestUtils.executeQueryWithParameter;

import com.snowflake.kafka.connector.internal.DescribeTableRow;
import com.snowflake.kafka.connector.internal.SnowflakeConnectionService;
import com.snowflake.kafka.connector.internal.TestUtils;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.function.Function;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

public class BaseIcebergIT {

  protected static SnowflakeConnectionService conn;

  @BeforeAll
  public static void setup() {
    conn = TestUtils.getConnectionServiceForStreaming();
  }

  @AfterAll
  public static void teardown() {
    conn.close();
  }

  protected static void createIcebergTable(String tableName) {
    createIcebergTableWithColumnClause(tableName, "record_metadata object()");
  }

  protected static void createIcebergTableNoSchemaEvolution(String tableName) {
    createIcebergTableWithColumnClause(
        tableName, "record_metadata object(), record_content object()");
  }

  protected static void createIcebergTableWithColumnClause(String tableName, String columnClause) {
    String query =
        "create or replace iceberg table identifier(?) ("
            + columnClause
            + ")"
            + "external_volume = 'test_exvol'"
            + "catalog = 'SNOWFLAKE'"
            + "base_location = 'it'";
    doExecuteQueryWithParameter(query, tableName);
    String allowStreamingIngestionQuery =
        "alter iceberg table identifier(?) set ALLOW_STREAMING_INGESTION_FOR_MANAGED_ICEBERG ="
            + " true;";
    doExecuteQueryWithParameter(allowStreamingIngestionQuery, tableName);
  }

  private static void doExecuteQueryWithParameter(String query, String tableName) {
    executeQueryWithParameter(conn.getConnection(), query, tableName);
  }

  protected static void dropIcebergTable(String tableName) {
    String query = "drop iceberg table if exists identifier(?)";
    doExecuteQueryWithParameter(query, tableName);
  }

  protected static void enableSchemaEvolution(String tableName) {
    String query = "alter iceberg table identifier(?) set enable_schema_evolution = true";
    doExecuteQueryWithParameter(query, tableName);
  }

  protected static <T> T select(
      String tableName, String query, Function<ResultSet, T> resultCollector) {
    return executeQueryAndCollectResult(conn.getConnection(), query, tableName, resultCollector);
  }

  protected static String describeRecordMetadataType(String tableName) {
    String query = "describe table identifier(?)";
    return executeQueryAndCollectResult(
        conn.getConnection(),
        query,
        tableName,
        (resultSet) -> {
          try {
            while (resultSet.next()) {
              if (resultSet.getString("name").equals("RECORD_METADATA")) {
                return resultSet.getString("type");
              }
            }
          } catch (SQLException e) {
            throw new RuntimeException(e);
          }
          throw new IllegalArgumentException("RECORD_METADATA column not found in the table");
        });
  }

  protected static List<DescribeTableRow> describeTable(String tableName) {
    return conn.describeTable(tableName)
        .orElseThrow(() -> new IllegalArgumentException("Table not found"));
  }
}
