package com.snowflake.kafka.connector.streaming.iceberg;

import static com.snowflake.kafka.connector.internal.TestUtils.executeQueryAndCollectResult;
import static com.snowflake.kafka.connector.internal.TestUtils.executeQueryWithParameter;

import com.snowflake.kafka.connector.internal.SnowflakeConnectionService;
import com.snowflake.kafka.connector.internal.TestUtils;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.function.Function;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

public class BaseIcebergIT {

  protected static SnowflakeConnectionService snowflakeDatabase;

  @BeforeAll
  public static void setup() {
    snowflakeDatabase = TestUtils.getConnectionServiceWithEncryptedKey();
  }

  @AfterAll
  public static void teardown() {
    snowflakeDatabase.close();
  }

  protected static void createIcebergTable(String tableName) {
    createIcebergTableWithColumnClause(tableName, "record_metadata object()", IcebergVersion.V2);
  }

  protected static void createIcebergTableWithColumnClause(
      String tableName, String columnClause, IcebergVersion icebergVersion) {
    String query =
        "create or replace iceberg table identifier(?) ("
            + columnClause
            + ") "
            + "external_volume = 'test_exvol' "
            + "catalog = 'SNOWFLAKE' "
            + "base_location = 'it' iceberg_version = "
            + (icebergVersion.ordinal() + 1)
            + ";";
    doExecuteQueryWithParameter(query, tableName);
  }

  private static void doExecuteQueryWithParameter(String query, String tableName) {
    executeQueryWithParameter(snowflakeDatabase.getConnection(), query, tableName);
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
    return executeQueryAndCollectResult(
        snowflakeDatabase.getConnection(), query, tableName, resultCollector);
  }

  protected static String describeRecordMetadataType(String tableName) {
    String query = "describe table identifier(?)";
    return executeQueryAndCollectResult(
        snowflakeDatabase.getConnection(),
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
}
