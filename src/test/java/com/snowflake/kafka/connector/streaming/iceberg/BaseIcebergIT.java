package com.snowflake.kafka.connector.streaming.iceberg;

import static com.snowflake.kafka.connector.internal.TestUtils.executeQueryWithParameter;

import com.snowflake.kafka.connector.internal.SnowflakeConnectionService;
import com.snowflake.kafka.connector.internal.TestUtils;
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

  protected static void createIcebergTableWithColumnClause(String tableName, String columnClause) {
    String query =
        "create or replace iceberg table identifier(?) ("
            + columnClause
            + ")"
            + "external_volume = 'test_exvol'"
            + "catalog = 'SNOWFLAKE'"
            + "base_location = 'it'";
    executeQueryWithParameter(query, tableName);
  }

  protected static void dropIcebergTable(String tableName) {
    String query = "drop iceberg table if exists identifier(?)";
    executeQueryWithParameter(query, tableName);
  }

  protected static void enableSchemaEvolution(String tableName) {
    String query = "alter iceberg table identifier(?) set enable_schema_evolution = true";
    executeQueryWithParameter(query, tableName);
  }
}
