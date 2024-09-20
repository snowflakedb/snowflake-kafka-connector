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

  protected static void createIcebergTable(String tableName) throws Exception {
    String query =
        "create or replace iceberg table identifier(?) (record_metadata object())"
            + "external_volume = 'test_exvol'"
            + "catalog = 'SNOWFLAKE'"
            + "base_location = 'it'";
    executeQueryWithParameter(query, tableName);
  }

  protected static void dropIcebergTable(String tableName) {
    String query = "drop iceberg table if exists identifier(?)";
    executeQueryWithParameter(query, tableName);
  }

  protected static void enableSchemaEvolution(String tableName) throws Exception {
    String query = "alter iceberg table identifier(?) set enable_schema_evolution = true";
    executeQueryWithParameter(query, tableName);
  }
}
