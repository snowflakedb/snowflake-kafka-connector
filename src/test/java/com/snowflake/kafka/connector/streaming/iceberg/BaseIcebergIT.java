package com.snowflake.kafka.connector.streaming.iceberg;

import static com.snowflake.kafka.connector.internal.TestUtils.executeQueryAndCollectResult;
import static com.snowflake.kafka.connector.internal.TestUtils.executeQueryWithParameter;

import com.snowflake.kafka.connector.internal.SnowflakeConnectionService;
import com.snowflake.kafka.connector.internal.TestUtils;
import java.sql.ResultSet;
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

  protected static <T> T select(
      String tableName, String query, Function<ResultSet, T> resultCollector) {
    return executeQueryAndCollectResult(
        snowflakeDatabase.getConnection(), query, tableName, resultCollector);
  }
}
