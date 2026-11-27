package com.snowflake.kafka.connector.streaming.iceberg;

import static org.assertj.core.api.Assertions.assertThat;

import com.snowflake.kafka.connector.internal.TestUtils;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * The connector appends its own table properties after the user's create options. A user option
 * that repeats one of them must not change the created table: Snowflake either rejects the
 * statement or keeps the connector's value.
 */
public class IcebergCreateOptionsPrecedenceIT extends BaseIcebergIT {

  private final String tableName = TestUtils.randomTableName();

  @AfterEach
  void dropTable() {
    dropIcebergTable(tableName);
  }

  @Test
  void userOptionCannotTurnOffConnectorSchemaEvolution() throws SQLException {
    RuntimeException rejected = null;
    try {
      snowflakeDatabase.createIcebergTableWithOnlyMetadataColumn(
          tableName, "EXTERNAL_VOLUME='test_exvol' ENABLE_SCHEMA_EVOLUTION = FALSE");
    } catch (RuntimeException e) {
      rejected = e;
    }

    if (rejected != null) {
      assertThat(rejected.getMessage()).containsIgnoringCase("ENABLE_SCHEMA_EVOLUTION");
      assertThat(snowflakeDatabase.tableExist(tableName)).isFalse();
      return;
    }
    try (ResultSet rs =
        snowflakeDatabase
            .getConnection()
            .createStatement()
            .executeQuery("show tables like '" + tableName + "'")) {
      assertThat(rs.next()).isTrue();
      assertThat(rs.getString("enable_schema_evolution")).isEqualToIgnoringCase("Y");
    }
  }
}
