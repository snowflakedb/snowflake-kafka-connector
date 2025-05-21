package com.snowflake.kafka.connector.internal.streaming.validation;

import static org.assertj.core.api.Assertions.assertThat;

import com.snowflake.kafka.connector.internal.SnowflakeConnectionService;
import com.snowflake.kafka.connector.internal.TestUtils;
import com.snowflake.kafka.connector.internal.streaming.common.ColumnProperties;
import java.util.Map;
import net.snowflake.ingest.connection.JWTManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class RowSchemaProviderIT {
  private static final Map<String, String> config = TestUtils.getConfForStreaming();
  private static final SnowflakeConnectionService conn =
      TestUtils.getConnectionServiceForStreaming();
  private static final JWTManager jwtManager = JWTManagerProvider.fromConfig(config);

  private static final String NUMBER_COL = "NUMBER_COL";
  private static final String DECIMAL_COL = "DECIMAL_COL";
  private static final String INT_COL = "INT_COL";
  private static final String FLOAT_COL = "FLOAT_COL";
  private static final String DOUBLE_COL = "DOUBLE_COL";
  private static final String VARCHAR_COL = "VARCHAR_COL";
  private static final String BOOLEAN_COL = "BOOLEAN_COL";
  private static final String DATE_COL = "DATE_COL";
  private static final String TIMESTAMP_COL = "TIMESTAMP_COL";
  private static final String VARIANT_COL = "VARIANT_COL";
  private static final String OBJECT_COL = "OBJECT_COL";
  private static final String ARRAY_COL = "ARRAY_COL";

  private String tableName;

  @BeforeEach
  void setUp() {
    tableName = TestUtils.randomTableName();
  }

  @AfterEach
  void afterEach() {
    TestUtils.dropTable(tableName);
  }

  @Test
  void shouldGetSchemaForSnowflakeTable() {
    // given
    RowSchemaProvider provider =
        new FailsafeRowSchemaProvider(new RowsetApiRowSchemaProvider(jwtManager));
    // The best formatting I could get...
    String query =
        String.format(
            "create table identifier(?) ("
                + "%s number(16,1) not null,"
                + "%s decimal not null,"
                + "%s int not null,"
                + "%s float,"
                + "%s double,"
                + "%s varchar(2137),"
                + "%s boolean,"
                + "%s date,"
                + "%s timestamp,"
                + "%s variant,"
                + "%s object,"
                + "%s array"
                + ")",
            NUMBER_COL,
            DECIMAL_COL,
            INT_COL,
            FLOAT_COL,
            DOUBLE_COL,
            VARCHAR_COL,
            BOOLEAN_COL,
            DATE_COL,
            TIMESTAMP_COL,
            VARIANT_COL,
            OBJECT_COL,
            ARRAY_COL);
    TestUtils.executeQueryWithParameter(conn.getConnection(), query, tableName);

    // when
    RowSchema result = provider.getRowSchema(tableName, config);

    // then
    assertThat(result.getNonNullableFieldNames())
        .containsExactlyInAnyOrder(NUMBER_COL, DECIMAL_COL, INT_COL);
    assertThat(result.getColumnProperties())
        .hasSize(12)
        .containsEntry(
            NUMBER_COL, new ColumnProperties("FIXED", "FIXED", 16, 1, null, null, false, null))
        .containsEntry(
            VARIANT_COL,
            new ColumnProperties("VARIANT", "VARIANT", null, null, null, null, true, null))
        .containsEntry(
            FLOAT_COL, new ColumnProperties("REAL", "REAL", null, null, null, null, true, null))
        .containsEntry(
            VARCHAR_COL,
            new ColumnProperties("TEXT", "TEXT", null, null, 8548L, 2137L, true, null));
  }
}
