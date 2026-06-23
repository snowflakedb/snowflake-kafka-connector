package com.snowflake.kafka.connector.streaming.iceberg;

import static org.assertj.core.api.Assertions.assertThat;

import com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams;
import com.snowflake.kafka.connector.internal.TestUtils;
import java.util.Collections;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Verifies that with {@code snowflake.autocreate.table.type=iceberg} and NO pre-created table, the
 * auto-creates a managed Iceberg table and ingestion lands. Unlike the other Iceberg ITs, this one
 * does not override {@link #createIcebergTable()}, so the table is absent at startup and
 * SnowflakeSinkServiceV2#createTableIfNotExists must create it via
 * createIcebergTableWithOnlyMetadataColumn.
 */
public class IcebergAutoCreateIT extends IcebergIngestionIT {

  // Intentionally do NOT override createIcebergTable() -> table is absent; connector creates it.

  @Override
  protected void customizeIcebergConfig(Map<String, String> config) {
    // Pin the external volume BaseIcebergIT uses (via the create-options passthrough) so
    // auto-create
    // does not depend on an account default external volume being configured in the test env.
    config.put(
        KafkaConnectorConfigParams.SNOWFLAKE_ICEBERG_CREATE_TABLE_OPTIONS,
        "EXTERNAL_VOLUME='test_exvol'");
  }

  @Test
  void autoCreatesIcebergTable_whenTableTypeIsIceberg() throws Exception {
    // The table is auto-created during setUp's startPartition; ingest one record and assert.
    service.insert(Collections.singletonList(createKafkaRecord("{\"id\": 1}", 0, false)));
    waitForOffset(1);

    assertThat(snowflakeDatabase.tableExist(tableName)).isTrue();
    assertThat(snowflakeDatabase.isIcebergTable(tableName)).isTrue();
    assertThat(TestUtils.getNumberOfRows(tableName)).isEqualTo(1);
  }
}
