package com.snowflake.kafka.connector.internal.streaming;

import static com.snowflake.kafka.connector.internal.TestUtils.assertTableColumnCount;
import static com.snowflake.kafka.connector.internal.TestUtils.assertTableHasColumn;
import static com.snowflake.kafka.connector.internal.TestUtils.assertTableRowCount;
import static com.snowflake.kafka.connector.internal.TestUtils.assertWithRetry;
import static com.snowflake.kafka.connector.internal.TestUtils.getTableRows;
import static com.snowflake.kafka.connector.internal.TestUtils.tableSize;
import static java.lang.String.format;
import static org.apache.kafka.connect.data.Schema.STRING_SCHEMA;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.snowflake.kafka.connector.InjectQueryRunner;
import com.snowflake.kafka.connector.InjectQueryRunnerExtension;
import com.snowflake.kafka.connector.InjectSnowflakeDataSourceExtension;
import com.snowflake.kafka.connector.internal.SnowflakeConnectionService;
import com.snowflake.kafka.connector.internal.SnowflakeSinkService;
import com.snowflake.kafka.connector.internal.TestUtils;
import com.snowflake.kafka.connector.internal.streaming.v2.PipeNameProvider;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.storage.Converter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith({InjectSnowflakeDataSourceExtension.class, InjectQueryRunnerExtension.class})
// Manual mode meaning user creates his own pipe and table objects
class ManualModeIT {

  private final ObjectMapper objectMapper = new ObjectMapper();
  private final SnowflakeConnectionService conn = TestUtils.getConnectionServiceForStreaming();

  private String tableName;
  private String topicName;
  private TopicPartition topicPartition;
  private SnowflakeSinkService snowflakeSinkService;

  @InjectQueryRunner private QueryRunner queryRunner;

  @BeforeEach
  void beforeEach() throws SQLException {
    final Map<String, String> config = TestUtils.getConfForStreaming();
    tableName = TestUtils.randomTableName();
    topicName = tableName;
    topicPartition = new TopicPartition(topicName, 0);
    snowflakeSinkService =
        StreamingSinkServiceBuilder.builder(conn, config)
            .withSinkTaskContext(new InMemorySinkTaskContext(Collections.singleton(topicPartition)))
            .build();
    queryRunner.execute(
        format("create table %s (city varchar, age number, married boolean)", tableName));
  }

  @AfterEach
  void afterEach() {
    TestUtils.dropTable(tableName);
  }

  @Nested
  class TableAndPipeDefinedByUser {

    private String pipeName;

    @BeforeEach
    void beforeEach() throws SQLException {
      pipeName = PipeNameProvider.buildPipeName(tableName);
      queryRunner.execute(
          format(
              "CREATE OR REPLACE PIPE %s AS COPY INTO %s FROM (SELECT $1:RECORD_CONTENT.city,"
                  + " $1:RECORD_CONTENT.age,  $1:RECORD_CONTENT.married FROM TABLE(DATA_SOURCE(TYPE"
                  + " => 'STREAMING')))",
              pipeName, tableName));
    }

    @AfterEach
    void afterEach() throws SQLException {
      TestUtils.dropPipe(pipeName);
    }

    @Test
    void test_streaming_ingestion_with_user_defined_table_and_pipe() throws Exception {

      List<SinkRecord> records = buildContentSinkRecords();
      snowflakeSinkService.startPartition(topicPartition);
      snowflakeSinkService.insert(records);

      // Wait for data to be ingested into the table
      assertWithRetry(() -> tableSize(tableName) == 2);
      snowflakeSinkService.closeAll();

      // Assert that the table has exactly 2 rows with the given values
      assertTableRowCount(tableName, 2);

      List<Map<String, Object>> rows = getTableRows(tableName);
      Map<String, Object> expectedRow0Values = new HashMap<>();
      expectedRow0Values.put("CITY", "Pcim Górny");
      expectedRow0Values.put("AGE", 30L);
      expectedRow0Values.put("MARRIED", false);
      assertEquals(expectedRow0Values, rows.get(0));
    }
  }

  @Nested
  class DefaultPipe {

    @BeforeEach
    void beforeEach() throws SQLException {
      queryRunner.execute(
          format(
              "create or replace table %s (record_metadata variant, record_content variant)",
              tableName));
    }

    @Test
    void test_streaming_ingestion_with_user_defined_table_and_default_pipe() throws Exception {
      List<SinkRecord> records = buildContentSinkRecords();
      snowflakeSinkService.startPartition(topicPartition);
      snowflakeSinkService.insert(records);

      // Wait for data to be ingested into the table
      assertWithRetry(() -> tableSize(tableName) == 2);
      snowflakeSinkService.closeAll();

      // Assert that the table has exactly 2 rows and 2 columns
      assertTableRowCount(tableName, 2);
      assertTableColumnCount(tableName, 2);
      assertTableHasColumn(tableName, "record_metadata");
      assertTableHasColumn(tableName, "record_content");
    }
  }

  private List<SinkRecord> buildContentSinkRecords() throws JsonProcessingException {
    // this json row is sent twice to Kafka
    final byte[] jsonPayload =
        objectMapper
            .writeValueAsString(Map.of("city", "Pcim Górny", "age", 30, "married", false))
            .getBytes(StandardCharsets.UTF_8);
    Converter converter = new JsonConverter();
    final Map<String, String> converterConfig = new HashMap<>();
    converterConfig.put("schemas.enable", "false");
    converter.configure(converterConfig, false);
    SchemaAndValue input = converter.toConnectData(topicName, jsonPayload);
    return List.of(
        new SinkRecord(topicName, 0, STRING_SCHEMA, "test_key1", input.schema(), input.value(), 1),
        new SinkRecord(topicName, 0, STRING_SCHEMA, "test_key2", input.schema(), input.value(), 2));
  }
}
