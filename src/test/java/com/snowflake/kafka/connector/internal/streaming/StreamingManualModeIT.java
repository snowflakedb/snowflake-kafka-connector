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
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.snowflake.kafka.connector.InjectQueryRunner;
import com.snowflake.kafka.connector.InjectQueryRunnerExtension;
import com.snowflake.kafka.connector.InjectSnowflakeDataSourceExtension;
import com.snowflake.kafka.connector.internal.SnowflakeConnectionService;
import com.snowflake.kafka.connector.internal.SnowflakeSinkService;
import com.snowflake.kafka.connector.internal.TestUtils;
import com.snowflake.kafka.connector.internal.streaming.v2.PipeNameProvider;
import java.io.IOException;
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
class StreamingManualModeIT {

  private final ObjectMapper objectMapper = new ObjectMapper();
  private final SnowflakeConnectionService conn = TestUtils.getConnectionServiceWithEncryptedKey();

  private String tableName;
  private String topicName;
  private TopicPartition topicPartition;
  private SnowflakeSinkService snowflakeSinkService;

  @InjectQueryRunner private QueryRunner queryRunner;

  @BeforeEach
  void beforeEach() throws SQLException {
    final Map<String, String> config = TestUtils.getConnectorConfigurationForStreaming(true);
    tableName = TestUtils.randomTableName();
    topicName = tableName;
    topicPartition = new TopicPartition(topicName, 0);
    snowflakeSinkService =
        StreamingSinkServiceBuilder.builder(conn, config)
            .withSinkTaskContext(new InMemorySinkTaskContext(Collections.singleton(topicPartition)))
            .build();
    queryRunner.execute(
        format(
            "create table %s (city varchar, age number, married boolean, has_cat boolean,"
                + " crazy_field_name boolean, skills variant, family variant)",
            tableName));
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
              "CREATE OR REPLACE PIPE %s AS COPY INTO %s FROM (SELECT $1:city, $1:age,  $1:married,"
                  + " $1['has cat'] has_cat, $1['! @&$#* has Łułósżź'] crazy_field_name, $1:skills,"
                  + " $1:family FROM TABLE(DATA_SOURCE(TYPE => 'STREAMING')))",
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

      List<Map<String, Object>> dbRows = getTableRows(tableName);

      final Map<String, Object> firstRow = dbRows.get(0);
      makeCommonAssertions(firstRow);
      assertEquals(true, firstRow.get("HAS_CAT"));
      assertEquals(true, firstRow.get("CRAZY_FIELD_NAME"));
    }
  }

  @Nested
  class DefaultPipe {

    @BeforeEach
    void beforeEach() throws SQLException {
      queryRunner.execute(
          format(
              "create or replace table %s (record_metadata variant, city varchar, age number,"
                  + " married boolean, \"has cat\" boolean , \"! @&$#* has Łułósżź\" boolean,"
                  + " skills variant, family variant)",
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
      assertTableColumnCount(tableName, 8);
      Map<String, Object> firstRow = getTableRows(tableName).get(0);
      assertTableHasColumn(tableName, "record_metadata");
      makeCommonAssertions(firstRow);
      assertEquals(true, firstRow.get("! @&$#* has Łułósżź"));
      assertEquals(true, firstRow.get("has cat"));
    }
  }

  private List<SinkRecord> buildContentSinkRecords() throws JsonProcessingException {
    // this json row is sent twice to Kafka
    final byte[] jsonPayload =
        objectMapper
            .writeValueAsString(
                Map.of(
                    "city",
                    "Pcim Górny",
                    "age",
                    30,
                    "married",
                    true,
                    "has cat",
                    true,
                    "! @&$#* has Łułósżź",
                    true,
                    "skills",
                    List.of("sitting", "standing", "eating"),
                    "family",
                    Map.of("son", "Jack", "daughter", "Anna")))
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

  private JsonNode toJson(Object value) throws IOException {
    if (value instanceof String) {
      return objectMapper.readTree((String) value);
    }
    if (value instanceof byte[]) {
      return objectMapper.readTree((byte[]) value);
    }

    return objectMapper.valueToTree(value);
  }

  private void makeCommonAssertions(final Map<String, Object> firstRow) throws IOException {
    assertEquals("Pcim Górny", firstRow.get("CITY"));
    assertEquals(30L, firstRow.get("AGE"));
    assertEquals(true, firstRow.get("MARRIED"));
    assertEquals(toJson(List.of("sitting", "standing", "eating")), toJson(firstRow.get("SKILLS")));
    assertEquals(toJson(Map.of("son", "Jack", "daughter", "Anna")), toJson(firstRow.get("FAMILY")));
  }
}
