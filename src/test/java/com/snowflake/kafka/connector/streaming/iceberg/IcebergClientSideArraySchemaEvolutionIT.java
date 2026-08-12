package com.snowflake.kafka.connector.streaming.iceberg;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.snowflake.kafka.connector.ConnectorConfigTools;
import com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams;
import com.snowflake.kafka.connector.StaticTopicToTableResolver;
import com.snowflake.kafka.connector.config.SinkTaskConfig;
import com.snowflake.kafka.connector.dlq.InMemoryKafkaRecordErrorReporter;
import com.snowflake.kafka.connector.internal.SnowflakeConnectionService;
import com.snowflake.kafka.connector.internal.SnowflakeErrors;
import com.snowflake.kafka.connector.internal.SnowflakeKafkaConnectorException;
import com.snowflake.kafka.connector.internal.SnowflakeSinkService;
import com.snowflake.kafka.connector.internal.TestUtils;
import com.snowflake.kafka.connector.internal.schemaevolution.ColumnInfos;
import com.snowflake.kafka.connector.internal.schemaevolution.SnowflakeColumnTypeMapper;
import com.snowflake.kafka.connector.internal.streaming.InMemorySinkTaskContext;
import com.snowflake.kafka.connector.internal.streaming.StreamingSinkServiceBuilder;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Iceberg client-side SE experiments for Connect {@code ARRAY} fields (Worldpay / SNOW-3901852).
 *
 * <p>Default mapper emits bare {@code ARRAY} (rejected by Iceberg with 091386). Override with
 * env {@code SNOWFLAKE_EXPERIMENTAL_ARRAY_COLUMN_TYPE=VARIANT} or {@code ARRAY(VARIANT)}.
 *
 * <p>Config shape: pre-created Iceberg + {@code snowflake.validation=client_side} + {@code
 * autocreate.table.type=snowflake}.
 */
public class IcebergClientSideArraySchemaEvolutionIT {

  private static final int PARTITION = 0;
  private static final String ARRAY_COLUMN = "META_RETRYHDR_REASONS";
  private static final String ARRAY_DDL_ENV = "SNOWFLAKE_EXPERIMENTAL_ARRAY_COLUMN_TYPE";

  private SnowflakeConnectionService conn;
  private String tableName;
  private String topic;
  private TopicPartition topicPartition;
  private SnowflakeSinkService service;

  private static String arrayDdlType() {
    return Optional.ofNullable(System.getenv(ARRAY_DDL_ENV))
        .filter(s -> !s.isBlank())
        .map(String::trim)
        .orElse("ARRAY");
  }

  @BeforeEach
  public void setUp() {
    conn = TestUtils.getConnectionService();
    tableName = TestUtils.randomTableName();
    topic = tableName;
    topicPartition = new TopicPartition(topic, PARTITION);

    createIcebergTableWithCityOnly();

    Map<String, String> config = TestUtils.getConnectorConfigurationForStreaming(false);
    ConnectorConfigTools.setDefaultValues(config);
    config.put(KafkaConnectorConfigParams.SNOWFLAKE_AUTOCREATE_TABLE_TYPE, "snowflake");
    config.put(KafkaConnectorConfigParams.SNOWFLAKE_VALIDATION, "client_side");
    config.put(KafkaConnectorConfigParams.SNOWFLAKE_ENABLE_SCHEMATIZATION, "true");
    config.put(
        KafkaConnectorConfigParams.SNOWFLAKE_COMPATIBILITY_ENABLE_COLUMN_IDENTIFIER_NORMALIZATION,
        "true");

    SinkTaskConfig sinkTaskConfig =
        SinkTaskConfig.builderFrom(config)
            .tolerateErrors(false)
            .dlqTopicName("test_DLQ")
            .topicToTableResolver(
                new StaticTopicToTableResolver(Collections.singletonMap(topic, tableName)))
            .build();

    service =
        StreamingSinkServiceBuilder.builder(conn, sinkTaskConfig)
            .withErrorReporter(new InMemoryKafkaRecordErrorReporter())
            .withSinkTaskContext(new InMemorySinkTaskContext(Collections.singleton(topicPartition)))
            .build();
    service.startPartition(topicPartition);
    service.awaitInitialization();
  }

  @AfterEach
  public void tearDown() {
    if (service != null) {
      service.closeAll();
    }
    if (conn != null) {
      conn.executeQueryWithParameters("drop iceberg table if exists identifier(?)", tableName);
      conn.close();
    }
  }

  @Test
  void appendColumnsToTable_usesConfiguredArrayDdlType() {
    String ddlType = new SnowflakeColumnTypeMapper().mapToColumnType(Schema.Type.ARRAY, null);
    assertThat(ddlType).isEqualTo(arrayDdlType());

    Map<String, ColumnInfos> cols = new HashMap<>();
    cols.put(ARRAY_COLUMN, new ColumnInfos(ddlType));

    if ("ARRAY".equals(ddlType)) {
      assertThatThrownBy(() -> conn.appendColumnsToTable(tableName, cols))
          .isInstanceOf(SnowflakeKafkaConnectorException.class)
          .satisfies(
              ex -> {
                SnowflakeKafkaConnectorException skce = (SnowflakeKafkaConnectorException) ex;
                assertThat(skce.getCode()).isEqualTo(SnowflakeErrors.ERROR_2015.getCode());
                assertThat(skce.getMessage())
                    .containsIgnoringCase("Unsupported data type 'ARRAY' for iceberg tables");
              });
    } else {
      conn.appendColumnsToTable(tableName, cols);
      assertThat(columnType(ARRAY_COLUMN)).containsIgnoringCase(expectedTypeSubstring(ddlType));
    }
  }

  @Test
  void insertWithArrayField_clientSideSe_endToEnd() throws Exception {
    String ddlType = arrayDdlType();
    String json =
        "{\""
            + ARRAY_COLUMN.toLowerCase()
            + "\":[\"reason1\",\"reason2\"],\"city\":\"Hsinchu\"}";

    if ("ARRAY".equals(ddlType)) {
      assertThatThrownBy(
              () ->
                  service.insert(
                      Collections.singletonList(createJsonRecord(json, /* offset= */ 0))))
          .isInstanceOf(DataException.class);
      assertThat(TestUtils.getNumberOfRows(tableName)).isEqualTo(0);
      assertThat(columnNames()).doesNotContain(ARRAY_COLUMN);
      return;
    }

    service.insert(Collections.singletonList(createJsonRecord(json, /* offset= */ 0)));
    TestUtils.assertWithRetry(() -> service.getOffset(topicPartition) == 1);

    assertThat(columnNames()).contains(ARRAY_COLUMN);
    assertThat(columnType(ARRAY_COLUMN)).containsIgnoringCase(expectedTypeSubstring(ddlType));

    Map<String, Object> row = queryCityAndArrayColumn();
    assertThat(row.get("CITY").toString()).isEqualToIgnoringCase("Hsinchu");
    String arrayValue = String.valueOf(row.get(ARRAY_COLUMN));
    assertThat(arrayValue).contains("reason1").contains("reason2");
  }

  private static String expectedTypeSubstring(String ddlType) {
    // DESC TABLE may expand VARIANT / ARRAY(VARIANT) with precision; match the stem.
    if (ddlType.toUpperCase().startsWith("ARRAY(")) {
      return "ARRAY";
    }
    return ddlType;
  }

  @Test
  void insertWithMapAndArrayColumnNames_clientSideSe() throws Exception {
    // Customer report: columns named map/array rejected. With VARIANT mapping (Iceberg-safe),
    // verify whether names themselves are the problem under client-side SE.
    assumeTrue(
        !"ARRAY".equals(arrayDdlType()),
        "Skip when ARRAY DDL is bare ARRAY (Iceberg type rejection masks name check)");

    String json = "{\"city\":\"Hsinchu\",\"map\":{\"k\":1},\"array\":[\"a\",\"b\"]}";
    service.insert(Collections.singletonList(createJsonRecord(json, /* offset= */ 0)));
    TestUtils.assertWithRetry(() -> service.getOffset(topicPartition) == 1);

    assertThat(columnNames()).contains("MAP", "ARRAY", "CITY");
    Map<String, Object> row =
        TestUtils.executeQueryAndCollectResult(
            conn.getConnection(),
            "select CITY, MAP, ARRAY from identifier(?)",
            tableName,
            (ResultSet rs) -> {
              try {
                assertThat(rs.next()).isTrue();
                Map<String, Object> out = new HashMap<>();
                out.put("CITY", rs.getObject(1));
                out.put("MAP", rs.getObject(2));
                out.put("ARRAY", rs.getObject(3));
                return out;
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            });
    assertThat(String.valueOf(row.get("CITY"))).isEqualToIgnoringCase("Hsinchu");
    assertThat(String.valueOf(row.get("MAP"))).contains("k");
    assertThat(String.valueOf(row.get("ARRAY"))).contains("a");
  }

  private void createIcebergTableWithCityOnly() {
    String volume =
        Optional.ofNullable(System.getenv("ICEBERG_EXTERNAL_VOLUME"))
            .filter(s -> !s.isBlank())
            .orElse("STREAMING_ICEBERG_BENCHMARK_VOLUME");
    String ddl =
        "create or replace iceberg table identifier(?) ("
            + "RECORD_METADATA VARIANT, CITY TEXT"
            + ") catalog = 'SNOWFLAKE' external_volume = '"
            + volume
            + "' iceberg_version = 3 enable_schema_evolution = true";
    conn.executeQueryWithParameters(ddl, tableName);
    assertThat(conn.isIcebergTable(tableName)).isTrue();
  }

  private SinkRecord createJsonRecord(String json, long offset) {
    JsonConverter converter = new JsonConverter();
    converter.configure(Collections.singletonMap("schemas.enable", "false"), false);
    SchemaAndValue input =
        converter.toConnectData(topic, json.getBytes(StandardCharsets.UTF_8));
    return new SinkRecord(
        topic,
        PARTITION,
        Schema.STRING_SCHEMA,
        "key",
        input.schema(),
        input.value(),
        offset,
        System.currentTimeMillis(),
        TimestampType.CREATE_TIME);
  }

  private java.util.Set<String> columnNames() {
    return conn.describeTable(tableName).orElseThrow().stream()
        .map(row -> row.getColumn().toUpperCase())
        .collect(java.util.stream.Collectors.toSet());
  }

  private String columnType(String column) {
    return conn.describeTable(tableName).orElseThrow().stream()
        .filter(row -> row.getColumn().equalsIgnoreCase(column))
        .findFirst()
        .orElseThrow(() -> new AssertionError("column not found: " + column))
        .getType();
  }

  private Map<String, Object> queryCityAndArrayColumn() {
    return TestUtils.executeQueryAndCollectResult(
        conn.getConnection(),
        "select CITY, " + ARRAY_COLUMN + " from identifier(?)",
        tableName,
        (ResultSet rs) -> {
          try {
            assertThat(rs.next()).isTrue();
            Map<String, Object> out = new HashMap<>();
            out.put("CITY", rs.getObject(1));
            out.put(ARRAY_COLUMN, rs.getObject(2));
            return out;
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        });
  }
}
