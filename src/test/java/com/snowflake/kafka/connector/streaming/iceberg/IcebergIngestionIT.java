package com.snowflake.kafka.connector.streaming.iceberg;

import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.ENABLE_SCHEMATIZATION_CONFIG;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.ICEBERG_ENABLED;
import static com.snowflake.kafka.connector.internal.TestUtils.getConfForStreaming;
import static org.assertj.core.api.Assertions.assertThat;

import com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig;
import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.dlq.InMemoryKafkaRecordErrorReporter;
import com.snowflake.kafka.connector.internal.DescribeTableRow;
import com.snowflake.kafka.connector.internal.SnowflakeSinkService;
import com.snowflake.kafka.connector.internal.SnowflakeSinkServiceFactory;
import com.snowflake.kafka.connector.internal.TestUtils;
import com.snowflake.kafka.connector.internal.streaming.InMemorySinkTaskContext;
import com.snowflake.kafka.connector.internal.streaming.IngestionMethodConfig;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class IcebergIngestionIT extends BaseIcebergIT {

  private String tableName;
  private static final int PARTITION = 0;
  private String topic;
  private TopicPartition topicPartition;

  private SnowflakeSinkService service;

  private final String simpleRecordJson = "{\"simple\": \"extra field\"}";

  private static final String primitiveJson =
      "{\n"
          + "  \"id_int8\": 0,\n"
          + "  \"id_int16\": 42,\n"
          + "  \"id_int32\": 42,\n"
          + "  \"id_int64\": 42,\n"
          + "  \"description\": \"dogs are the best\",\n"
          + "  \"rating_float32\": 0.99,\n"
          + "  \"rating_float64\": 0.99,\n"
          + "  \"approval\": true\n"
          + "}";

  private static final String primitiveJsonWithSchema =
      "{\n"
          + "  \"schema\": {\n"
          + "    \"type\": \"struct\",\n"
          + "    \"fields\": [\n"
          + "      {\n"
          + "        \"field\": \"id_int8\",\n"
          + "        \"type\": \"int8\"\n"
          + "      },\n"
          + "      {\n"
          + "        \"field\": \"id_int16\",\n"
          + "        \"type\": \"int16\"\n"
          + "      },\n"
          + "      {\n"
          + "        \"field\": \"id_int32\",\n"
          + "        \"type\": \"int32\"\n"
          + "      },\n"
          + "      {\n"
          + "        \"field\": \"id_int64\",\n"
          + "        \"type\": \"int64\"\n"
          + "      },\n"
          + "      {\n"
          + "        \"field\": \"description\",\n"
          + "        \"type\": \"string\"\n"
          + "      },\n"
          + "      {\n"
          + "        \"field\": \"rating_float32\",\n"
          + "        \"type\": \"float\"\n"
          + "      },\n"
          + "      {\n"
          + "        \"field\": \"rating_float64\",\n"
          + "        \"type\": \"double\"\n"
          + "      },\n"
          + "      {\n"
          + "        \"field\": \"approval\",\n"
          + "        \"type\": \"boolean\"\n"
          + "      }\n"
          + "    ],\n"
          + "    \"optional\": false,\n"
          + "    \"name\": \"sf.kc.test\"\n"
          + "  },\n"
          + "  \"payload\": "
          + primitiveJson
          + "}";

  @BeforeEach
  public void setUp() {
    tableName = TestUtils.randomTableName();
    topic = tableName;
    topicPartition = new TopicPartition(topic, PARTITION);
    Map<String, String> config = getConfForStreaming();
    SnowflakeSinkConnectorConfig.setDefaultValues(config);
    config.put(ICEBERG_ENABLED, "TRUE");
    config.put(ENABLE_SCHEMATIZATION_CONFIG, "TRUE");

    createIcebergTable(tableName);
    enableSchemaEvolution(tableName);

    // only insert fist topic to topicTable
    Map<String, String> topic2Table = new HashMap<>();
    topic2Table.put(topic, tableName);

    service =
        SnowflakeSinkServiceFactory.builder(conn, IngestionMethodConfig.SNOWPIPE_STREAMING, config)
            .setRecordNumber(1)
            .setErrorReporter(new InMemoryKafkaRecordErrorReporter())
            .setSinkTaskContext(new InMemorySinkTaskContext(Collections.singleton(topicPartition)))
            .setTopic2TableMap(topic2Table)
            .addTask(tableName, topicPartition)
            .build();
  }

  @AfterEach
  public void tearDown() {
    if (service != null) {
      service.closeAll();
    }
    dropIcebergTable(tableName);
  }

  @ParameterizedTest()
  @MethodSource("prepareData")
  @Disabled
  void shouldEvolveSchemaAndInsertRecords(
      String description, String message, DescribeTableRow[] expectedSchema, boolean withSchema)
      throws Exception {
    // start off with just one column
    List<DescribeTableRow> rows = describeTable(tableName);
    assertThat(rows.size()).isEqualTo(1);
    assertThat(rows.get(0).getColumn()).isEqualTo(Utils.TABLE_COLUMN_METADATA);

    SinkRecord record = createKafkaRecord(message, 0, withSchema);
    service.insert(Collections.singletonList(record));
    TestUtils.assertWithRetry(() -> service.getOffset(topicPartition) == -1, 20, 5);
    rows = describeTable(tableName);
    assertThat(rows.size()).isEqualTo(9);

    // don't check metadata column schema, we have different tests for that
    rows =
        rows.stream()
            .filter(r -> !r.getColumn().equals(Utils.TABLE_COLUMN_METADATA))
            .collect(Collectors.toList());

    assertThat(rows).containsExactlyInAnyOrder(expectedSchema);

    // resend and store same record without any issues now
    service.insert(Collections.singletonList(record));
    TestUtils.assertWithRetry(() -> service.getOffset(topicPartition) == 1, 20, 5);

    // and another record with same schema
    service.insert(Collections.singletonList(createKafkaRecord(message, 1, withSchema)));
    TestUtils.assertWithRetry(() -> service.getOffset(topicPartition) == 2, 20, 5);

    // and another record with extra field - schema evolves again
    service.insert(Collections.singletonList(createKafkaRecord(simpleRecordJson, 2, false)));

    rows = describeTable(tableName);
    assertThat(rows.size()).isEqualTo(10);
    assertThat(rows).contains(new DescribeTableRow("SIMPLE", "VARCHAR(16777216)"));

    // reinsert record with extra field
    service.insert(Collections.singletonList(createKafkaRecord(simpleRecordJson, 2, false)));
    TestUtils.assertWithRetry(() -> service.getOffset(topicPartition) == 3, 20, 5);
  }

  private static Stream<Arguments> prepareData() {
    return Stream.of(
        Arguments.of(
            "Primitive JSON with schema",
            primitiveJsonWithSchema,
            new DescribeTableRow[] {
              new DescribeTableRow("ID_INT8", "NUMBER(10,0)"),
              new DescribeTableRow("ID_INT16", "NUMBER(10,0)"),
              new DescribeTableRow("ID_INT32", "NUMBER(10,0)"),
              new DescribeTableRow("ID_INT64", "NUMBER(19,0)"),
              new DescribeTableRow("DESCRIPTION", "VARCHAR(16777216)"),
              new DescribeTableRow("RATING_FLOAT32", "FLOAT"),
              new DescribeTableRow("RATING_FLOAT64", "FLOAT"),
              new DescribeTableRow("APPROVAL", "BOOLEAN")
            },
            true),
        Arguments.of(
            "Primitive JSON without schema",
            primitiveJson,
            new DescribeTableRow[] {
              new DescribeTableRow("ID_INT8", "NUMBER(19,0)"),
              new DescribeTableRow("ID_INT16", "NUMBER(19,0)"),
              new DescribeTableRow("ID_INT32", "NUMBER(19,0)"),
              new DescribeTableRow("ID_INT64", "NUMBER(19,0)"),
              new DescribeTableRow("DESCRIPTION", "VARCHAR(16777216)"),
              new DescribeTableRow("RATING_FLOAT32", "FLOAT"),
              new DescribeTableRow("RATING_FLOAT64", "FLOAT"),
              new DescribeTableRow("APPROVAL", "BOOLEAN")
            },
            false));
  }

  private SinkRecord createKafkaRecord(String jsonString, int offset, boolean withSchema) {
    JsonConverter converter = new JsonConverter();
    converter.configure(
        Collections.singletonMap("schemas.enable", Boolean.toString(withSchema)), false);
    SchemaAndValue inputValue =
        converter.toConnectData(topic, jsonString.getBytes(StandardCharsets.UTF_8));
    return new SinkRecord(
        topic,
        PARTITION,
        Schema.STRING_SCHEMA,
        "test",
        inputValue.schema(),
        inputValue.value(),
        offset);
  }
}
