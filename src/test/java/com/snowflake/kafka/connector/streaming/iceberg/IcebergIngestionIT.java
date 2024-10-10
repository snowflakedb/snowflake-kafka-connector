package com.snowflake.kafka.connector.streaming.iceberg;

import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.ENABLE_SCHEMATIZATION_CONFIG;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.ICEBERG_ENABLED;
import static com.snowflake.kafka.connector.internal.TestUtils.getConfForStreaming;

import com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig;
import com.snowflake.kafka.connector.dlq.InMemoryKafkaRecordErrorReporter;
import com.snowflake.kafka.connector.internal.SnowflakeSinkService;
import com.snowflake.kafka.connector.internal.SnowflakeSinkServiceFactory;
import com.snowflake.kafka.connector.internal.TestUtils;
import com.snowflake.kafka.connector.internal.streaming.InMemorySinkTaskContext;
import com.snowflake.kafka.connector.internal.streaming.IngestionMethodConfig;
import com.snowflake.kafka.connector.internal.streaming.SnowflakeSinkServiceV2;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class IcebergIngestionIT extends BaseIcebergIT {

  private static final String ICEBERG_ENABLED_PRIVATE_FIELD_NAME = "icebergEnabled";
  private String tableName;
  private static final int PARTITION = 0;
  private String topic;
  private TopicPartition topicPartition;
  private String testChannelName;

  private SnowflakeSinkService service;

  private final String primitiveJson =
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

  private final String primitiveJsonWithSchema =
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
    testChannelName = SnowflakeSinkServiceV2.partitionChannelKey(topic, PARTITION);
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

  @Test
  @Disabled
  void shouldInsertJsonRecordWithoutSchema() throws Exception {
    SinkRecord record = createKafkaRecord(primitiveJson, 0, false);
    service.insert(Collections.singletonList(record));
    service.insert(Collections.singletonList(record));
    TestUtils.assertWithRetry(
        () -> service.getOffset(new TopicPartition(topic, PARTITION)) == 0 + 1, 20, 5);
  }

  @Test
  void shouldInsertJsonRecordWithSchema() throws Exception {
    SinkRecord record = createKafkaRecord(primitiveJsonWithSchema, 0, true);
    service.insert(Collections.singletonList(record));
    TestUtils.assertWithRetry(
        () -> service.getOffset(new TopicPartition(topic, PARTITION)) == -1, 20, 5);
    service.insert(Collections.singletonList(record));
    TestUtils.assertWithRetry(
        () -> service.getOffset(new TopicPartition(topic, PARTITION)) == 1, 20, 5);
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
