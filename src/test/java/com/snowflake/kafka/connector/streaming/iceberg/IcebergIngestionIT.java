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
import com.snowflake.kafka.connector.streaming.iceberg.sql.MetadataRecord.RecordWithMetadata;
import com.snowflake.kafka.connector.streaming.iceberg.sql.PrimitiveJsonRecord;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

public abstract class IcebergIngestionIT extends BaseIcebergIT {

  private static final int PARTITION = 0;
  private String topic;

  protected String tableName;
  protected TopicPartition topicPartition;
  protected SnowflakeSinkService service;
  protected static final String simpleRecordJson = "{\"simple\": \"extra field\"}";

  protected static final PrimitiveJsonRecord primitiveJsonRecordValue =
      // FIXME: there is currently some bug in Iceberg when storing int64 values
      new PrimitiveJsonRecord(8L, 16L, 32L, /*64L,*/ "dogs are the best", 0.5, 0.25, true);
  protected static final PrimitiveJsonRecord emptyPrimitiveJsonRecordValue =
      // FIXME: there is currently some bug in Iceberg when storing int64 values
      new PrimitiveJsonRecord(0L, 0L, 0L, /*0L, */ null, 0.0, 0.0, false);
  protected static final String primitiveJson =
      "{"
          + "  \"id_int8\": 8,"
          + "  \"id_int16\": 16,"
          + "  \"id_int32\": 32,"
          + "  \"id_int64\": 64,"
          + "  \"description\": \"dogs are the best\","
          + "  \"rating_float32\": 0.5,"
          + "  \"rating_float64\": 0.25,"
          + "  \"approval\": true"
          + "}";

  protected static final String primitiveJsonWithSchema =
      "{"
          + "  \"schema\": {"
          + "    \"type\": \"struct\","
          + "    \"fields\": ["
          + "      {"
          + "        \"field\": \"id_int8\","
          + "        \"type\": \"int8\""
          + "      },"
          + "      {"
          + "        \"field\": \"id_int16\","
          + "        \"type\": \"int16\""
          + "      },"
          + "      {"
          + "        \"field\": \"id_int32\","
          + "        \"type\": \"int32\""
          + "      },"
          + "      {"
          + "        \"field\": \"id_int64\","
          + "        \"type\": \"int64\""
          + "      },"
          + "      {"
          + "        \"field\": \"description\","
          + "        \"type\": \"string\""
          + "      },"
          + "      {"
          + "        \"field\": \"rating_float32\","
          + "        \"type\": \"float\""
          + "      },"
          + "      {"
          + "        \"field\": \"rating_float64\","
          + "        \"type\": \"double\""
          + "      },"
          + "      {"
          + "        \"field\": \"approval\","
          + "        \"type\": \"boolean\""
          + "      }"
          + "    ],"
          + "    \"optional\": false,"
          + "    \"name\": \"sf.kc.test\""
          + "  },"
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
    config.put(ENABLE_SCHEMATIZATION_CONFIG, isSchemaEvolutionEnabled().toString());

    createIcebergTable();
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

  protected abstract void createIcebergTable();

  protected abstract Boolean isSchemaEvolutionEnabled();

  protected void waitForOffset(int targetOffset) throws Exception {
    TestUtils.assertWithRetry(() -> service.getOffset(topicPartition) == targetOffset);
  }

  protected SinkRecord createKafkaRecord(String jsonString, int offset, boolean withSchema) {
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

  private final String selectAllSortByOffset =
      "WITH extracted_data AS ("
          + "SELECT *, RECORD_METADATA:\"offset\"::number AS offset_extracted "
          + "FROM identifier(?) "
          + ") "
          + "SELECT * FROM extracted_data "
          + "ORDER BY offset_extracted asc;";

  protected List<RecordWithMetadata<PrimitiveJsonRecord>> selectAllSchematizedRecords() {

    return select(tableName, selectAllSortByOffset, PrimitiveJsonRecord::fromSchematizedResult);
  }

  protected List<RecordWithMetadata<PrimitiveJsonRecord>> selectAllFromRecordContent() {
    return select(tableName, selectAllSortByOffset, PrimitiveJsonRecord::fromRecordContentColumn);
  }
}
