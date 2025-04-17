package com.snowflake.kafka.connector.streaming.iceberg;

import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.*;
import static com.snowflake.kafka.connector.internal.TestUtils.getConfForStreaming;

import com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig;
import com.snowflake.kafka.connector.dlq.InMemoryKafkaRecordErrorReporter;
import com.snowflake.kafka.connector.internal.SnowflakeSinkService;
import com.snowflake.kafka.connector.internal.SnowflakeSinkServiceFactory;
import com.snowflake.kafka.connector.internal.TestUtils;
import com.snowflake.kafka.connector.internal.streaming.InMemorySinkTaskContext;
import com.snowflake.kafka.connector.internal.streaming.IngestionMethodConfig;
import com.snowflake.kafka.connector.streaming.iceberg.sql.ComplexJsonRecord;
import com.snowflake.kafka.connector.streaming.iceberg.sql.MetadataRecord.RecordWithMetadata;
import com.snowflake.kafka.connector.streaming.iceberg.sql.PrimitiveJsonRecord;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Headers;
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
  protected InMemoryKafkaRecordErrorReporter kafkaRecordErrorReporter;
  protected static final String simpleRecordJson = "{\"simple\": \"extra field\"}";

  @BeforeEach
  public void setUp() {
    tableName = TestUtils.randomTableName();
    topic = tableName;
    topicPartition = new TopicPartition(topic, PARTITION);
    Map<String, String> config = getConfForStreaming();
    SnowflakeSinkConnectorConfig.setDefaultValues(config);
    config.put(ICEBERG_ENABLED, "TRUE");
    config.put(ENABLE_SCHEMATIZATION_CONFIG, isSchemaEvolutionEnabled().toString());
    config.put(SNOWPIPE_STREAMING_ENABLE_SINGLE_BUFFER, "true");
    // "snowflake.streaming.max.client.lag" = 1 second, for faster tests
    config.put(SNOWPIPE_STREAMING_MAX_CLIENT_LAG, "1");
    config.put(ERRORS_TOLERANCE_CONFIG, SnowflakeSinkConnectorConfig.ErrorTolerance.ALL.toString());
    config.put(ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_CONFIG, "test_DLQ");

    createIcebergTable();
    enableSchemaEvolution(tableName);

    // only insert fist topic to topicTable
    Map<String, String> topic2Table = new HashMap<>();
    topic2Table.put(topic, tableName);

    kafkaRecordErrorReporter = new InMemoryKafkaRecordErrorReporter();
    service =
        SnowflakeSinkServiceFactory.builder(conn, IngestionMethodConfig.SNOWPIPE_STREAMING, config)
            .setErrorReporter(kafkaRecordErrorReporter)
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

  protected void waitForOffset(long targetOffset) throws Exception {
    TestUtils.assertWithRetry(() -> service.getOffset(topicPartition) == targetOffset);
  }

  protected SinkRecord createKafkaRecord(String jsonString, long offset, boolean withSchema) {
    JsonConverter converter = new JsonConverter();
    converter.configure(
        Collections.singletonMap("schemas.enable", Boolean.toString(withSchema)), false);
    SchemaAndValue inputValue =
        converter.toConnectData(topic, jsonString.getBytes(StandardCharsets.UTF_8));
    Headers headers = new ConnectHeaders();
    headers.addBoolean("booleanHeader", true);
    headers.addString("stringHeader", "test");
    headers.addInt("intHeader", 123);
    headers.addDouble("doubleHeader", 1.234);
    headers.addFloat("floatHeader", 1.234f);
    headers.addLong("longHeader", 123L);
    headers.addShort("shortHeader", (short) 123);
    return new SinkRecord(
        topic,
        PARTITION,
        Schema.STRING_SCHEMA,
        "test",
        inputValue.schema(),
        inputValue.value(),
        offset,
        System.currentTimeMillis(),
        TimestampType.CREATE_TIME,
        headers);
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

  protected List<RecordWithMetadata<ComplexJsonRecord>>
      selectAllComplexJsonRecordFromRecordContent() {
    return select(tableName, selectAllSortByOffset, ComplexJsonRecord::fromRecordContentColumn);
  }
}
