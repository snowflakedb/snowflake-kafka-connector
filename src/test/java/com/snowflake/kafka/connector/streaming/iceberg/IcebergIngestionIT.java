package com.snowflake.kafka.connector.streaming.iceberg;

import static com.snowflake.kafka.connector.internal.TestUtils.getConnectorConfigurationForStreaming;

import com.snowflake.kafka.connector.ConnectorConfigTools;
import com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams;
import com.snowflake.kafka.connector.config.SinkTaskConfig;
import com.snowflake.kafka.connector.dlq.InMemoryKafkaRecordErrorReporter;
import com.snowflake.kafka.connector.internal.SnowflakeSinkService;
import com.snowflake.kafka.connector.internal.TestUtils;
import com.snowflake.kafka.connector.internal.streaming.InMemorySinkTaskContext;
import com.snowflake.kafka.connector.internal.streaming.StreamingSinkServiceBuilder;
import com.snowflake.kafka.connector.streaming.iceberg.sql.ComplexJsonRecord;
import com.snowflake.kafka.connector.streaming.iceberg.sql.RecordWithMetadata;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
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

  /**
   * Override in subclasses to create the target Iceberg table before the service starts. KCv4
   * requires pre-created Iceberg tables; auto-creation is not supported for them.
   */
  protected void createIcebergTable() {}

  /** Hook for subclasses to add/override connector config keys (e.g. external volume, version). */
  protected void customizeIcebergConfig(Map<String, String> config) {}

  @BeforeEach
  public void setUp() {
    tableName = TestUtils.randomTableName();
    topic = tableName;
    topicPartition = new TopicPartition(topic, PARTITION);

    createIcebergTable();

    Map<String, String> config = getConnectorConfigurationForStreaming(false);
    ConnectorConfigTools.setDefaultValues(config);
    // Declare the managed-Iceberg table type via config (exercises the real builderFrom parsing
    // path). The table is pre-created, so it is used as-is; table.type only governs auto-creation.
    config.put(KafkaConnectorConfigParams.SNOWFLAKE_AUTOCREATE_TABLE_TYPE, "iceberg");
    // Managed-Iceberg schema evolution is server-side; the connector rejects client-side
    // validation for Iceberg + schematization. Use server-side validation for all Iceberg ITs.
    config.put(KafkaConnectorConfigParams.SNOWFLAKE_VALIDATION, "server_side");
    customizeIcebergConfig(config);
    SinkTaskConfig sinkTaskConfig =
        SinkTaskConfig.builderFrom(config)
            .tolerateErrors(false)
            .dlqTopicName("test_DLQ")
            .topicToTableMap(Collections.singletonMap(topic, tableName))
            .build();

    kafkaRecordErrorReporter = new InMemoryKafkaRecordErrorReporter();
    service =
        StreamingSinkServiceBuilder.builder(snowflakeDatabase, sinkTaskConfig)
            .withErrorReporter(kafkaRecordErrorReporter)
            .withSinkTaskContext(new InMemorySinkTaskContext(Collections.singleton(topicPartition)))
            .build();

    // Mirror Kafka Connect's open(partitions) → put(records) lifecycle: start and await the channel
    // before any insert. Without this, the first insert would short-circuit via the
    // "channel doesn't exist" path in SnowflakeSinkServiceV2#insert and rewind the offsets, which
    // these direct-call tests don't replay (no real Kafka consumer in the loop).
    service.startPartition(topicPartition);
    service.awaitInitialization();
  }

  @AfterEach
  public void tearDown() {
    if (service != null) {
      service.closeAll();
    }
    dropIcebergTable(tableName);
  }

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

  protected List<RecordWithMetadata<ComplexJsonRecord>>
      selectAllComplexJsonRecordFromRecordContent() {
    return select(tableName, selectAllSortByOffset, ComplexJsonRecord::fromRecordContentColumn);
  }
}
