package com.snowflake.kafka.connector.internal.streaming;

import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_CONFIG;
import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.ERRORS_TOLERANCE_CONFIG;
import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.VALUE_CONVERTER;
import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.VALUE_CONVERTER_SCHEMA_REGISTRY_URL;

import com.snowflake.kafka.connector.builder.SinkRecordBuilder;
import com.snowflake.kafka.connector.dlq.InMemoryKafkaRecordErrorReporter;
import com.snowflake.kafka.connector.internal.SnowflakeConnectionService;
import com.snowflake.kafka.connector.internal.SnowflakeSinkService;
import com.snowflake.kafka.connector.internal.TestUtils;
import com.snowflake.kafka.connector.internal.streaming.v2.PipeNameProvider;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class SnowflakeSinkServiceV2SchematizationIT extends SnowflakeSinkServiceV2BaseIT {

  private final SnowflakeConnectionService conn = TestUtils.getConnectionService();
  private Map<String, String> config;
  private SnowflakeSinkService service;
  private String pipe;

  @BeforeEach
  public void setup() {
    config = TestUtils.getConnectorConfigurationForStreaming(false);
    config.put(VALUE_CONVERTER, "org.apache.kafka.connect.json.JsonConverter");
    config.put(VALUE_CONVERTER_SCHEMA_REGISTRY_URL, "http://fake-url");
    config.put("schemas.enable", "false");
    config.put(ERRORS_TOLERANCE_CONFIG, "all");
    config.put(ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_CONFIG, "dlq_topic");
    pipe = PipeNameProvider.buildPipeName(table);
  }

  @AfterEach
  public void teardown() {
    service.closeAll();
    TestUtils.dropTable(table);
    TestUtils.dropPipe(pipe);
  }

  @Test
  public void snowflakeSinkTask_put_whenJsonRecordCannotBeSchematized_sendRecordToDLQ() {
    // given
    conn.createTableWithOnlyMetadataColumn(table);

    InMemoryKafkaRecordErrorReporter errorReporter = new InMemoryKafkaRecordErrorReporter();

    service =
        StreamingSinkServiceBuilder.builder(conn, config)
            .withSinkTaskContext(new InMemorySinkTaskContext(Collections.singleton(topicPartition)))
            .withErrorReporter(errorReporter)
            .build();
    service.startPartition(topicPartition);

    // Create a record that cannot be schematized (array at root level)
    String notSchematizeableJsonRecord = "[{\"name\":\"sf\",\"answer\":42}]";
    SinkRecord record = createKafkaRecordWithoutSchema(notSchematizeableJsonRecord, 0);

    // when
    service.insert(record);

    // then
    Assertions.assertEquals(1, errorReporter.getReportedRecords().size());
  }

  /** Helper method to create a Kafka record from JSON string */
  private SinkRecord createKafkaRecord(String jsonWithSchema, long offset, boolean withSchema) {
    JsonConverter jsonConverter = new JsonConverter();
    Map<String, String> converterConfig = new HashMap<>();
    converterConfig.put("schemas.enable", String.valueOf(withSchema));
    jsonConverter.configure(converterConfig, false);

    byte[] valueBytes = jsonWithSchema.getBytes(StandardCharsets.UTF_8);
    SchemaAndValue schemaAndValue = jsonConverter.toConnectData(topic, valueBytes);

    return SinkRecordBuilder.forTopicPartition(topic, partition)
        .withSchemaAndValue(schemaAndValue)
        .withOffset(offset)
        .withKey("test")
        .build();
  }

  /**
   * Convenience method to create a Kafka record from JSON without schema (schemas.enable = false)
   */
  private SinkRecord createKafkaRecordWithoutSchema(String jsonPayload, long offset) {
    return createKafkaRecord(jsonPayload, offset, false);
  }
}
