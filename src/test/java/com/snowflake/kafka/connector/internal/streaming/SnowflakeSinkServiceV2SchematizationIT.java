package com.snowflake.kafka.connector.internal.streaming;

import com.snowflake.kafka.connector.builder.SinkRecordBuilder;
import com.snowflake.kafka.connector.config.SinkTaskConfig;
import com.snowflake.kafka.connector.dlq.InMemoryKafkaRecordErrorReporter;
import com.snowflake.kafka.connector.internal.SnowflakeConnectionService;
import com.snowflake.kafka.connector.internal.SnowflakeSinkService;
import com.snowflake.kafka.connector.internal.TestUtils;
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
  private SinkTaskConfig sinkTaskConfig;
  private SnowflakeSinkService service;
  private String pipe;

  @BeforeEach
  public void setup() {
    Map<String, String> config = TestUtils.getConnectorConfigurationForStreaming(false);
    sinkTaskConfig =
        SinkTaskConfig.builderFrom(config).tolerateErrors(true).dlqTopicName("dlq_topic").build();
    pipe = table;
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
        StreamingSinkServiceBuilder.builder(conn, sinkTaskConfig)
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
