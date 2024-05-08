package com.snowflake.kafka.connector.records;

import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.SNOWFLAKE_STREAMING_METADATA_CONNECTOR_PUSH_TIME;
import static com.snowflake.kafka.connector.records.RecordService.CONNECTOR_PUSH_TIME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.google.common.collect.ImmutableMap;
import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.builder.SinkRecordBuilder;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.core.JsonProcessingException;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.JsonNode;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

class SnowpipeStreamingMetaColumnTest extends AbstractMetaColumnTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Override
  @Nullable
  JsonNode getMetadataNode(RecordService service, SinkRecord record)
      throws JsonProcessingException {
    Map<String, Object> processedRecord =
        service.getProcessedRecordForStreamingIngest(record, Instant.now());
    return getMetadataNode(processedRecord);
  }

  @Test
  void connectorTimestamp_whenEnabled_writes() throws JsonProcessingException {
    // given
    SchemaAndValue input = getJsonInputData();
    SinkRecord record =
        SinkRecordBuilder.forTopicPartition(topic, partition)
            .withValueSchema(input.schema())
            .withValue(input.value())
            .build();

    Instant now = Instant.now();

    RecordService service = new RecordService();

    Map<String, String> config =
        ImmutableMap.of(SNOWFLAKE_STREAMING_METADATA_CONNECTOR_PUSH_TIME, "true");
    service.setMetadataConfig(new SnowflakeMetadataConfig(config));

    // when
    Map<String, Object> recordData = service.getProcessedRecordForStreamingIngest(record, now);
    JsonNode metadata = getMetadataNode(recordData);

    // then
    assertNotNull(metadata);
    assertEquals(now.toEpochMilli(), metadata.get(CONNECTOR_PUSH_TIME).asLong());
  }

  @Test
  void connectorTimestamp_byDefault_ignores() throws JsonProcessingException {
    // given
    SchemaAndValue input = getJsonInputData();
    SinkRecord record =
        SinkRecordBuilder.forTopicPartition(topic, partition)
            .withValueSchema(input.schema())
            .withValue(input.value())
            .build();

    RecordService service = new RecordService();

    // when
    JsonNode metadata = getMetadataNode(service, record);

    // then
    assertNotNull(metadata);
    assertFalse(metadata.has(CONNECTOR_PUSH_TIME));
  }

  private @Nullable JsonNode getMetadataNode(Map<String, Object> processedRecord) {
    return Optional.ofNullable(processedRecord.get(Utils.TABLE_COLUMN_METADATA))
        .map(metadata -> assertInstanceOf(String.class, metadata))
        .map(this::readJson)
        .orElse(null);
  }

  private JsonNode readJson(String json) {
    try {
      return MAPPER.readTree(json);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }
}
