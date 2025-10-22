package com.snowflake.kafka.connector.records;

import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.SNOWFLAKE_STREAMING_METADATA_CONNECTOR_PUSH_TIME;
import static com.snowflake.kafka.connector.records.RecordService.CONNECTOR_PUSH_TIME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.builder.SinkRecordBuilder;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

class SnowpipeStreamingMetaColumnTest extends AbstractMetaColumnTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Override
  @Nullable
  JsonNode getMetadataNode(RecordService service, SinkRecord record)
      throws JsonProcessingException {
    Map<String, Object> processedRecord = service.getProcessedRecordForStreamingIngest(record);
    return getMetadataNode(processedRecord);
  }

  @Test
  void connectorTimestamp_byDefault_writes() throws JsonProcessingException {
    // given
    SchemaAndValue input = getJsonInputData();
    SinkRecord record =
        SinkRecordBuilder.forTopicPartition(topic, partition)
            .withValueSchema(input.schema())
            .withValue(input.value())
            .build();

    Instant fixedNow = Instant.now();
    Clock fixedClock = Clock.fixed(fixedNow, ZoneOffset.UTC);

    ObjectMapper mapper = new ObjectMapper();
    RecordService service =
        new RecordService(
            fixedClock, new SnowflakeTableStreamingRecordMapper(mapper, false), mapper);

    // when
    Map<String, Object> recordData = service.getProcessedRecordForStreamingIngest(record);
    JsonNode metadata = getMetadataNode(recordData);

    // then
    assertNotNull(metadata);
    assertEquals(fixedNow.toEpochMilli(), metadata.get(CONNECTOR_PUSH_TIME).asLong());
  }

  @Test
  void connectorTimestamp_whenDisabled_ignores() throws JsonProcessingException {
    // given
    SchemaAndValue input = getJsonInputData();
    SinkRecord record =
        SinkRecordBuilder.forTopicPartition(topic, partition)
            .withValueSchema(input.schema())
            .withValue(input.value())
            .build();

    RecordService service = RecordServiceFactory.createRecordService(false, false);

    Map<String, String> config =
        ImmutableMap.of(SNOWFLAKE_STREAMING_METADATA_CONNECTOR_PUSH_TIME, "false");
    service.setMetadataConfig(new SnowflakeMetadataConfig(config));

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
