package com.snowflake.kafka.connector.records;

import static com.snowflake.kafka.connector.records.RecordService.CONNECTOR_PUSH_TIME;
import static com.snowflake.kafka.connector.records.RecordService.META;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.snowflake.kafka.connector.builder.SinkRecordBuilder;
import java.time.Instant;
import javax.annotation.Nullable;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.core.JsonProcessingException;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.JsonNode;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

class SnowpipeMetaColumnTest extends AbstractMetaColumnTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Override
  @Nullable
  JsonNode getMetadataNode(RecordService service, SinkRecord record)
      throws JsonProcessingException {
    String processedRecord = service.getProcessedRecordForSnowpipe(record);
    return MAPPER.readTree(processedRecord).get(META);
  }

  @Test
  void connectorTimestamp_alwaysNull() throws JsonProcessingException {
    // given
    SchemaAndValue input = getJsonInputData();
    SinkRecord record =
        SinkRecordBuilder.forTopicPartition(topic, partition)
            .withValueSchema(input.schema())
            .withValue(input.value())
            .build();

    Instant now = Instant.now();

    RecordService service = new RecordService();

    // when
    JsonNode metadata = getMetadataNode(service, record);

    // then
    assertNotNull(metadata);
    assertFalse(metadata.has(CONNECTOR_PUSH_TIME));
  }
}
