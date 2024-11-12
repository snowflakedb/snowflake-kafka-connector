package com.snowflake.kafka.connector.records;

import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.SNOWFLAKE_STREAMING_METADATA_CONNECTOR_PUSH_TIME;
import static com.snowflake.kafka.connector.records.RecordService.CONNECTOR_PUSH_TIME;
import static com.snowflake.kafka.connector.records.RecordService.META;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.snowflake.kafka.connector.builder.SinkRecordBuilder;
import java.util.Map;
import javax.annotation.Nullable;
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

    RecordService service = RecordServiceFactory.createRecordService(false, false);

    Map<String, String> config =
        ImmutableMap.of(SNOWFLAKE_STREAMING_METADATA_CONNECTOR_PUSH_TIME, "true");
    service.setMetadataConfig(new SnowflakeMetadataConfig(config));

    // when
    JsonNode metadata = getMetadataNode(service, record);

    // then
    assertNotNull(metadata);
    assertFalse(metadata.has(CONNECTOR_PUSH_TIME));
  }
}
