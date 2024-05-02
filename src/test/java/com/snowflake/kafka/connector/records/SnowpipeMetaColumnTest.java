package com.snowflake.kafka.connector.records;

import static com.snowflake.kafka.connector.records.RecordService.META;

import net.snowflake.client.jdbc.internal.fasterxml.jackson.core.JsonProcessingException;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.JsonNode;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.connect.sink.SinkRecord;

class SnowpipeMetaColumnTest extends AbstractMetaColumnTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Override
  JsonNode getMetadataNode(RecordService service, SinkRecord record)
      throws JsonProcessingException {
    String processedRecord = service.getProcessedRecordForSnowpipe(record);
    return MAPPER.readTree(processedRecord).get(META);
  }
}
