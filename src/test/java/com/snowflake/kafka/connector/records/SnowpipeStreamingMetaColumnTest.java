package com.snowflake.kafka.connector.records;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import com.snowflake.kafka.connector.Utils;
import java.util.Map;
import java.util.Optional;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.core.JsonProcessingException;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.JsonNode;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.connect.sink.SinkRecord;

class SnowpipeStreamingMetaColumnTest extends AbstractMetaColumnTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Override
  JsonNode getMetadataNode(RecordService service, SinkRecord record)
      throws JsonProcessingException {
    Map<String, Object> processedRecord = service.getProcessedRecordForStreamingIngest(record);

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
