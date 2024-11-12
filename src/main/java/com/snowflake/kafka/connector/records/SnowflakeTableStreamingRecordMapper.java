package com.snowflake.kafka.connector.records;

import static com.snowflake.kafka.connector.Utils.TABLE_COLUMN_CONTENT;
import static com.snowflake.kafka.connector.Utils.TABLE_COLUMN_METADATA;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.internal.SnowflakeErrors;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

class SnowflakeTableStreamingRecordMapper extends StreamingRecordMapper {

  public SnowflakeTableStreamingRecordMapper(ObjectMapper mapper, boolean schematizationEnabled) {
    super(mapper, schematizationEnabled);
  }

  @Override
  public Map<String, Object> processSnowflakeRecord(
      RecordService.SnowflakeTableRow row, boolean includeAllMetadata)
      throws JsonProcessingException {
    final Map<String, Object> streamingIngestRow = new HashMap<>();
    for (JsonNode node : row.getContent().getData()) {
      if (schematizationEnabled) {
        streamingIngestRow.putAll(getMapFromJsonNodeForStreamingIngest(node));
      } else {
        streamingIngestRow.put(TABLE_COLUMN_CONTENT, mapper.writeValueAsString(node));
      }
    }
    if (includeAllMetadata) {
      streamingIngestRow.put(TABLE_COLUMN_METADATA, mapper.writeValueAsString(row.getMetadata()));
    }
    return streamingIngestRow;
  }

  private Map<String, Object> getMapFromJsonNodeForStreamingIngest(JsonNode node)
      throws JsonProcessingException {
    final Map<String, Object> streamingIngestRow = new HashMap<>();

    // return empty if tombstone record
    if (node.isEmpty()) {
      return streamingIngestRow;
    }

    Iterator<String> columnNames = node.fieldNames();
    while (columnNames.hasNext()) {
      String columnName = columnNames.next();
      JsonNode columnNode = node.get(columnName);
      Object columnValue = getTextualValue(columnNode);
      // while the value is always dumped into a string, the Streaming Ingest SDK
      // will transform the value according to its type in the table
      streamingIngestRow.put(Utils.quoteNameIfNeeded(columnName), columnValue);
    }
    // Thrown an exception if the input JsonNode is not in the expected format
    if (streamingIngestRow.isEmpty()) {
      throw SnowflakeErrors.ERROR_0010.getException(
          "Not able to convert node to Snowpipe Streaming input format");
    }
    return streamingIngestRow;
  }
}
