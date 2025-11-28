package com.snowflake.kafka.connector.records;

import static com.snowflake.kafka.connector.Utils.TABLE_COLUMN_METADATA;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.snowflake.kafka.connector.internal.SnowflakeErrors;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

class SnowflakeTableStreamingRecordMapper extends StreamingRecordMapper {

  public SnowflakeTableStreamingRecordMapper(ObjectMapper mapper) {
    super(mapper);
  }

  @Override
  public Map<String, Object> processSnowflakeRecord(
      RecordService.SnowflakeTableRow row, boolean includeAllMetadata)
      throws JsonProcessingException {
    final Map<String, Object> streamingIngestRow = new HashMap<>(getContent(row));
    if (includeAllMetadata) {
      streamingIngestRow.put(TABLE_COLUMN_METADATA, getMetadata(row));
    }
    return streamingIngestRow;
  }

  private Map<String, Object> getContent(RecordService.SnowflakeTableRow row) {
    try {
      return getContentForSchematized(row);
    } catch (Exception e) {
      throw SnowflakeErrors.ERROR_0010.getException(e);
    }
  }

  private Map<String, Object> getContentForSchematized(RecordService.SnowflakeTableRow row)
      throws JsonProcessingException {
    Map<String, Object> result = new HashMap<>();
    for (JsonNode node : row.getContent().getData()) {
      result.putAll(getMapFromJsonNodeForStreamingIngest(node));
    }
    return result;
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
      if (columnNode.isObject()) {
        columnValue = mapper.convertValue(columnNode, OBJECTS_MAP_TYPE_REFERENCE);
      }
      if (columnNode.isArray()) {
        columnValue = mapper.convertValue(columnNode, OBJECTS_LIST_TYPE_REFERENCE);
      }
      streamingIngestRow.put(columnName, columnValue);
    }
    // Thrown an exception if the input JsonNode is not in the expected format
    if (streamingIngestRow.isEmpty()) {
      throw SnowflakeErrors.ERROR_0010.getException(
          "Not able to convert node to Snowpipe Streaming input format");
    }
    return streamingIngestRow;
  }

  private Object getMetadata(RecordService.SnowflakeTableRow row) throws JsonProcessingException {
    return getMapForMetadata(row.getMetadata());
  }
}
