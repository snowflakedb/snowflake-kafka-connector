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

  public SnowflakeTableStreamingRecordMapper(
      ObjectMapper mapper, boolean schematizationEnabled, boolean ssv2Enabled) {
    super(mapper, schematizationEnabled, ssv2Enabled);
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

  private Map<String, Object> getContent(RecordService.SnowflakeTableRow row)
      throws JsonProcessingException {
    if (ssv2Enabled) {
      return getContentForSSv2(row);
    } else {
      return getContentForSSv1(row);
    }
  }

  private Map<String, Object> getContentForSSv2(RecordService.SnowflakeTableRow row) {
    try {
      if (schematizationEnabled) {
        return getContentForSchematizedSSv2(row);
      } else {
        return getContentForNonSchematizedSSv2(row);
      }
    } catch (Exception e) {
      throw SnowflakeErrors.ERROR_0010.getException(e);
    }
  }

  private Map<String, Object> getContentForNonSchematizedSSv2(RecordService.SnowflakeTableRow row) {
    Map<String, Object> result = new HashMap<>();
    for (JsonNode node : row.getContent().getData()) {
      switch (node.getNodeType()) {
        case OBJECT:
          result.put(TABLE_COLUMN_CONTENT, mapper.convertValue(node, OBJECTS_MAP_TYPE_REFERENCE));
          break;
        case ARRAY:
          result.put(TABLE_COLUMN_CONTENT, mapper.convertValue(node, OBJECTS_LIST_TYPE_REFERENCE));
          break;
        default:
          result.put(TABLE_COLUMN_CONTENT, node.asText());
      }
    }
    return result;
  }

  private Map<String, Object> getContentForSchematizedSSv2(RecordService.SnowflakeTableRow row)
      throws JsonProcessingException {
    Map<String, Object> result = new HashMap<>();
    for (JsonNode node : row.getContent().getData()) {
      result.putAll(getMapFromJsonNodeForStreamingIngest(node, false));
    }
    return result;
  }

  private Map<String, Object> getContentForSSv1(RecordService.SnowflakeTableRow row)
      throws JsonProcessingException {
    Map<String, Object> result = new HashMap<>();
    for (JsonNode node : row.getContent().getData()) {
      if (schematizationEnabled) {
        result.putAll(getMapFromJsonNodeForStreamingIngest(node, true));
      } else {
        result.put(TABLE_COLUMN_CONTENT, mapper.writeValueAsString(node));
      }
    }
    return result;
  }

  private Map<String, Object> getMapFromJsonNodeForStreamingIngest(
      JsonNode node, boolean nestedObjectsAsString) throws JsonProcessingException {
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
      if (!nestedObjectsAsString) {
        if (columnNode.isObject()) {
          columnValue = mapper.convertValue(columnNode, OBJECTS_MAP_TYPE_REFERENCE);
        }
        if (columnNode.isArray()) {
          columnValue = mapper.convertValue(columnNode, OBJECTS_LIST_TYPE_REFERENCE);
        }
      }
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

  private Object getMetadata(RecordService.SnowflakeTableRow row) throws JsonProcessingException {
    if (ssv2Enabled) {
      Map<String, Object> mapForMetadata = getMapForMetadata(row.getMetadata());
      return metadataFromMap(mapForMetadata);
    } else {
      return mapper.writeValueAsString(row.getMetadata());
    }
  }
}
