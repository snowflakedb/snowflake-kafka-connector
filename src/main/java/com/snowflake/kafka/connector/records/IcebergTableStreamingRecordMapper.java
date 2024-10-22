package com.snowflake.kafka.connector.records;

import static com.snowflake.kafka.connector.Utils.TABLE_COLUMN_CONTENT;
import static com.snowflake.kafka.connector.Utils.TABLE_COLUMN_METADATA;
import static com.snowflake.kafka.connector.records.RecordService.*;

import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.internal.SnowflakeErrors;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.core.JsonProcessingException;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.core.type.TypeReference;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.JsonNode;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.ObjectMapper;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.node.NumericNode;

class IcebergTableStreamingRecordMapper implements StreamingRecordMapper {
  private final ObjectMapper mapper;

  public IcebergTableStreamingRecordMapper(ObjectMapper objectMapper) {
    this.mapper = objectMapper;
  }

  @Override
  public Map<String, Object> processSnowflakeRecord(
      SnowflakeTableRow row, boolean schematizationEnabled, boolean includeAllMetadata)
      throws JsonProcessingException {
    final Map<String, Object> streamingIngestRow = new HashMap<>();
    for (JsonNode node : row.getContent().getData()) {
      if (schematizationEnabled) {
        streamingIngestRow.putAll(getMapFromJsonNodeForStreamingIngest(node));
      } else {
        Map<String, Object> result =
            mapper.convertValue(node, new TypeReference<Map<String, Object>>() {});
        streamingIngestRow.put(TABLE_COLUMN_CONTENT, result);
      }
    }
    if (includeAllMetadata) {
      streamingIngestRow.put(
          TABLE_COLUMN_METADATA, getMapFromJsonNodeForIceberg(row.getMetadata()));
    }
    return streamingIngestRow;
  }

  private Map<String, Object> getMapFromJsonNodeForStreamingIngest(JsonNode node)
      throws JsonProcessingException {
    return getMapFromJsonNodeForStreamingIngest(node, true);
  }

  private Map<String, Object> getMapFromJsonNodeForIceberg(JsonNode node)
      throws JsonProcessingException {
    return getMapFromJsonNodeForStreamingIngest(node, false);
  }

  private Map<String, Object> getMapFromJsonNodeForStreamingIngest(
      JsonNode node, boolean quoteColumnName) throws JsonProcessingException {
    final Map<String, Object> streamingIngestRow = new HashMap<>();

    // return empty if tombstone record
    if (node.isEmpty()) {
      return streamingIngestRow;
    }

    Iterator<String> columnNames = node.fieldNames();
    while (columnNames.hasNext()) {
      String columnName = columnNames.next();
      JsonNode columnNode = node.get(columnName);
      Object columnValue;
      if (columnNode.isTextual()) {
        columnValue = columnNode.textValue();
      } else if (columnNode.isNull()) {
        columnValue = null;
      } else {
        columnValue = writeValueAsStringOrNan(columnNode);
      }
      // while the value is always dumped into a string, the Streaming Ingest SDK
      // will transform the value according to its type in the table
      streamingIngestRow.put(
          quoteColumnName ? Utils.quoteNameIfNeeded(columnName) : columnName, columnValue);
    }
    // Thrown an exception if the input JsonNode is not in the expected format
    if (streamingIngestRow.isEmpty()) {
      throw SnowflakeErrors.ERROR_0010.getException(
          "Not able to convert node to Snowpipe Streaming input format");
    }
    return streamingIngestRow;
  }

  private String writeValueAsStringOrNan(JsonNode columnNode) throws JsonProcessingException {
    if (columnNode instanceof NumericNode && ((NumericNode) columnNode).isNaN()) {
      return "NaN";
    } else {
      return mapper.writeValueAsString(columnNode);
    }
  }
}
