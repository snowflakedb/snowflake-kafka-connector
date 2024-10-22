package com.snowflake.kafka.connector.records;

import static com.snowflake.kafka.connector.Utils.TABLE_COLUMN_CONTENT;
import static com.snowflake.kafka.connector.Utils.TABLE_COLUMN_METADATA;
import static com.snowflake.kafka.connector.records.RecordService.*;

import com.snowflake.kafka.connector.Utils;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.core.type.TypeReference;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.JsonNode;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.ObjectMapper;

class IcebergTableStreamingRecordMapper implements StreamingRecordMapper {
  private final ObjectMapper mapper;

  private static final TypeReference<Map<String, Object>> OBJECTS_MAP_TYPE_REFERENCE =
      new TypeReference<Map<String, Object>>() {};

  private static final TypeReference<Map<String, String>> HEADERS_MAP_TYPE_REFERENCE =
      new TypeReference<Map<String, String>>() {};

  public IcebergTableStreamingRecordMapper(ObjectMapper objectMapper) {
    this.mapper = objectMapper;
  }

  @Override
  public Map<String, Object> processSnowflakeRecord(
      SnowflakeTableRow row, boolean schematizationEnabled, boolean includeAllMetadata) {
    final Map<String, Object> streamingIngestRow = new HashMap<>();
    for (JsonNode node : row.getContent().getData()) {
      if (schematizationEnabled) {
        streamingIngestRow.putAll(getMapForSchematization(node));
      } else {
        streamingIngestRow.put(TABLE_COLUMN_CONTENT, getMapForNoSchematization(node));
      }
    }
    if (includeAllMetadata) {
      streamingIngestRow.put(TABLE_COLUMN_METADATA, getMapForMetadata(row.getMetadata()));
    }
    return streamingIngestRow;
  }

  private Map<String, Object> getMapForNoSchematization(JsonNode node) {
    return mapper.convertValue(node, OBJECTS_MAP_TYPE_REFERENCE);
  }

  private Map<String, Object> getMapForSchematization(JsonNode node) {
    // we need to quote the keys on the first level of the map as they are column names in the table
    // the rest must stay as is as the nested objects are not column names but fields name with case
    // sensitivity
    return mapper.convertValue(node, OBJECTS_MAP_TYPE_REFERENCE).entrySet().stream()
        .map(
            entry ->
                new AbstractMap.SimpleEntry<>(
                    Utils.quoteNameIfNeeded(entry.getKey()), entry.getValue()))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  private Map<String, Object> getMapForMetadata(JsonNode metadataNode) {
    Map<String, Object> values = mapper.convertValue(metadataNode, OBJECTS_MAP_TYPE_REFERENCE);
    // we don't want headers to be serialized as Map<String, Object> so we overwrite it as
    // Map<String, String>
    Map<String, String> headers =
        mapper.convertValue(metadataNode.findValue(HEADERS), HEADERS_MAP_TYPE_REFERENCE);
    values.put(HEADERS, headers);
    return values;
  }
}
