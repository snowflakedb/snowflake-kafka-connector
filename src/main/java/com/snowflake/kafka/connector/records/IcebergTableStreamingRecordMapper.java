package com.snowflake.kafka.connector.records;

import static com.snowflake.kafka.connector.Utils.TABLE_COLUMN_CONTENT;
import static com.snowflake.kafka.connector.Utils.TABLE_COLUMN_METADATA;
import static com.snowflake.kafka.connector.records.RecordService.*;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.snowflake.kafka.connector.Utils;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

class IcebergTableStreamingRecordMapper extends StreamingRecordMapper {
  private static final TypeReference<Map<String, Object>> OBJECTS_MAP_TYPE_REFERENCE =
      new TypeReference<>() {};

  public IcebergTableStreamingRecordMapper(
      ObjectMapper objectMapper, boolean schematizationEnabled, boolean ssv2Enabled) {
    super(objectMapper, schematizationEnabled, ssv2Enabled);
  }

  @Override
  public Map<String, Object> processSnowflakeRecord(SnowflakeTableRow row, boolean includeMetadata)
      throws JsonProcessingException {
    final Map<String, Object> streamingIngestRow = new HashMap<>();
    for (JsonNode node : row.getContent().getData()) {
      if (schematizationEnabled) {
        streamingIngestRow.putAll(getMapForSchematization(node));
      } else {
        streamingIngestRow.put(TABLE_COLUMN_CONTENT, getMapForNoSchematization(node));
      }
    }
    if (includeMetadata) {
      Map<String, Object> mapForMetadata = getMapForMetadata(row.getMetadata());
      if (ssv2Enabled) {
        // ssv2 requires explicit type casting in pipe definition. For Map<> it does not ingest data
        // when any of the values is missing.
        // Passing POJO solves this problem and makes pipe definition easier.
        MetadataRecord metadata =
            new MetadataRecord(
                getNullSafeLong(mapForMetadata, OFFSET),
                (String) mapForMetadata.get(TOPIC),
                (Integer) mapForMetadata.get(PARTITION),
                (String) mapForMetadata.get(KEY),
                (Integer) mapForMetadata.get(SCHEMA_ID),
                (Integer) mapForMetadata.get(KEY_SCHEMA_ID),
                getNullSafeLong(mapForMetadata, "CreateTime"),
                getNullSafeLong(mapForMetadata, "LogAppendTime"),
                getNullSafeLong(mapForMetadata, "SnowflakeConnectorPushTime"),
                (Map<String, String>) mapForMetadata.get(HEADERS));
        streamingIngestRow.put(TABLE_COLUMN_METADATA, metadata);
      } else {
        streamingIngestRow.put(TABLE_COLUMN_METADATA, mapForMetadata);
      }
    }
    return streamingIngestRow;
  }

  private static Long getNullSafeLong(Map<String, Object> mapForMetadata, String key) {
    return mapForMetadata.get(key) == null ? null : ((Number) mapForMetadata.get(key)).longValue();
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
        .collect(HashMap::new, (m, v) -> m.put(v.getKey(), v.getValue()), HashMap::putAll);
  }

  private Map<String, Object> getMapForMetadata(JsonNode metadataNode)
      throws JsonProcessingException {
    Map<String, Object> values = mapper.convertValue(metadataNode, OBJECTS_MAP_TYPE_REFERENCE);
    // we don't want headers to be serialized as Map<String, Object> so we overwrite it as
    // Map<String, String>
    Map<String, String> headers = convertHeaders(metadataNode.findValue(HEADERS));
    values.put(HEADERS, headers);
    return values;
  }

  private Map<String, String> convertHeaders(JsonNode headersNode) throws JsonProcessingException {
    final Map<String, String> headers = new HashMap<>();

    if (headersNode == null || headersNode.isNull() || headersNode.isEmpty()) {
      return headers;
    }

    Iterator<String> fields = headersNode.fieldNames();
    while (fields.hasNext()) {
      String key = fields.next();
      JsonNode valueNode = headersNode.get(key);
      String value = getTextualValue(valueNode);
      headers.put(key, value);
    }
    return headers;
  }
}
