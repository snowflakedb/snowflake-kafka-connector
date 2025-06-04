package com.snowflake.kafka.connector.records;

import static com.snowflake.kafka.connector.Utils.TABLE_COLUMN_CONTENT;
import static com.snowflake.kafka.connector.Utils.TABLE_COLUMN_METADATA;
import static com.snowflake.kafka.connector.records.RecordService.*;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.snowflake.kafka.connector.Utils;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Map;

class IcebergTableStreamingRecordMapper extends StreamingRecordMapper {

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
        MetadataRecord metadata = metadataFromMap(mapForMetadata);
        streamingIngestRow.put(TABLE_COLUMN_METADATA, metadata);
      } else {
        streamingIngestRow.put(TABLE_COLUMN_METADATA, mapForMetadata);
      }
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
        .collect(HashMap::new, (m, v) -> m.put(v.getKey(), v.getValue()), HashMap::putAll);
  }
}
