package com.snowflake.kafka.connector.records;

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

  public IcebergTableStreamingRecordMapper(ObjectMapper objectMapper) {
    super(objectMapper);
  }

  @Override
  public Map<String, Object> processSnowflakeRecord(SnowflakeTableRow row, boolean includeMetadata)
      throws JsonProcessingException {
    final Map<String, Object> streamingIngestRow = new HashMap<>();
    for (JsonNode node : row.getContent().getData()) {
      streamingIngestRow.putAll(getMapForSchematization(node));
    }
    if (includeMetadata) {
      streamingIngestRow.put(TABLE_COLUMN_METADATA, getMapForMetadata(row.getMetadata()));
    }
    return streamingIngestRow;
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
