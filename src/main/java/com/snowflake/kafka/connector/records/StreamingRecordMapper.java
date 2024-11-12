package com.snowflake.kafka.connector.records;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.NumericNode;
import com.snowflake.kafka.connector.records.RecordService.SnowflakeTableRow;
import java.util.Map;

abstract class StreamingRecordMapper {

  protected final ObjectMapper mapper;
  protected final boolean schematizationEnabled;

  public StreamingRecordMapper(ObjectMapper mapper, boolean schematizationEnabled) {
    this.mapper = mapper;
    this.schematizationEnabled = schematizationEnabled;
  }

  abstract Map<String, Object> processSnowflakeRecord(
      SnowflakeTableRow row, boolean includeAllMetadata) throws JsonProcessingException;

  protected String getTextualValue(JsonNode valueNode) throws JsonProcessingException {
    String value;
    if (valueNode.isTextual()) {
      value = valueNode.textValue();
    } else if (valueNode.isNull()) {
      value = null;
    } else {
      value = writeValueAsStringOrNan(valueNode);
    }
    return value;
  }

  protected String writeValueAsStringOrNan(JsonNode columnNode) throws JsonProcessingException {
    if (columnNode instanceof NumericNode && ((NumericNode) columnNode).isNaN()) {
      return "NaN";
    } else {
      return mapper.writeValueAsString(columnNode);
    }
  }
}
