package com.snowflake.kafka.connector.records;

import static com.snowflake.kafka.connector.records.RecordService.HEADERS;
import static com.snowflake.kafka.connector.records.RecordService.KEY;
import static com.snowflake.kafka.connector.records.RecordService.KEY_SCHEMA_ID;
import static com.snowflake.kafka.connector.records.RecordService.OFFSET;
import static com.snowflake.kafka.connector.records.RecordService.PARTITION;
import static com.snowflake.kafka.connector.records.RecordService.SCHEMA_ID;
import static com.snowflake.kafka.connector.records.RecordService.TOPIC;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.DoubleNode;
import com.fasterxml.jackson.databind.node.FloatNode;
import com.fasterxml.jackson.databind.node.NumericNode;
import com.snowflake.kafka.connector.records.RecordService.SnowflakeTableRow;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

abstract class StreamingRecordMapper {

  protected static final TypeReference<Map<String, Object>> OBJECTS_MAP_TYPE_REFERENCE =
      new TypeReference<>() {};

  protected static final TypeReference<List<Object>> OBJECTS_LIST_TYPE_REFERENCE =
      new TypeReference<>() {};

  protected final ObjectMapper mapper;
  protected final boolean schematizationEnabled;
  protected final boolean ssv2Enabled;

  public StreamingRecordMapper(
      ObjectMapper mapper, boolean schematizationEnabled, boolean ssv2Enabled) {
    this.mapper = mapper;
    this.schematizationEnabled = schematizationEnabled;
    this.ssv2Enabled = ssv2Enabled;
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
      value = writeValueAsStringOrNanOrInfinity(valueNode);
    }
    return value;
  }

  protected String writeValueAsStringOrNanOrInfinity(JsonNode columnNode)
      throws JsonProcessingException {
    if (columnNode instanceof NumericNode && ((NumericNode) columnNode).isNaN()) {
      // DoubleNode::isNaN() and FloatNode::isNaN() will return true on both infinite values,
      // therefore we need to handle them here, where isNaN() is true
      boolean infinity = false;
      boolean negative = false;
      if (columnNode instanceof DoubleNode) {
        double value = (columnNode).doubleValue();
        infinity = Double.isInfinite(value);
        negative = value < 0;
      } else if ((columnNode instanceof FloatNode)) {
        float value = (columnNode).floatValue();
        infinity = Float.isInfinite(value);
        negative = value < 0;
      }
      if (infinity) {
        if (negative) {
          return "-Inf";
        } else {
          return "Inf";
        }
      } else {
        return "NaN";
      }
    } else {
      return mapper.writeValueAsString(columnNode);
    }
  }

  protected static Long getNullSafeLong(Map<String, Object> mapForMetadata, String key) {
    return mapForMetadata.get(key) == null ? null : ((Number) mapForMetadata.get(key)).longValue();
  }

  protected static String getNullSafeString(Map<String, Object> mapForMetadata, String key) {
    Object object = mapForMetadata.get(key);
    return object == null ? null : object.toString();
  }

  protected Map<String, Object> getMapForMetadata(JsonNode metadataNode)
      throws JsonProcessingException {
    Map<String, Object> values = mapper.convertValue(metadataNode, OBJECTS_MAP_TYPE_REFERENCE);
    // we don't want headers to be serialized as Map<String, Object> so we overwrite it as
    // Map<String, String>
    Map<String, String> headers = convertHeaders(metadataNode.findValue(HEADERS));
    values.put(HEADERS, headers);
    return values;
  }

  protected Map<String, String> convertHeaders(JsonNode headersNode)
      throws JsonProcessingException {
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

  protected MetadataRecord metadataFromMap(Map<String, Object> mapForMetadata) {
    return new MetadataRecord(
        getNullSafeLong(mapForMetadata, OFFSET),
        (String) mapForMetadata.get(TOPIC),
        (Integer) mapForMetadata.get(PARTITION),
        getNullSafeString(mapForMetadata, KEY),
        (Integer) mapForMetadata.get(SCHEMA_ID),
        (Integer) mapForMetadata.get(KEY_SCHEMA_ID),
        getNullSafeLong(mapForMetadata, "CreateTime"),
        getNullSafeLong(mapForMetadata, "LogAppendTime"),
        getNullSafeLong(mapForMetadata, "SnowflakeConnectorPushTime"),
        (Map<String, String>) mapForMetadata.get(HEADERS));
  }
}
