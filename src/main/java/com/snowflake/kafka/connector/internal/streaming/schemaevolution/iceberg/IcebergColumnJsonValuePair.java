package com.snowflake.kafka.connector.internal.streaming.schemaevolution.iceberg;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.Map;

class IcebergColumnJsonValuePair {
  private final String columnName;
  private final JsonNode jsonNode;

  static IcebergColumnJsonValuePair from(Map.Entry<String, JsonNode> field) {
    return new IcebergColumnJsonValuePair(field.getKey(), field.getValue());
  }

  IcebergColumnJsonValuePair(String columnName, JsonNode jsonNode) {
    this.columnName = columnName;
    this.jsonNode = jsonNode;
  }

  String getColumnName() {
    return columnName;
  }

  JsonNode getJsonNode() {
    return jsonNode;
  }
}
