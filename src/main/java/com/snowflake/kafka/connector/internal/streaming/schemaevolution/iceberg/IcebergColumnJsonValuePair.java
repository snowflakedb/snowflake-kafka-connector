package com.snowflake.kafka.connector.internal.streaming.schemaevolution.iceberg;

import com.fasterxml.jackson.databind.JsonNode;
import com.snowflake.kafka.connector.Utils;
import java.util.Map;

class IcebergColumnJsonValuePair {
  private final String columnName;
  private final String quotedColumnName;
  private final JsonNode jsonNode;

  static IcebergColumnJsonValuePair from(Map.Entry<String, JsonNode> field) {
    return new IcebergColumnJsonValuePair(field.getKey(), field.getValue());
  }

  IcebergColumnJsonValuePair(String columnName, JsonNode jsonNode) {
    this.columnName = columnName;
    this.quotedColumnName = Utils.quoteNameIfNeeded(columnName);
    this.jsonNode = jsonNode;
  }

  String getColumnName() {
    return columnName;
  }

  String getQuotedColumnName() {
    return quotedColumnName;
  }

  JsonNode getJsonNode() {
    return jsonNode;
  }
}
