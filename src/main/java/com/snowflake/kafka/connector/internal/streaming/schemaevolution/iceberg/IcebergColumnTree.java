package com.snowflake.kafka.connector.internal.streaming.schemaevolution.iceberg;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.Map;

/** Class with object types compatible with Snowflake Iceberg table */
public class IcebergColumnTree {

  private final IcebergFieldNode rootNode;

  public String getColumnName() {
    return rootNode.name;
  }

  IcebergColumnTree(ApacheIcebergColumnSchema columnSchema) {
    this.rootNode = new IcebergFieldNode(columnSchema.getColumnName(), columnSchema.getSchema());
  }

  public IcebergColumnTree(String columnName, JsonNode recordNode) {
    // check for more than 1
    if (recordNode.isObject()) {
      Map.Entry<String, JsonNode> parentNode =
          recordNode.properties().stream()
              .findFirst()
              .orElseThrow(() -> new IllegalArgumentException("more than one child")); // todo
      this.rootNode = new IcebergFieldNode(parentNode.getKey(), parentNode.getValue());
    } else {
      this.rootNode = new IcebergFieldNode(columnName, recordNode);
    }
  }

  public String buildQuery() {
    StringBuilder sb = new StringBuilder();
    return rootNode.buildQuery(sb, "ROOT_NODE").toString();
  }
}
