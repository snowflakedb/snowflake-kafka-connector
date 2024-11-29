package com.snowflake.kafka.connector.internal.streaming.schemaevolution.iceberg;

import java.util.LinkedHashMap;

class IcebergFieldNode {

  final String name;

  final String snowflakeIcebergType;

  final LinkedHashMap<String, IcebergFieldNode> children;

  public IcebergFieldNode(
      String name, String snowflakeIcebergType, LinkedHashMap<String, IcebergFieldNode> children) {
    this.name = name;
    this.snowflakeIcebergType = snowflakeIcebergType;
    this.children = children;
  }
}
