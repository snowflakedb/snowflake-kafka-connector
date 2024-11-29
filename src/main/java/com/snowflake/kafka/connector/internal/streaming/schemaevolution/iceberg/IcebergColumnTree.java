package com.snowflake.kafka.connector.internal.streaming.schemaevolution.iceberg;

/** Class with object types compatible with Snowflake Iceberg table */
class IcebergColumnTree {

  private final IcebergFieldNode rootNode;

  String getColumnName() {
    return rootNode.name;
  }

  public IcebergFieldNode getRootNode() {
    return rootNode;
  }

  IcebergColumnTree(IcebergFieldNode rootNode) {
    this.rootNode = rootNode;
  }

  /** Returns data type of the column */
  String buildType() {
    StringBuilder sb = new StringBuilder();
    return rootNode.buildQuery(sb, "ROOT_NODE").toString();
  }
}
