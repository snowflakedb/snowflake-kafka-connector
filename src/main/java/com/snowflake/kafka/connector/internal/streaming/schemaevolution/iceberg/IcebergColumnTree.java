package com.snowflake.kafka.connector.internal.streaming.schemaevolution.iceberg;

/** Class with object types compatible with Snowflake Iceberg table */
class IcebergColumnTree {

  private final IcebergFieldNode rootNode;
  private final String comment;

  String getColumnName() {
    return rootNode.name;
  }

  IcebergFieldNode getRootNode() {
    return rootNode;
  }

  String getComment() {
    return comment;
  }

  public IcebergColumnTree(IcebergFieldNode rootNode, String comment) {
    this.rootNode = rootNode;
    this.comment = comment;
  }

  IcebergColumnTree(IcebergFieldNode rootNode) {
    this.rootNode = rootNode;
    this.comment = null;
  }
}
