package com.snowflake.kafka.connector.internal.streaming.schemaevolution.iceberg;

import com.google.common.base.Preconditions;

/** Class with object types compatible with Snowflake Iceberg table */
class IcebergColumnTree {

  private final IcebergFieldNode rootNode;

  String getColumnName() {
    return rootNode.name;
  }

  IcebergColumnTree(ApacheIcebergColumnSchema columnSchema) {
    // rootNodes name serve as a name of the column, hence it is uppercase
    String columnName = columnSchema.getColumnName().toUpperCase();
    this.rootNode = new IcebergFieldNode(columnName, columnSchema.getSchema());
  }

  IcebergColumnTree(IcebergColumnJsonValuePair pair) {
    // rootNodes name serve as a name of the column, hence it is uppercase
    String columnName = pair.getColumnName().toUpperCase();
    this.rootNode = new IcebergFieldNode(columnName, pair.getJsonNode());
  }

  /** Add fields from other tree. Do not override nor modify any already existing nodes. */
  IcebergColumnTree merge(IcebergColumnTree modifiedTree) {
    Preconditions.checkArgument(
        this.getColumnName().equals(modifiedTree.getColumnName()),
        "Error merging column schemas. Tried to merge schemas for two different columns");
    this.rootNode.merge(modifiedTree.rootNode);
    return this;
  }

  String buildType() {
    StringBuilder sb = new StringBuilder();
    return rootNode.buildQuery(sb, "ROOT_NODE").toString();
  }
}
