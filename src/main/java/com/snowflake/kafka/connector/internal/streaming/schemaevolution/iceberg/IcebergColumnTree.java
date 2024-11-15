package com.snowflake.kafka.connector.internal.streaming.schemaevolution.iceberg;

/** Class with object types compatible with Snowflake Iceberg table */
public class IcebergColumnTree {

  private final IcebergFieldNode rootNode;

  public IcebergColumnTree(ApacheIcebergColumnSchema columnSchema) {
    this.rootNode = new IcebergFieldNode(columnSchema.getColumnName(), columnSchema.getSchema());
  }

  public String buildQuery() {
    StringBuilder sb = new StringBuilder();
    return rootNode.buildQuery(sb, "ROOT_NODE").toString();
  }
}
