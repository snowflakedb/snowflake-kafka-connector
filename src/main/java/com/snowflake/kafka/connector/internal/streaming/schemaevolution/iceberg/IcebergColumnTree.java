package com.snowflake.kafka.connector.internal.streaming.schemaevolution.iceberg;

/** Class with object types compatible with Snowflake Iceberg table */
public class IcebergColumnTree {

  private final IcebergFieldNode rootNode;

  public String getColumnName() {
    return rootNode.name;
  }

  IcebergColumnTree(ApacheIcebergColumnSchema columnSchema) {
    this.rootNode = new IcebergFieldNode(columnSchema.getColumnName(), columnSchema.getSchema());
  }

  IcebergColumnTree(IcebergColumnJsonValuePair pair) {
    this.rootNode = new IcebergFieldNode(pair.getColumnName(), pair.getJsonNode());
  }

  public String buildQueryPartWithNamesAndTypes() {
    StringBuilder sb = new StringBuilder();
    return rootNode.buildQuery(sb, "ROOT_NODE").toString();
  }
}
