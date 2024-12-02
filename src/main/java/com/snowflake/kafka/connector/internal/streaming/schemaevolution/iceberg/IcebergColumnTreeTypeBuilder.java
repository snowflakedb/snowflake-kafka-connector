package com.snowflake.kafka.connector.internal.streaming.schemaevolution.iceberg;

public class IcebergColumnTreeTypeBuilder {

  private static final String ROOT_NODE_TYPE = "ROOT_NODE";

  /** Returns data type of the column */
  String buildType(IcebergColumnTree columnTree) {
    StringBuilder sb = new StringBuilder();
    IcebergFieldNode rootNode = columnTree.getRootNode();
    return buildType(sb, rootNode, ROOT_NODE_TYPE).toString();
  }

  /**
   * Generate Snow SQL type for the column.
   *
   * @param sb StringBuilder
   * @param parentType Snowflake Iceberg table compatible type. ROOT_NODE_TYPE is a special case,
   *     here we never generate column name for it.
   * @return SQL type of the column
   */
  private StringBuilder buildType(StringBuilder sb, IcebergFieldNode fieldNode, String parentType) {
    if (parentType.equals("ARRAY")
        || parentType.equals("MAP")
        || parentType.equals(ROOT_NODE_TYPE)) {
      sb.append(fieldNode.snowflakeIcebergType);
    } else {
      appendNameAndType(sb, fieldNode);
    }
    if (!fieldNode.children.isEmpty()) {
      sb.append("(");
      appendChildren(sb, fieldNode);
      sb.append(")");
    }
    return sb;
  }

  private void appendNameAndType(StringBuilder sb, IcebergFieldNode fieldNode) {
    sb.append(fieldNode.name);
    sb.append(" ");
    sb.append(fieldNode.snowflakeIcebergType);
  }

  private void appendChildren(StringBuilder sb, IcebergFieldNode parentNode) {
    String parentType = parentNode.snowflakeIcebergType;
    parentNode.children.forEach(
        (name, childNode) -> {
          buildType(sb, childNode, parentType);
          sb.append(", ");
        });
    removeLastSeparator(sb);
  }

  private void removeLastSeparator(StringBuilder sb) {
    sb.deleteCharAt(sb.length() - 1);
    sb.deleteCharAt(sb.length() - 1);
  }
}
