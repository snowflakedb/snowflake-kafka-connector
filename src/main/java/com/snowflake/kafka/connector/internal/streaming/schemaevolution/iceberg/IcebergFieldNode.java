package com.snowflake.kafka.connector.internal.streaming.schemaevolution.iceberg;

import java.util.LinkedHashMap;

class IcebergFieldNode {

  // todo consider refactoring into some more classes
  private final IcebergColumnTypeMapper mapper = IcebergColumnTypeMapper.INSTANCE;

  final String name;

  final String snowflakeIcebergType;

  final LinkedHashMap<String, IcebergFieldNode> children;

  public IcebergFieldNode(
      String name, String snowflakeIcebergType, LinkedHashMap<String, IcebergFieldNode> children) {
    this.name = name;
    this.snowflakeIcebergType = snowflakeIcebergType;
    this.children = children;
  }

  /**
   * @param sb StringBuilder
   * @param parentType Snowflake Iceberg table compatible type. If a root node is a parent then
   *     "ROOT_NODE" is passed, because we always generate root nodes column name.
   * @return field name + data type
   */
  StringBuilder buildQuery(StringBuilder sb, String parentType) {
    if (parentType.equals("ARRAY") || parentType.equals("MAP") || parentType.equals("ROOT_NODE")) {
      sb.append(snowflakeIcebergType);
    } else {
      appendNameAndType(sb);
    }
    if (!children.isEmpty()) {
      sb.append("(");
      appendChildren(sb, this.snowflakeIcebergType);
      sb.append(")");
    }
    return sb;
  }

  private void appendNameAndType(StringBuilder sb) {
    sb.append(name);
    sb.append(" ");
    sb.append(snowflakeIcebergType);
  }

  private void appendChildren(StringBuilder sb, String parentType) {
    children.forEach(
        (name, node) -> {
          node.buildQuery(sb, parentType);
          sb.append(", ");
        });
    removeLastSeparator(sb);
  }

  private void removeLastSeparator(StringBuilder sb) {
    sb.deleteCharAt(sb.length() - 1);
    sb.deleteCharAt(sb.length() - 1);
  }
}
