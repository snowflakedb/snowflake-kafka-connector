package com.snowflake.kafka.connector.internal.streaming.schemaevolution.iceberg;

import java.util.LinkedHashMap;
import java.util.stream.Collectors;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

class IcebergFieldNode {

  public final String name;

  public final String snowflakeIcebergType;

  public final LinkedHashMap<String, IcebergFieldNode> children;

  IcebergFieldNode(String name, Type apacheIcebergSchema) {
    this.name = name;
    this.snowflakeIcebergType = IcebergColumnTypeMapper.mapToSnowflakeDataType(apacheIcebergSchema);
    this.children = produceChildren(apacheIcebergSchema);
  }

  private LinkedHashMap<String, IcebergFieldNode> produceChildren(Type apacheIcebergSchema) {
    // primitives must not have children
    if (apacheIcebergSchema.isPrimitiveType()) {
      return new LinkedHashMap<>();
    }
    Type.NestedType nestedField = apacheIcebergSchema.asNestedType();
    return nestedField.fields().stream()
        .collect(
            Collectors.toMap(
                Types.NestedField::name,
                this::fromNestedField,
                // It's impossible to have two same keys
                (v1, v2) -> {
                  throw new IllegalArgumentException("Two same keys: " + v1);
                },
                LinkedHashMap::new));
  }

  private IcebergFieldNode fromNestedField(Types.NestedField field) {
    return new IcebergFieldNode(field.name(), field.type());
  }

  /**
   * @param sb StringBuilder
   * @param parentType Snowflake Iceberg table compatible type. If a root node is a parent then
   *     "ROOT_NODE" is passed, because we always generate root nodes column name.
   * @return StringBuilder with appended query elements
   */
  StringBuilder buildQuery(StringBuilder sb, String parentType) {
    if (parentType.equals("ARRAY") || parentType.equals("MAP")) {
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

  private StringBuilder appendNameAndType(StringBuilder sb) {
    sb.append(name);
    sb.append(" ");
    sb.append(snowflakeIcebergType);
    return sb;
  }

  private StringBuilder appendChildren(StringBuilder sb, String parentType) {
    children.forEach(
        (name, node) -> {
          node.buildQuery(sb, parentType);
          sb.append(", ");
        });
    removeLastSeparator(sb);
    return sb;
  }

  private StringBuilder removeLastSeparator(StringBuilder sb) {
    sb.deleteCharAt(sb.length() - 1);
    sb.deleteCharAt(sb.length() - 1);
    return sb;
  }
}
