package com.snowflake.kafka.connector.internal.streaming.schemaevolution.iceberg;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.LinkedHashMap;
import java.util.stream.Collectors;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

class IcebergFieldNode {

  // todo consider refactoring into some more classes
  private final IcebergColumnTypeMapper mapper = IcebergColumnTypeMapper.INSTANCE;

  public final String name;

  public final String snowflakeIcebergType;

  public final LinkedHashMap<String, IcebergFieldNode> children;

  IcebergFieldNode(String name, Type apacheIcebergSchema) {
    this.name = name;
    this.snowflakeIcebergType = mapper.mapToSnowflakeDataType(apacheIcebergSchema);
    this.children = produceChildren(apacheIcebergSchema);
  }

  // todo refactor
  IcebergFieldNode(String name, JsonNode jsonNode) {
    this.name = name;
    this.snowflakeIcebergType = mapper.mapToColumnDataTypeFromJson(jsonNode);
    this.children = produceChildren(jsonNode);
  }

  private LinkedHashMap<String, IcebergFieldNode> produceChildren(JsonNode recordNode) {
    // primitives must not have children
    if (recordNode.isEmpty() || recordNode.isNull()) {
      return new LinkedHashMap<>();
    }
    if (recordNode.isObject()) {
      ObjectNode objectNode = (ObjectNode) recordNode;
      return objectNode.properties().stream()
          .collect(
              Collectors.toMap(
                  stringJsonNodeEntry -> stringJsonNodeEntry.getKey(),
                  stringJsonNodeEntry ->
                      new IcebergFieldNode(
                          stringJsonNodeEntry.getKey(), stringJsonNodeEntry.getValue()),
                  (v1, v2) -> {
                    throw new IllegalArgumentException("Two same keys: " + v1);
                  },
                  LinkedHashMap::new));
    }
    if (recordNode.isArray()) {
      // todo
      throw new IllegalArgumentException("Array is not yet implemented");
    }
    return new LinkedHashMap<>();
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
