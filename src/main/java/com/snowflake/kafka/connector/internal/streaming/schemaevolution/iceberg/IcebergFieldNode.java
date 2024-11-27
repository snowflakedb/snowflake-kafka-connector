package com.snowflake.kafka.connector.internal.streaming.schemaevolution.iceberg;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;

class IcebergFieldNode {

  // todo consider refactoring into some more classes
  private final IcebergColumnTypeMapper mapper = IcebergColumnTypeMapper.INSTANCE;

  final String name;

  final String snowflakeIcebergType;

  final LinkedHashMap<String, IcebergFieldNode> children;

  IcebergFieldNode(String name, Type apacheIcebergSchema) {
    this.name = name;
    this.snowflakeIcebergType = mapper.mapToColumnTypeFromIcebergSchema(apacheIcebergSchema);
    this.children = produceChildren(apacheIcebergSchema);
  }

  IcebergFieldNode(String name, JsonNode jsonNode) {
    this.name = name;
    this.snowflakeIcebergType = mapper.mapToColumnTypeFromJson(jsonNode);
    this.children = produceChildren(jsonNode);
  }

  IcebergFieldNode(String name, Schema kafkaConnectField) {
    this.name = name;
    this.snowflakeIcebergType =
        mapper.mapToColumnTypeFromKafkaSchema(
            kafkaConnectField.schema().type(), kafkaConnectField.schema().name());
    this.children = produceChildren(kafkaConnectField);
  }

  private IcebergFieldNode(String name, String snowflakeIcebergType) {
    this.name = name;
    this.snowflakeIcebergType = snowflakeIcebergType;
    this.children = new LinkedHashMap<>();
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

  /**
   * Method does not modify, delete any existing nodes and its types, names. It is meant only to add
   * new children.
   */
  void merge(IcebergFieldNode nodeToMerge) {
    nodeToMerge.children.forEach(
        (key, node) -> {
          IcebergFieldNode thisChild = this.children.get(key);
          if (thisChild == null) {
            this.children.put(key, node);
          } else {
            thisChild.merge(node);
          }
        });
  }

  private LinkedHashMap<String, IcebergFieldNode> produceChildren(JsonNode recordNode) {
    if (recordNode.isNull()) {
      return new LinkedHashMap<>();
    }
    if (recordNode.isArray()) {
      ArrayNode arrayNode = (ArrayNode) recordNode;
      return produceChildrenFromArray(arrayNode);
    }
    if (recordNode.isObject()) {
      ObjectNode objectNode = (ObjectNode) recordNode;
      return produceChildrenFromObject(objectNode);
    }
    return new LinkedHashMap<>();
  }

  private LinkedHashMap<String, IcebergFieldNode> produceChildrenFromArray(ArrayNode arrayNode) {
    JsonNode arrayElement = arrayNode.get(0);
    // VARCHAR is set for an empty array: [] -> ARRAY(VARCHAR)
    if (arrayElement == null) {
      LinkedHashMap<String, IcebergFieldNode> child = new LinkedHashMap<>();
      child.put("element", new IcebergFieldNode("element", "VARCHAR(16777216)"));
      return child;
    }
    LinkedHashMap<String, IcebergFieldNode> child = new LinkedHashMap<>();
    child.put("element", new IcebergFieldNode("element", arrayElement));
    return child;
  }

  private LinkedHashMap<String, IcebergFieldNode> produceChildrenFromObject(ObjectNode objectNode) {
    return objectNode.properties().stream()
        .collect(
            Collectors.toMap(
                Map.Entry::getKey,
                stringJsonNodeEntry ->
                    new IcebergFieldNode(
                        stringJsonNodeEntry.getKey(), stringJsonNodeEntry.getValue()),
                (v1, v2) -> {
                  throw new IllegalArgumentException("Two same keys: " + v1);
                },
                LinkedHashMap::new));
  }

  private LinkedHashMap<String, IcebergFieldNode> produceChildren(Schema connectSchema) {
    Schema.Type type = connectSchema.type();
    if (connectSchema.type() == Schema.Type.STRUCT) {
      return produceChildrenFromStruct(connectSchema);
    }
    if (connectSchema.type() == Schema.Type.MAP) {
      return produceChildrenFromMap(connectSchema);
    }
    if (connectSchema.type() == Schema.Type.ARRAY) {
      return produceChildrenForArray(connectSchema);
    } else { // isPrimitive == true
      return new LinkedHashMap<>();
    }
  }

  private LinkedHashMap<String, IcebergFieldNode> produceChildrenForArray(
      Schema connectSchemaForArray) {
    LinkedHashMap<String, IcebergFieldNode> child = new LinkedHashMap<>();
    child.put("element", new IcebergFieldNode("element", connectSchemaForArray.valueSchema()));
    return child;
  }

  private LinkedHashMap<String, IcebergFieldNode> produceChildrenFromStruct(Schema connectSchema) {
    return connectSchema.fields().stream()
        .collect(
            Collectors.toMap(
                Field::name,
                f -> new IcebergFieldNode(f.name(), f.schema()),
                (v1, v2) -> {
                  throw new IllegalArgumentException("Two same keys: " + v1);
                },
                LinkedHashMap::new));
  }

  private LinkedHashMap<String, IcebergFieldNode> produceChildrenFromMap(Schema connectSchema) {
    LinkedHashMap<String, IcebergFieldNode> keyValue = new LinkedHashMap<>();
    // these names will not be used when creating a query
    keyValue.put("key", new IcebergFieldNode("key", connectSchema.keySchema()));
    keyValue.put("value", new IcebergFieldNode("value", connectSchema.valueSchema()));
    return keyValue;
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
