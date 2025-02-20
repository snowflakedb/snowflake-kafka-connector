package com.snowflake.kafka.connector.internal.streaming.schemaevolution.iceberg;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.snowflake.kafka.connector.internal.KCLogger;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;

public class IcebergColumnTreeFactory {

  private final KCLogger LOGGER = new KCLogger(IcebergColumnTreeFactory.class.getName());

  private final IcebergColumnTypeMapper mapper;

  public IcebergColumnTreeFactory() {
    this.mapper = new IcebergColumnTypeMapper();
  }

  IcebergColumnTree fromIcebergSchema(IcebergColumnSchema columnSchema) {
    LOGGER.debug(
        "Attempting to parse schema from schema stored in a channel for column: "
            + columnSchema.getColumnName());
    IcebergFieldNode rootNode =
        createNode(columnSchema.getColumnName().toUpperCase(), columnSchema.getSchema());
    return new IcebergColumnTree(rootNode);
  }

  IcebergColumnTree fromJson(IcebergColumnJsonValuePair pair) {
    LOGGER.debug(
        "Attempting to parse schema from records payload for column: " + pair.getColumnName());
    IcebergFieldNode rootNode = createNode(pair.getColumnName().toUpperCase(), pair.getJsonNode());
    return new IcebergColumnTree(rootNode);
  }

  IcebergColumnTree fromConnectSchema(Field kafkaConnectField) {
    LOGGER.debug(
        "Attempting to parse schema from schema attached to a record for column: "
            + kafkaConnectField.name());
    IcebergFieldNode rootNode =
        createNode(kafkaConnectField.name().toUpperCase(), kafkaConnectField.schema());
    return new IcebergColumnTree(rootNode, kafkaConnectField.schema().doc());
  }

  // -- parse tree from Iceberg schema logic --
  private IcebergFieldNode createNode(String name, Type apacheIcebergSchema) {
    String snowflakeType = mapper.mapToColumnTypeFromIcebergSchema(apacheIcebergSchema);
    return new IcebergFieldNode(name, snowflakeType, produceChildren(apacheIcebergSchema));
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
                field -> createNode(field.name(), field.type()),
                // It's impossible to have two same keys
                (v1, v2) -> {
                  throw new IllegalArgumentException("Two same keys: " + v1);
                },
                LinkedHashMap::new));
  }

  // -- parse tree from kafka record payload logic --
  private IcebergFieldNode createNode(String name, JsonNode jsonNode) {
    String snowflakeType = mapper.mapToColumnTypeFromJson(name, jsonNode);
    return new IcebergFieldNode(name, snowflakeType, produceChildren(jsonNode));
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
    // STRING is set for an empty array: [] -> ARRAY(STRING)
    if (arrayElement == null) {
      LinkedHashMap<String, IcebergFieldNode> child = new LinkedHashMap<>();
      child.put("element", new IcebergFieldNode("element", "STRING", new LinkedHashMap<>()));
      return child;
    }
    LinkedHashMap<String, IcebergFieldNode> child = new LinkedHashMap<>();
    child.put("element", createNode("element", arrayElement));
    return child;
  }

  private LinkedHashMap<String, IcebergFieldNode> produceChildrenFromObject(ObjectNode objectNode) {
    return objectNode.properties().stream()
        .collect(
            Collectors.toMap(
                Map.Entry::getKey,
                stringJsonNodeEntry ->
                    createNode(stringJsonNodeEntry.getKey(), stringJsonNodeEntry.getValue()),
                (v1, v2) -> {
                  throw new IllegalArgumentException("Two same keys: " + v1);
                },
                LinkedHashMap::new));
  }

  // -- parse tree from kafka record schema logic --
  private IcebergFieldNode createNode(String name, Schema schema) {
    String snowflakeType =
        mapper.mapToColumnTypeFromKafkaSchema(schema.schema().type(), schema.schema().name());
    return new IcebergFieldNode(name, snowflakeType, produceChildren(schema.schema()));
  }

  private LinkedHashMap<String, IcebergFieldNode> produceChildren(Schema connectSchema) {
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
    child.put("element", createNode("element", connectSchemaForArray.valueSchema()));
    return child;
  }

  private LinkedHashMap<String, IcebergFieldNode> produceChildrenFromStruct(Schema connectSchema) {
    return connectSchema.fields().stream()
        .collect(
            Collectors.toMap(
                Field::name,
                f -> createNode(f.name(), f.schema()),
                (v1, v2) -> {
                  throw new IllegalArgumentException("Two same keys: " + v1);
                },
                LinkedHashMap::new));
  }

  private LinkedHashMap<String, IcebergFieldNode> produceChildrenFromMap(Schema connectSchema) {
    LinkedHashMap<String, IcebergFieldNode> keyValue = new LinkedHashMap<>();
    // these names will not be used when creating a query
    keyValue.put("key", createNode("key", connectSchema.keySchema()));
    keyValue.put("value", createNode("value", connectSchema.valueSchema()));
    return keyValue;
  }
}
