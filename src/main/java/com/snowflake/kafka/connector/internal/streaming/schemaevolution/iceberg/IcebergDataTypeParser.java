package com.snowflake.kafka.connector.internal.streaming.schemaevolution.iceberg;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import java.util.Iterator;
import java.util.List;
import javax.annotation.Nonnull;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.JsonUtil;

/**
 * This class is used to Iceberg data type (include primitive types and nested types) serialization
 * and deserialization.
 */
class IcebergDataTypeParser {
  public static final String ELEMENT = "element";
  public static final String KEY = "key";
  public static final String VALUE = "value";
  private static final String TYPE = "type";
  private static final String STRUCT = "struct";
  private static final String LIST = "list";
  private static final String MAP = "map";
  private static final String FIELDS = "fields";
  private static final String DOC = "doc";
  private static final String NAME = "name";
  private static final String ID = "id";
  private static final String ELEMENT_ID = "element-id";
  private static final String KEY_ID = "key-id";
  private static final String VALUE_ID = "value-id";
  private static final String REQUIRED = "required";
  private static final String ELEMENT_REQUIRED = "element-required";
  private static final String VALUE_REQUIRED = "value-required";

  private static final String EMPTY_FIELD_CHAR = "\\";

  /** Object mapper for this class */
  private static final ObjectMapper MAPPER = new ObjectMapper();

  /**
   * Get Iceberg data type information by deserialization.
   *
   * @param icebergDataType string representation of Iceberg data type
   * @return Iceberg data type
   */
  static Type deserializeIcebergType(String icebergDataType) {
    try {
      JsonNode json = MAPPER.readTree(icebergDataType);
      return getTypeFromJson(json);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException(
          String.format("Failed to deserialize Iceberg data type: %s", icebergDataType));
    }
  }

  /**
   * Get corresponding Iceberg data type from JsonNode.
   *
   * @param jsonNode JsonNode parsed from Iceberg type string.
   * @return Iceberg data type
   */
  static Type getTypeFromJson(@Nonnull JsonNode jsonNode) {
    if (jsonNode.isTextual()) {
      return Types.fromPrimitiveString(jsonNode.asText());
    } else if (jsonNode.isObject()) {
      if (!jsonNode.has(TYPE)) {
        throw new IllegalArgumentException(
            String.format("Missing key '%s' in schema: %s", TYPE, jsonNode));
      }
      String type = jsonNode.get(TYPE).asText();
      if (STRUCT.equals(type)) {
        return structFromJson(jsonNode);
      } else if (LIST.equals(type)) {
        return listFromJson(jsonNode);
      } else if (MAP.equals(type)) {
        return mapFromJson(jsonNode);
      }
      throw new IllegalArgumentException(
          String.format("Cannot parse Iceberg type: %s, schema: %s", type, jsonNode));
    }

    throw new IllegalArgumentException("Cannot parse Iceberg type from schema: " + jsonNode);
  }

  /**
   * Get Iceberg struct type information from JsonNode.
   *
   * @param json JsonNode parsed from Iceberg type string.
   * @return struct type
   */
  static @Nonnull Types.StructType structFromJson(@Nonnull JsonNode json) {
    if (!json.has(FIELDS)) {
      throw new IllegalArgumentException(
          String.format("Missing key '%s' in schema: %s", FIELDS, json));
    }
    JsonNode fieldArray = json.get(FIELDS);
    Preconditions.checkArgument(fieldArray != null, "Field array cannot be null");
    Preconditions.checkArgument(
        fieldArray.isArray(), "Cannot parse struct fields from non-array: %s", fieldArray);

    List<Types.NestedField> fields = Lists.newArrayListWithExpectedSize(fieldArray.size());
    Iterator<JsonNode> iterator = fieldArray.elements();
    while (iterator.hasNext()) {
      JsonNode field = iterator.next();
      Preconditions.checkArgument(
          field.isObject(), "Cannot parse struct field from non-object: %s", field);

      int id = JsonUtil.getInt(ID, field);

      /* TypeToMessageType throws on empty field name, use a backslash to represent it and escape remaining backslash. */
      String name =
          JsonUtil.getString(NAME, field)
              .replace(EMPTY_FIELD_CHAR, EMPTY_FIELD_CHAR + EMPTY_FIELD_CHAR);
      if (name.isEmpty()) {
        name = EMPTY_FIELD_CHAR;
      }
      Type type = getTypeFromJson(field.get(TYPE));

      String doc = JsonUtil.getStringOrNull(DOC, field);
      boolean isRequired = JsonUtil.getBool(REQUIRED, field);
      if (isRequired) {
        fields.add(Types.NestedField.required(id, name, type, doc));
      } else {
        fields.add(Types.NestedField.optional(id, name, type, doc));
      }
    }

    return Types.StructType.of(fields);
  }

  /**
   * Get Iceberg list type information from JsonNode.
   *
   * @param json JsonNode parsed from Iceberg type string.
   * @return list type
   */
  static Types.ListType listFromJson(JsonNode json) {
    int elementId = JsonUtil.getInt(ELEMENT_ID, json);
    Type elementType = getTypeFromJson(json.get(ELEMENT));
    boolean isRequired = JsonUtil.getBool(ELEMENT_REQUIRED, json);

    if (isRequired) {
      return Types.ListType.ofRequired(elementId, elementType);
    } else {
      return Types.ListType.ofOptional(elementId, elementType);
    }
  }

  /**
   * Get Iceberg map type from JsonNode.
   *
   * @param json JsonNode parsed from Iceberg type string.
   * @return map type
   */
  static Types.MapType mapFromJson(JsonNode json) {
    int keyId = JsonUtil.getInt(KEY_ID, json);
    Type keyType = getTypeFromJson(json.get(KEY));

    int valueId = JsonUtil.getInt(VALUE_ID, json);
    Type valueType = getTypeFromJson(json.get(VALUE));

    boolean isRequired = JsonUtil.getBool(VALUE_REQUIRED, json);

    if (isRequired) {
      return Types.MapType.ofRequired(keyId, valueId, keyType, valueType);
    } else {
      return Types.MapType.ofOptional(keyId, valueId, keyType, valueType);
    }
  }
}
