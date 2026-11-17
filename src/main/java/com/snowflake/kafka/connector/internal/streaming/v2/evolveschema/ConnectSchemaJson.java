package com.snowflake.kafka.connector.internal.streaming.v2.evolveschema;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Locale;
import java.util.Map;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;

/**
 * Writes a Kafka Connect schema in JsonConverter's schema format ({@code type}, {@code optional},
 * {@code name}, {@code version}, {@code doc}, {@code parameters}, and {@code fields} / {@code
 * items} / {@code keys} / {@code values}), which is what the evolve-schema endpoint expects for
 * {@link EvolveSchemaRequest#SCHEMA_TYPE_KAFKA_CONNECT}.
 *
 * <p>Default values are omitted: they don't affect column types.
 */
final class ConnectSchemaJson {

  private static final JsonNodeFactory NODES = JsonNodeFactory.instance;

  private ConnectSchemaJson() {}

  static ObjectNode toJson(Schema schema) {
    ObjectNode node = NODES.objectNode();
    node.put("type", schema.type().getName().toLowerCase(Locale.ROOT));
    switch (schema.type()) {
      case STRUCT:
        ArrayNode fields = node.putArray("fields");
        for (Field field : schema.fields()) {
          ObjectNode fieldNode = toJson(field.schema());
          fieldNode.put("field", field.name());
          fields.add(fieldNode);
        }
        break;
      case ARRAY:
        node.set("items", toJson(schema.valueSchema()));
        break;
      case MAP:
        node.set("keys", toJson(schema.keySchema()));
        node.set("values", toJson(schema.valueSchema()));
        break;
      default:
        break;
    }
    node.put("optional", schema.isOptional());
    if (schema.name() != null) {
      node.put("name", schema.name());
    }
    if (schema.version() != null) {
      node.put("version", schema.version());
    }
    if (schema.doc() != null) {
      node.put("doc", schema.doc());
    }
    if (schema.parameters() != null && !schema.parameters().isEmpty()) {
      ObjectNode parameters = node.putObject("parameters");
      for (Map.Entry<String, String> parameter : schema.parameters().entrySet()) {
        parameters.put(parameter.getKey(), parameter.getValue());
      }
    }
    return node;
  }
}
