package com.snowflake.kafka.connector;

import com.snowflake.kafka.connector.internal.SnowflakeErrors;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** This is a class containing the helper functions related to schematization */
public class SchematizationUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(SchematizationUtils.class);

  public static Map<String, String> getSchemaMapFromRecord(SinkRecord record) {
    Map<String, String> schemaMap = new HashMap<>();
    Schema schema = record.valueSchema();
    for (Field field : schema.fields()) {
      schemaMap.put(field.name(), convertToSnowflakeType(field.schema().type()));
    }
    return schemaMap;
  }

  /**
   * With the list of extra columns, collect their types from either the record or from schema
   * fetched from schema registry
   *
   * @param recordNode the record body
   * @param columnNames the names of the extra columns
   * @param schemaMap the schema map from schema registry, could be empty
   * @return the map from columnNames to their types
   */
  public static Map<String, String> getColumnTypes(
      JsonNode recordNode, List<String> columnNames, Map<String, String> schemaMap) {
    if (columnNames == null) {
      return new HashMap<>();
    }
    Map<String, String> extraColumnToType = new HashMap<>();

    for (String columnName : columnNames) {
      if (!extraColumnToType.containsKey(columnName)) {
        String type;
        if (schemaMap.isEmpty()) {
          // no schema associated with the record
          type = getTypeFromJsonObject(recordNode.get(columnName));
        } else {
          type = schemaMap.get(columnName);
        }
        extraColumnToType.put(columnName, type);
      }
    }
    return extraColumnToType;
  }

  private static String getTypeFromJsonObject(Object value) {
    if (value == null) {
      return "VARIANT";
    }
    Type schemaType = ConnectSchema.schemaType(value.getClass());
    if (schemaType == null) {
      // only when the type of the value is unrecognizable for JAVA
      throw SnowflakeErrors.ERROR_5021.getException();
    }
    return convertToSnowflakeType(schemaType);
  }

  private static String convertToSnowflakeType(Type kafkaType) {
    switch (kafkaType) {
      case INT8:
        return "BYTEINT";
      case INT16:
        return "SMALLINT";
      case INT32:
        return "INT";
      case INT64:
        return "BIGINT";
      case FLOAT32:
        return "FLOAT";
      case FLOAT64:
        return "DOUBLE";
      case BOOLEAN:
        return "BOOLEAN";
      case STRING:
        return "VARCHAR";
      case BYTES:
        return "BINARY";
      case ARRAY:
        return "ARRAY";
      default:
        // MAP and STRUCT will go here
        return "VARIANT";
    }
  }

  /**
   * Transform the objectName to uppercase unless it is enclosed in double quotes
   *
   * <p>In that case, drop the quotes and leave it as it is.
   *
   * @param objectName name of the snowflake object, could be tableName, columnName, roleName, etc.
   * @return Transformed objectName
   */
  public static String formatName(String objectName) {
    return (objectName.charAt(0) == '"' && objectName.charAt(objectName.length() - 1) == '"')
        ? objectName.substring(1, objectName.length() - 1)
        : objectName.toUpperCase();
  }
}
