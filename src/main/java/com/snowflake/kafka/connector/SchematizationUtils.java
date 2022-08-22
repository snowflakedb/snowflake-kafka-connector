package com.snowflake.kafka.connector;

import com.google.common.annotations.VisibleForTesting;
import com.snowflake.kafka.connector.internal.SnowflakeErrors;
import io.confluent.connect.avro.AvroConverterConfig;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Schema.Type;

/** This is a class containing the helper functions related to schematization */
public class SchematizationUtils {

  // TODO: SNOW-649753 Directly get a list of columns from the response instead of parsing them from
  //  a string
  static final String EXTRA_COLUMNS_PREFIX = "Extra columns: ";

  static final String DEPRECATED_EXTRA_COLUMNS_PREFIX = "Extra column: ";
  static final String EXTRA_COLUMNS_SUFFIX =
      ". Columns not present in the table shouldn't be specified.";

  static final String NONNULLABLE_COLUMNS_PREFIX = "Missing columns: ";

  static final String DEPRECATED_NONNULLABLE_COLUMNS_PREFIX = "Missing column: ";

  static final String NONNULLABLE_COLUMNS_SUFFIX =
      ". Values for all non-nullable columns must be specified.";

  private static SchemaRegistryClient getAvroSchemaRegistryClientFromURL(
      final String schemaRegistryURL) {
    Map<String, String> srConfig = new HashMap<>();
    srConfig.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryURL);
    AvroConverterConfig avroConverterConfig = new AvroConverterConfig(srConfig);
    return new CachedSchemaRegistryClient(
        avroConverterConfig.getSchemaRegistryUrls(),
        avroConverterConfig.getMaxSchemasPerSubject(),
        Collections.singletonList(new AvroSchemaProvider()),
        srConfig,
        avroConverterConfig.requestHeaders());
  }

  /**
   * Get schema with its subject being [topicName]-[type]
   *
   * <p>Schema is stored in the schema registry in terms of subject, in our case the subject name
   * could either be "-value" or "-key". These names are set by the producer (or schema registry
   * client) automatically.
   *
   * <p>Only the value of the record is schematized, so when using the method we mostly want to
   * retrieve the schema stored as [topicName]-value
   *
   * @param topicName the name of the topic
   * @param schemaRegistry the schema registry client
   * @param type can only be "value" or "key", indicating we get the value schema or the key schema
   * @return the mapping from columnName to their data type, the column
   */
  @VisibleForTesting
  public static Map<String, String> getAvroSchemaFromSchemaRegistryClient(
      final String topicName, final SchemaRegistryClient schemaRegistry, final String type) {
    String subjectName = topicName + "-" + type;
    SchemaMetadata schemaMetadata;
    try {
      schemaMetadata = schemaRegistry.getLatestSchemaMetadata(subjectName);
    } catch (IOException | RestClientException e) {
      // suppress the excpetion and return empty map to indicate a failure in fetching schemas
      return new HashMap<>();
    }
    Map<String, String> schemaMap = new HashMap<>();
    if (schemaMetadata != null) {
      AvroSchema schema = new AvroSchema(schemaMetadata.getSchema());
      for (Schema.Field field : schema.rawSchema().getFields()) {
        Schema fieldSchema = field.schema();
        String columnName = field.name().toUpperCase();
        // avro does not support double quotes so the columnName will be in uppercase anyway
        // doing conversion here would save the trouble for other components
        if (schemaMap.containsKey(columnName)) {
          throw SnowflakeErrors.ERROR_0025.getException();
        }
        switch (fieldSchema.getType()) {
          case BOOLEAN:
            schemaMap.put(columnName, "boolean");
            break;
          case BYTES:
            schemaMap.put(columnName, "binary");
            break;
          case DOUBLE:
            schemaMap.put(columnName, "double");
            break;
          case FLOAT:
            schemaMap.put(columnName, "float");
            break;
          case INT:
            schemaMap.put(columnName, "int");
            break;
          case LONG:
            schemaMap.put(columnName, "number");
            break;
          case STRING:
            schemaMap.put(columnName, "string");
            break;
          case ARRAY:
            schemaMap.put(columnName, "array");
            break;
          default:
            schemaMap.put(columnName, "variant");
        }
      }
    }
    // when no schema is retrieved we will get an empty map, and we will error out when try to use
    // it to create the table
    return schemaMap;
  }

  /**
   * From the connector config extract whether the avro value converter is used
   *
   * @param connectorConfig the connnector configuration
   * @return whether the avro value converter is used
   */
  public static boolean usesAvroValueConverter(final Map<String, String> connectorConfig) {
    List<String> validAvroConverter = new ArrayList<>();
    validAvroConverter.add(SnowflakeSinkConnectorConfig.CONFLUENT_AVRO_CONVERTER);
    if (connectorConfig.containsKey(SnowflakeSinkConnectorConfig.VALUE_CONVERTER_CONFIG_FIELD)) {
      String valueConverter =
          connectorConfig.get(SnowflakeSinkConnectorConfig.VALUE_CONVERTER_CONFIG_FIELD);
      return validAvroConverter.contains(valueConverter);
    }
    return false;
  }

  /**
   * Get the schema for the table from topics.
   *
   * <p>Topics will be collected from topicToTableMap. When topicToTableMap is empty the topic
   * should be the same as the tableName
   *
   * @param tableName the name of the table
   * @param topicToTableMap the mapping from topic to table, might be empty
   * @param schemaRegistryURL the URL to the schema registry
   * @return the map from the columnName to their type
   */
  public static Map<String, String> getSchemaMapForTable(
      final String tableName,
      final Map<String, String> topicToTableMap,
      final String schemaRegistryURL) {
    return getSchemaMapForTableWithSchemaRegistryClient(
        tableName, topicToTableMap, getAvroSchemaRegistryClientFromURL(schemaRegistryURL));
  }

  /**
   * Get the schema for a specific topics.
   *
   * @param topic the name of the topic
   * @param schemaRegistryURL the URL to the schema registry
   * @return the map from the columnName to their type
   */
  public static Map<String, String> getSchemaMapForTopic(
      final String topic, final String schemaRegistryURL) {
    return SchematizationUtils.getAvroSchemaFromSchemaRegistryClient(
        topic, getAvroSchemaRegistryClientFromURL(schemaRegistryURL), "value");
  }

  /**
   * Get the schema for the table from topics.
   *
   * <p>Topics will be collected from topicToTableMap. When topicToTableMap is empty the topic
   * should be the same as the tableName
   *
   * @param tableName the name of the table
   * @param topicToTableMap the mapping from topic to table, might be empty
   * @param schemaRegistry the schema registry client
   * @return the map from the columnName to their type
   */
  public static Map<String, String> getSchemaMapForTableWithSchemaRegistryClient(
      final String tableName,
      final Map<String, String> topicToTableMap,
      final SchemaRegistryClient schemaRegistry) {
    Map<String, String> schemaMap = new HashMap<>();
    if (!topicToTableMap.isEmpty()) {
      for (String topic : topicToTableMap.keySet()) {
        if (topicToTableMap.get(topic).equals(tableName)) {
          Map<String, String> tempMap =
              SchematizationUtils.getAvroSchemaFromSchemaRegistryClient(
                  topic, schemaRegistry, "value");
          schemaMap.putAll(tempMap);
        }
      }
    } else {
      // if topic is not present in topic2table map, the table name must be the same with the
      // topic
      schemaMap =
          SchematizationUtils.getAvroSchemaFromSchemaRegistryClient(
              tableName, schemaRegistry, "value");
    }
    return schemaMap;
  }

  /**
   * Collect the extra column from the error message and their types from either the record or from
   * schema fetched from schema registry
   *
   * @param recordMap the record body
   * @param message the error message in the response
   * @param schemaMap the schema map from schema registry, could be empty
   * @return the map from columnNames to their types
   */
  public static Map<String, String> collectExtraColumnToType(
      Map<String, Object> recordMap, String message, Map<String, String> schemaMap) {
    Map<String, String> extraColumnToType = new HashMap<>();
    List<String> columnNames = new ArrayList<>();
    boolean deprecated_behavior = false;
    if (message.contains(EXTRA_COLUMNS_PREFIX)) {
      int startIndex = message.indexOf(EXTRA_COLUMNS_PREFIX) + EXTRA_COLUMNS_PREFIX.length();
      int endIndex = message.indexOf(EXTRA_COLUMNS_SUFFIX);
      columnNames = getColumnNamesFromMessage(message.substring(startIndex, endIndex));
    } else if (message.contains(DEPRECATED_EXTRA_COLUMNS_PREFIX)) {
      // TODO: remove deprecated behavior once new SDK version is released
      int startIndex =
          message.indexOf(DEPRECATED_EXTRA_COLUMNS_PREFIX)
              + DEPRECATED_EXTRA_COLUMNS_PREFIX.length();
      int endIndex = message.indexOf(EXTRA_COLUMNS_SUFFIX);
      columnNames.add(message.substring(startIndex, endIndex));
      // columnName extracted from message is AFTER formatColumnName in the deprecated version of
      // SDK
      deprecated_behavior = true;
    } else {
      // return empty map
      return extraColumnToType;
    }

    for (String columnName : columnNames) {
      if (!extraColumnToType.containsKey(columnName)) {
        String type;
        if (schemaMap.isEmpty()) {
          // no schema from schema registry
          if (!deprecated_behavior) {
            type = getTypeFromJsonObject(recordMap.get(columnName));
          } else {
            Object value = null;
            for (String colName : recordMap.keySet()) {
              if (formatColumnName(colName).equals(columnName)) {
                value = recordMap.get(colName);
                break;
              }
            }
            type = getTypeFromJsonObject(value);
          }
        } else {
          type = null;
          if (!deprecated_behavior) {
            type = schemaMap.get(columnName);
          } else {
            for (String colName : schemaMap.keySet()) {
              if (formatColumnName(colName).equals(columnName)) {
                type = schemaMap.get(colName);
                break;
              }
            }
          }
          if (type == null) {
            type = "VARIANT";
          }
        }
        extraColumnToType.put(columnName, type);
      }
    }
    return extraColumnToType;
  }

  /**
   * Collect the non-nullable columns from error message
   *
   * @param message error message
   * @return a list of columnNames of non-nullable columns
   */
  public static List<String> collectNonNullableColumns(String message) {
    List<String> nonNullableColumns = new ArrayList<>();
    if (message.contains(NONNULLABLE_COLUMNS_PREFIX)) {
      int startIndex =
          message.indexOf(NONNULLABLE_COLUMNS_PREFIX) + NONNULLABLE_COLUMNS_PREFIX.length();
      int endIndex = message.indexOf(NONNULLABLE_COLUMNS_SUFFIX);
      nonNullableColumns = getColumnNamesFromMessage(message.substring(startIndex, endIndex));
    } else if (message.contains(DEPRECATED_NONNULLABLE_COLUMNS_PREFIX)) {
      // TODO: remove deprecated behavior once new SDK version is released
      //  The recordMap arg should also be removed since it's only used here
      int startIndex =
          message.indexOf(DEPRECATED_NONNULLABLE_COLUMNS_PREFIX)
              + DEPRECATED_NONNULLABLE_COLUMNS_PREFIX.length();
      int endIndex = message.indexOf(NONNULLABLE_COLUMNS_SUFFIX);
      String columnName = message.substring(startIndex, endIndex);
      // need to find the columnName before formatColumnName
      // this is wrong when the columnName was enclosed in double quotes
      nonNullableColumns.add(columnName);
    }
    return nonNullableColumns;
  }

  // TODO: SNOW-649753 Directly get a list of columns from the response instead of parsing them from
  //  a string
  /**
   * extra a list of columnNames from their string representation
   *
   * <p>input: "["a", B]", output: [""a"", "B"]
   *
   * @param message part of the error message that contains a list of columnNames
   * @return the list of columnNames
   */
  @VisibleForTesting
  public static List<String> getColumnNamesFromMessage(String message) {
    List<String> columnNamesFromMessage = new ArrayList<>();
    String originalMessage = message;
    message = message.substring(1, message.length() - 1);
    // drop the square brackets
    while (message.contains(",")) {
      int newIndex;
      if (message.startsWith("\"")) {
        int nextQuoteIndex = message.substring(1).indexOf("\"") + 1;
        if (nextQuoteIndex == 0) {
          throw SnowflakeErrors.ERROR_5023.getException(
              String.format("ColumnNames String: %s", originalMessage));
        }
        // find the next quote rather than the next comma
        // because comma could be contained in the columnName
        String columnName = message.substring(0, nextQuoteIndex + 1);
        columnNamesFromMessage.add(columnName);
        newIndex = nextQuoteIndex + 3;
        // skip the quote, the comma and the space
      } else {
        // in this case the columnName must be separated by comma
        int nextCommaIndex = message.indexOf(",");
        if (nextCommaIndex == -1) {
          throw SnowflakeErrors.ERROR_5023.getException(
              String.format("ColumnNames String: %s", originalMessage));
        }
        String columnName = message.substring(0, nextCommaIndex);
        columnNamesFromMessage.add(columnName);
        newIndex = nextCommaIndex + 2;
        // skip the comma and the space
      }
      if (newIndex >= message.length()) {
        // parse finished early
        // can be caused by comma in the columnName
        return columnNamesFromMessage;
      } else {
        message = message.substring(newIndex);
      }
    }
    columnNamesFromMessage.add(message);
    return columnNamesFromMessage;
  }

  private static String getTypeFromJsonObject(Object value) {
    if (value == null) {
      return "VARIANT";
    }
    Type schemaType = ConnectSchema.schemaType(value.getClass());
    if (schemaType == null) {
      return "VARIANT";
    }
    switch (schemaType) {
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
        return "VARIANT";
    }
  }

  /**
   * Transform the columnName to uppercase unless it is enclosed in double quotes
   *
   * <p>In that case, drop the quotes and leave it as it is.
   *
   * <p>This transformation exist to mimic the behavior of the Ingest SDK.
   *
   * @param columnName the original name of the column
   * @return Transformed columnName
   */
  public static String formatColumnName(String columnName) {
    // the columnName has been checked and guaranteed not to be null or empty
    return (columnName.charAt(0) == '"' && columnName.charAt(columnName.length() - 1) == '"')
        ? columnName.substring(1, columnName.length() - 1)
        : columnName.toUpperCase();
  }

  /**
   * Transform the roleName to uppercase unless it is enclosed in double quotes
   *
   * <p>In that case, drop the quotes and leave it as it is.
   *
   * @param roleName name of the role
   * @return Transformed roleName
   */
  public static String formatRoleName(String roleName) {
    return (roleName.charAt(0) == '"' && roleName.charAt(roleName.length() - 1) == '"')
        ? roleName.substring(1, roleName.length() - 1)
        : roleName.toUpperCase();
  }
}
