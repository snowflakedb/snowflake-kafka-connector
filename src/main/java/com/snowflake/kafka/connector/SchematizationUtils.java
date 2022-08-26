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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** This is a class containing the helper functions related to schematization */
public class SchematizationUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(SchematizationUtils.class);

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
      LOGGER.info("Schema with subject '{}' not found in schema registry", subjectName);
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
   * With the list of extra columns, collect their types from either the record or from schema
   * fetched from schema registry
   *
   * @param recordMap the record body
   * @param columnNames the names of the extra columns
   * @param schemaMap the schema map from schema registry, could be empty
   * @return the map from columnNames to their types
   */
  public static Map<String, String> collectExtraColumnToType(
      Map<String, Object> recordMap, List<String> columnNames, Map<String, String> schemaMap) {
    if (columnNames == null) {
      return new HashMap<>();
    }
    Map<String, String> extraColumnToType = new HashMap<>();

    for (String columnName : columnNames) {
      if (!extraColumnToType.containsKey(columnName)) {
        String type;
        if (schemaMap.isEmpty()) {
          // no schema from schema registry
          type = getTypeFromJsonObject(recordMap.get(columnName));
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
      throw SnowflakeErrors.ERROR_5023.getException();
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
