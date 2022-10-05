/*
 * Copyright (c) 2022 Snowflake Computing Inc. All rights reserved.
 */

package com.snowflake.kafka.connector.internal.streaming;

import com.snowflake.kafka.connector.internal.SnowflakeConnectionService;
import com.snowflake.kafka.connector.internal.SnowflakeErrors;
import com.snowflake.kafka.connector.internal.SnowflakeKafkaConnectorException;
import com.snowflake.kafka.connector.records.RecordService;
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

  /**
   * Execute a ALTER TABLE command if there is any extra column that needs to be added, or any
   * column nullability that needs to be updated, used by schema evolution
   *
   * @param conn connection to the Snowflake
   * @param tableName table name
   * @param nonNullableColumns a list of columns that needs to update the nullability
   * @param extraColNames a list of columns that needs to be updated
   * @param record the sink record that contains the schema and actual data
   */
  public static void alterTableIfNeeded(
      SnowflakeConnectionService conn,
      String tableName,
      List<String> nonNullableColumns,
      List<String> extraColNames,
      SinkRecord record) {
    // Update nullability if needed, ignore any exceptions since other task might be succeeded
    if (nonNullableColumns != null) {
      try {
        conn.alterNonNullableColumns(tableName, nonNullableColumns);
      } catch (SnowflakeKafkaConnectorException e) {
        LOGGER.warn(
            String.format("Failure altering table to update nullability: %s", tableName), e);
      }
    }

    // Add columns if needed, ignore any exceptions since other task might be succeeded
    if (extraColNames != null) {
      Map<String, String> extraColumnsToType = getColumnTypes(record, extraColNames);
      try {
        conn.appendColumnsToTable(tableName, extraColumnsToType);
      } catch (SnowflakeKafkaConnectorException e) {
        LOGGER.warn(String.format("Failure altering table to add column: %s", tableName), e);
      }
    }
  }

  /**
   * With the list of columns, collect their data types from either the schema or the data itself
   *
   * @param record the sink record that contains the schema and actual data
   * @param columnNames the names of the extra columns
   * @return a Map object where the key is column name and value is Snowflake data type
   */
  static Map<String, String> getColumnTypes(SinkRecord record, List<String> columnNames) {
    if (columnNames == null) {
      return new HashMap<>();
    }
    Map<String, String> columnToType = new HashMap<>();
    Map<String, String> schemaMap = getSchemaMapFromRecord(record);
    JsonNode recordNode = RecordService.convertToJson(record.valueSchema(), record.value());

    for (String columnName : columnNames) {
      if (!columnToType.containsKey(columnName)) {
        String type;
        if (schemaMap.isEmpty()) {
          // No schema associated with the record, we will try to infer it based on the data
          Object value = recordNode.get(columnName);
          if (value == null) {
            LOGGER.info(
                "The corresponding value is null for the given column: {}, so infer to VARIANT type",
                columnName);
            type = "VARIANT";
          } else {
            type = inferDataTypeFromJsonObject(recordNode.get(columnName));
          }
        } else {
          // Get from the schema
          type = schemaMap.get(columnName);
        }
        columnToType.put(columnName, type);
      }
    }
    return columnToType;
  }

  /**
   * Given a SinkRecord, get the schema information from it
   *
   * @param record the sink record that contains the schema and actual data
   * @return a Map object where the key is column name and value is Snowflake data type
   */
  private static Map<String, String> getSchemaMapFromRecord(SinkRecord record) {
    Map<String, String> schemaMap = new HashMap<>();
    Schema schema = record.valueSchema();
    if (schema != null) {
      for (Field field : schema.fields()) {
        schemaMap.put(field.name(), convertToSnowflakeType(field.schema().type()));
      }
    }
    return schemaMap;
  }

  /** Try to infer the data type from the data */
  private static String inferDataTypeFromJsonObject(Object value) {
    Type schemaType = ConnectSchema.schemaType(value.getClass());
    if (schemaType == null) {
      // only when the type of the value is unrecognizable for JAVA
      throw SnowflakeErrors.ERROR_5021.getException("class: " + value.getClass());
    }
    return convertToSnowflakeType(schemaType);
  }

  /** Convert the kafka data type to Snowflake data type */
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
        LOGGER.debug(
            "The corresponding kafka type is {}, so infer to VARIANT type", kafkaType.getName());
        return "VARIANT";
    }
  }
}
