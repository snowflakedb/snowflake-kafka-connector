/*
 * Copyright (c) 2022 Snowflake Computing Inc. All rights reserved.
 */

package com.snowflake.kafka.connector.internal.streaming;

import static org.apache.kafka.connect.data.Schema.Type.ARRAY;
import static org.apache.kafka.connect.data.Schema.Type.BOOLEAN;
import static org.apache.kafka.connect.data.Schema.Type.BYTES;
import static org.apache.kafka.connect.data.Schema.Type.FLOAT32;
import static org.apache.kafka.connect.data.Schema.Type.FLOAT64;
import static org.apache.kafka.connect.data.Schema.Type.INT16;
import static org.apache.kafka.connect.data.Schema.Type.INT32;
import static org.apache.kafka.connect.data.Schema.Type.INT64;
import static org.apache.kafka.connect.data.Schema.Type.STRING;
import static org.apache.kafka.connect.data.Schema.Type.STRUCT;

import com.snowflake.kafka.connector.internal.SnowflakeConnectionService;
import com.snowflake.kafka.connector.internal.SnowflakeErrors;
import com.snowflake.kafka.connector.internal.SnowflakeKafkaConnectorException;
import com.snowflake.kafka.connector.records.RecordService;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.JsonNode;

import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  public static void evolveSchemaIfNeeded(
      @Nonnull SnowflakeConnectionService conn,
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
            String.format(
                "Failure altering table to update nullability: %s, this could happen when multiple"
                    + " partitions try to alter the table at the same time and the warning could be"
                    + " ignored",
                tableName),
            e);
      }
    }

    // Add columns if needed, ignore any exceptions since other task might be succeeded
    if (extraColNames != null) {
      List<String> fieldNamesOrderedAsOnSource = Stream.concat(
          record.keySchema() != null ? record.keySchema().fields().stream().map(f -> f.name()) : Stream.empty(),
          record.valueSchema() != null ? record.valueSchema().fields().stream().map(f -> f.name())  : Stream.empty()
        ).collect(Collectors.toList());
      List<String> extraColNamesOrderedAsOnSource = new ArrayList<>(extraColNames);
      extraColNamesOrderedAsOnSource.sort(
        Comparator.comparingInt(fieldNamesOrderedAsOnSource::indexOf));
      Map<String, String> extraColumnsToType = getColumnTypes(record, extraColNamesOrderedAsOnSource);

      try {
        conn.appendColumnsToTable(tableName, extraColumnsToType);
      } catch (SnowflakeKafkaConnectorException e) {
        LOGGER.warn(
            String.format(
                "Failure altering table to add column: %s, this could happen when multiple"
                    + " partitions try to alter the table at the same time and the warning could be"
                    + " ignored",
                tableName),
            e);
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
    Map<String, String> columnToType = new LinkedHashMap<>();
    Map<String, String> schemaMap = getSchemaMapFromRecord(record);
    JsonNode recordNode = RecordService.convertToJson(record.valueSchema(), record.value());

    for (String columnName : columnNames) {
      if (!columnToType.containsKey(columnName)) {
        String type;
        if (schemaMap.isEmpty()) {
          // No schema associated with the record, we will try to infer it based on the data
          type = inferDataTypeFromJsonObject(recordNode.get(columnName));
        } else {
          // Get from the schema
          type = schemaMap.get(columnName);
          if (type == null) {
            // only when the type of the value is unrecognizable for JAVA
            throw SnowflakeErrors.ERROR_5022.getException(
                "column: " + columnName + " schemaMap: " + schemaMap);
          }
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
        schemaMap.put(field.name(), convertToSnowflakeType(field.schema().type(), field.schema().name()));
      }
    }
    return schemaMap;
  }

  /** Try to infer the data type from the data */
  private static String inferDataTypeFromJsonObject(JsonNode value) {
    Type schemaType = convertJsonNodeTypeToKafkaType(value);
    if (schemaType == null) {
      // only when the type of the value is unrecognizable for JAVA
      throw SnowflakeErrors.ERROR_5021.getException("class: " + value.getClass());
    }
    return convertToSnowflakeType(schemaType, null);
  }

  /** Convert a json node type to kafka data type */
  private static Type convertJsonNodeTypeToKafkaType(JsonNode value) {
    if (value == null || value.isNull()) {
      return STRING;
    } else if (value.isNumber()) {
      if (value.isShort()) {
        return INT16;
      } else if (value.isInt()) {
        return INT32;
      } else if (value.isFloat()) {
        return FLOAT32;
      } else if (value.isDouble()) {
        return FLOAT64;
      }
      return INT64;
    } else if (value.isTextual()) {
      return STRING;
    } else if (value.isBoolean()) {
      return BOOLEAN;
    } else if (value.isBinary()) {
      return BYTES;
    } else if (value.isArray()) {
      return ARRAY;
    } else if (value.isObject()) {
      return STRUCT;
    } else {
      return null;
    }
  }

  /** Convert the kafka data type to Snowflake data type */
  private static String convertToSnowflakeType(Type kafkaType, String semanticType) {
      if (semanticType != null) {
        switch (semanticType) {
            case Decimal.LOGICAL_NAME:
              return "DOUBLE";
            case Time.LOGICAL_NAME:
            case "io.debezium.time.MicroTime":   
              return "TIME(6)";
            case "io.debezium.time.Time":             
              return "TIME(3)";
            case Timestamp.LOGICAL_NAME:
            case "io.debezium.time.ZonedTimestamp":
            case "io.debezium.time.ZonedTime":      // Snowflake doesn't have zoned 'time-only' data types
            case "io.debezium.time.Timestamp":
            case "io.debezium.time.MicroTimestamp":
              return "TIMESTAMP";
            case Date.LOGICAL_NAME:
            case "io.debezium.time.Date":
              return "DATE";
        }
    }

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
        if (semanticType != null && semanticType.equals("io.debezium.data.Json")) {
          return "VARIANT";
        }
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
