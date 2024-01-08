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

import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.internal.SnowflakeConnectionService;
import com.snowflake.kafka.connector.internal.SnowflakeErrors;
import com.snowflake.kafka.connector.internal.SnowflakeKafkaConnectorException;
import com.snowflake.kafka.connector.records.RecordService;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;

import net.snowflake.client.jdbc.internal.fasterxml.jackson.core.JsonProcessingException;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.JsonNode;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
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
  public static boolean evolveSchemaIfNeeded(
      @Nonnull SnowflakeConnectionService conn,
      String tableName,
      List<String> nonNullableColumns,
      List<String> extraColNames,
      SinkRecord record) {
    boolean changesApplied = false;
    // Update nullability if needed, ignore any exceptions since other task might be succeeded
    if (nonNullableColumns != null) {
      try {
        conn.alterNonNullableColumns(tableName, nonNullableColumns);
        changesApplied = true;
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
//        TODO
//        Really we only want to ignoreSchema when we're dealing with nested Protobuf schemas, but as thats our only use case
//        right now, and working out if we're dealing with a nested protobuf schema is quite difficult, i'm gonna do this for now

      Map<String, String> extraColumnsToType = getColumnTypes(record, extraColNames, true);
      LOGGER.info("CAFLOG3 ||| {} ||| {} ||| {}", extraColumnsToType, record, extraColNames);
      try {
        if (extraColumnsToType.size() > 0) {
          conn.appendColumnsToTable(tableName, extraColumnsToType);
          changesApplied = true;
        }
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
    return changesApplied;
  }

  /**
   * With the list of columns, collect their data types from either the schema or the data itself
   *
   * @param record the sink record that contains the schema and actual data
   * @param columnNames the names of the extra columns
   * @return a Map object where the key is column name and value is Snowflake data type
   */
//  TODO: CDP-2873
  static Map<String, String> getColumnTypes(SinkRecord record, List<String> columnNames) {
    if (columnNames == null) {
      return new HashMap<>();
    }
    Map<String, String> schemaMap = getSchemaMapFromRecord(record);
    JsonNode recordNode = RecordService.convertToJson(record.valueSchema(), record.value(), true);
    Set<String> columnNamesSet = new HashSet<>(columnNames);

    return parseColumnTypes(recordNode, columnNamesSet, schemaMap);
  }

  static Map<String, String> getColumnTypes(SinkRecord record, List<String> columnNames, boolean ignoreSchema) {
    if (columnNames == null) {
      return new HashMap<>();
    }

    Map<String, String> schemaMap = (ignoreSchema ? new HashMap<>() : getSchemaMapFromRecord(record));
    JsonNode recordNode = RecordService.convertToJson((ignoreSchema ? null : record.valueSchema()), record.value());
    Set<String> columnNamesSet = new HashSet<>(columnNames);
    LOGGER.info("changes deployed - " + schemaMap + "::::" + ignoreSchema + "::::" + recordNode);

    return parseColumnTypes(recordNode, columnNamesSet, schemaMap);
  }

  private static Map<String, String> parseColumnTypes(JsonNode recordNode, Set<String> columnNamesSet, Map<String, String> schemaMap) {
    Map<String, String> columnToType = new HashMap<>();
    Iterator<Map.Entry<String, JsonNode>> fields = recordNode.fields();
    while (fields.hasNext()) {
      Map.Entry<String, JsonNode> field = fields.next();
      String colName = Utils.quoteNameIfNeeded(field.getKey());
      if (columnNamesSet.contains(field.getKey())) {
        String type;
        if (schemaMap.isEmpty()) {
          // No schema associated with the record, we will try to infer it based on the data
          type = inferDataTypeFromJsonObject(field.getValue());
        } else {
          // Get from the schema
          type = schemaMap.get(field.getKey());
          if (type == null) {
            // only when the type of the value is unrecognizable for JAVA
            throw SnowflakeErrors.ERROR_5022.getException(
                    "column: " + field.getKey() + " schemaMap: " + schemaMap);
          }
        }
        columnToType.put(colName, type);
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
    if (schema != null && schema.fields() != null) {
      for (Field field : schema.fields()) {
        String snowflakeType = convertToSnowflakeType(field.schema().type(), field.schema().name());
        LOGGER.info(
            "Got the snowflake data type for field:{}, schemaName:{}, kafkaType:{},"
                + " snowflakeType:{}",
            field.name(),
            field.schema().name(),
            field.schema().type(),
            snowflakeType);
        schemaMap.put(field.name(), snowflakeType);
      }
    }
    return schemaMap;
  }

  /** Try to infer the data type from the data */
  private static String inferDataTypeFromJsonObject(JsonNode value) {
    Type schemaType = convertJsonNodeTypeToKafkaType(value);
//    if (schemaType == null) {
//      // only when the type of the value is unrecognizable for JAVA
//      throw SnowflakeErrors.ERROR_5021.getException("class: " + value.getClass());
//    }
    return schemaType != null ? convertToSnowflakeType(schemaType) : null;
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
      try {
        if (new ObjectMapper().readTree(value.textValue()).isObject() ) {
          return STRUCT;
        } else {
          return STRING;
        }
      } catch(JsonProcessingException ignored) {}
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
  private static String convertToSnowflakeType(Type kafkaType, String schemaName) {
    switch (kafkaType) {
      case INT8:
        return "BYTEINT";
      case INT16:
        return "SMALLINT";
      case INT32:
        if (Date.LOGICAL_NAME.equals(schemaName)) {
          return "DATE";
        } else if (Time.LOGICAL_NAME.equals(schemaName)) {
          return "TIME(6)";
        } else {
          return "INT";
        }
      case INT64:
        if (Timestamp.LOGICAL_NAME.equals(schemaName)) {
          return "TIMESTAMP(6)";
        } else {
          return "BIGINT";
        }
      case FLOAT32:
        return "FLOAT";
      case FLOAT64:
        return "DOUBLE";
      case BOOLEAN:
        return "BOOLEAN";
      case STRING:
        return "VARCHAR";
      case BYTES:
        if (Decimal.LOGICAL_NAME.equals(schemaName)) {
          return "VARCHAR";
        } else {
          return "BINARY";
        }
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
