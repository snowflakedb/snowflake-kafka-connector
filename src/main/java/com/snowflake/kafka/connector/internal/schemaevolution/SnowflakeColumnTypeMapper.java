/*
 * Copyright (c) 2026 Snowflake Computing Inc. All rights reserved.
 *
 * Ported from KC v3.2 for client-side schema evolution in KC v4.
 */

package com.snowflake.kafka.connector.internal.schemaevolution;

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

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Maps Kafka Connect types to Snowflake DDL types.
 *
 * <p>Type mappings:
 * - INT8 → BYTEINT, INT16 → SMALLINT, INT32 → INT
 * - INT64 → BIGINT (or TIMESTAMP with logical name)
 * - FLOAT32 → FLOAT, FLOAT64 → DOUBLE
 * - BOOLEAN → BOOLEAN, STRING → VARCHAR
 * - BYTES → BINARY (or VARCHAR for Decimal)
 * - ARRAY → ARRAY, STRUCT/MAP → VARIANT
 */
public class SnowflakeColumnTypeMapper extends ColumnTypeMapper {

  private static final Logger LOGGER = LoggerFactory.getLogger(SnowflakeColumnTypeMapper.class);

  @Override
  public String mapToColumnType(Schema.Type kafkaType, String schemaName) {
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

  @Override
  public Schema.Type mapJsonNodeTypeToKafkaType(JsonNode value) {
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

  @Override
  public String inferTypeFromJavaValue(Object value) {
    if (value == null) {
      return "VARCHAR";
    } else if (value instanceof Short) {
      return "SMALLINT";
    } else if (value instanceof Integer) {
      return "INT";
    } else if (value instanceof Long) {
      return "BIGINT";
    } else if (value instanceof Float) {
      return "FLOAT";
    } else if (value instanceof Double) {
      return "DOUBLE";
    } else if (value instanceof Number) {
      return "BIGINT";
    } else if (value instanceof Boolean) {
      return "BOOLEAN";
    } else if (value instanceof String) {
      return "VARCHAR";
    } else if (value instanceof byte[]) {
      return "BINARY";
    } else if (value instanceof java.util.Collection) {
      return "ARRAY";
    } else if (value instanceof java.util.Map) {
      return "VARIANT";
    } else {
      LOGGER.debug(
          "Unknown Java type {}, defaulting to VARIANT", value.getClass().getName());
      return "VARIANT";
    }
  }
}
