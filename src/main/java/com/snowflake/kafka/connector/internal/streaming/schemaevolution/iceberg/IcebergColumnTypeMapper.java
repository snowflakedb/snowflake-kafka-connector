package com.snowflake.kafka.connector.internal.streaming.schemaevolution.iceberg;

import static org.apache.kafka.connect.data.Schema.Type.ARRAY;
import static org.apache.kafka.connect.data.Schema.Type.BOOLEAN;
import static org.apache.kafka.connect.data.Schema.Type.BYTES;
import static org.apache.kafka.connect.data.Schema.Type.FLOAT32;
import static org.apache.kafka.connect.data.Schema.Type.FLOAT64;
import static org.apache.kafka.connect.data.Schema.Type.INT64;
import static org.apache.kafka.connect.data.Schema.Type.STRING;
import static org.apache.kafka.connect.data.Schema.Type.STRUCT;

import com.fasterxml.jackson.databind.JsonNode;
import com.snowflake.kafka.connector.internal.streaming.schemaevolution.ColumnTypeMapper;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;

public class IcebergColumnTypeMapper extends ColumnTypeMapper {

  /**
   * See <a href="https://docs.snowflake.com/en/user-guide/tables-iceberg-data-types">Data types for
   * Apache Iceberg™ tables</a>
   */
  public static String mapToSnowflakeDataType(Type apacheIcebergType) {
    switch (apacheIcebergType.typeId()) {
      case BOOLEAN:
        return "BOOLEAN";
      case INTEGER:
        return "NUMBER(10,0)";
      case LONG:
        return "NUMBER(19,0)";
      case FLOAT:
      case DOUBLE:
        return "FLOAT";
      case DATE:
        return "DATE";
      case TIME:
        return "TIME(6)";
      case TIMESTAMP:
        Types.TimestampType timestamp = (Types.TimestampType) apacheIcebergType;
        return timestamp.shouldAdjustToUTC() ? "TIMESTAMP_LTZ" : "TIMESTAMP";
      case STRING:
        return "VARCHAR(16777216)";
      case UUID:
        return "BINARY(16)";
      case FIXED:
        throw new IllegalArgumentException("FIXED column type not supported!");
      case BINARY:
        return "BINARY";
      case DECIMAL:
        Types.DecimalType decimal = (Types.DecimalType) apacheIcebergType;
        return decimal.toString().toUpperCase();
      case STRUCT:
        return "OBJECT";
      case LIST:
        return "ARRAY";
      case MAP:
        return "MAP";
      default:
        throw new IllegalArgumentException(
            "Fail unsupported datatype! - " + apacheIcebergType.typeId());
    }
  }

  @Override
  public String mapToColumnType(Schema.Type kafkaType, String schemaName) {
    switch (kafkaType) {
      case INT8:
      case INT16:
        return "INT";
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
          return "LONG";
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
      default:
        // MAP and STRUCT will go here
        throw new IllegalArgumentException("Arrays, struct and map not supported!");
    }
  }

  /**
   * Map the JSON node type to Kafka type
   *
   * @param value JSON node
   * @return Kafka type
   */
  @Override
  public Schema.Type mapJsonNodeTypeToKafkaType(JsonNode value) {
    if (value == null || value.isNull()) {
      return STRING;
    } else if (value.isNumber()) {
      if (value.isFloat()) {
        return FLOAT32;
      } else if (value.isDouble()) {
        return FLOAT64;
      }
      return INT64; // short, int, long we treat as 64-bit numbers as from the value we can't infer
      // smaller types
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
}
