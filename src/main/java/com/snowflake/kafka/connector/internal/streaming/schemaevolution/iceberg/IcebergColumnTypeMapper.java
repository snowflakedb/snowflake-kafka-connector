package com.snowflake.kafka.connector.internal.streaming.schemaevolution.iceberg;

import static com.snowflake.kafka.connector.internal.SnowflakeErrors.ERROR_5026;
import static org.apache.kafka.connect.data.Schema.Type.ARRAY;
import static org.apache.kafka.connect.data.Schema.Type.BOOLEAN;
import static org.apache.kafka.connect.data.Schema.Type.BYTES;
import static org.apache.kafka.connect.data.Schema.Type.FLOAT32;
import static org.apache.kafka.connect.data.Schema.Type.FLOAT64;
import static org.apache.kafka.connect.data.Schema.Type.INT64;
import static org.apache.kafka.connect.data.Schema.Type.STRING;
import static org.apache.kafka.connect.data.Schema.Type.STRUCT;

import com.fasterxml.jackson.databind.JsonNode;
import com.snowflake.kafka.connector.internal.SnowflakeErrors;
import com.snowflake.kafka.connector.internal.SnowflakeKafkaConnectorException;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;

class IcebergColumnTypeMapper {

  /**
   * See <a href="https://docs.snowflake.com/en/user-guide/tables-iceberg-data-types">Data types for
   * Apache Iceberg™ tables</a>
   */
  String mapToColumnTypeFromIcebergSchema(Type apacheIcebergType) {
    switch (apacheIcebergType.typeId()) {
      case BOOLEAN:
        return "BOOLEAN";
      case INTEGER:
        return "INT";
      case LONG:
        return "LONG";
      case FLOAT:
      case DOUBLE:
        return "DOUBLE";
      case DATE:
        return "DATE";
      case TIME:
        return "TIME(6)";
      case TIMESTAMP:
        Types.TimestampType timestamp = (Types.TimestampType) apacheIcebergType;
        return timestamp.shouldAdjustToUTC() ? "TIMESTAMP_LTZ" : "TIMESTAMP";
      case STRING:
        return "STRING";
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
        throw SnowflakeErrors.ERROR_5025.getException(
            "Data type: " + apacheIcebergType.typeId().name());
    }
  }

  /**
   * Method to convert datatype read from a record to column type used in Snowflake. This used for a
   * code path without available schema.
   *
   * <p>Converts Types from: JsonNode -> KafkaKafka -> Snowflake.
   */
  String mapToColumnTypeFromJson(String name, JsonNode value) {
    Schema.Type kafkaType = mapJsonNodeTypeToKafkaType(name, value);
    return mapToColumnTypeFromKafkaSchema(kafkaType, null);
  }

  String mapToColumnTypeFromKafkaSchema(Schema.Type kafkaType, String schemaName) {
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
        return "ARRAY";
      case STRUCT:
        return "OBJECT";
      case MAP:
        return "MAP";
      default:
        // todo try to throw info about a whole record - this is pure
        throw new IllegalArgumentException(
            "Error parsing datatype from Kafka record: " + kafkaType);
    }
  }

  /**
   * Map the JSON node type to Kafka type. For null and empty values, we can't infer the type, so we
   * throw an exception.
   *
   * @param name column/field name
   * @param value JSON node
   * @throws SnowflakeKafkaConnectorException if the value is null or empty array or empty object
   * @return Kafka type
   */
  Schema.Type mapJsonNodeTypeToKafkaType(String name, JsonNode value) {
    if (cannotInferType(value)) {
      throw ERROR_5026.getException("'" + name + "' field value is null or empty");
    }
    if (value.isNumber()) {
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

  boolean cannotInferType(JsonNode value) {
    // cannot infer type if value null or empty array or empty object
    return value == null
        || value.isNull()
        || (value.isArray() && value.isEmpty())
        || (value.isObject() && value.isEmpty());
  }
}
