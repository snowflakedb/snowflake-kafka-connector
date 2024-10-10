package com.snowflake.kafka.connector.internal.streaming.schemaevolution.iceberg;

import com.snowflake.kafka.connector.internal.streaming.schemaevolution.ColumnTypeMapper;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;

public class IcebergColumnTypeMapper extends ColumnTypeMapper {
  @Override
  public String mapToColumnType(Schema.Type kafkaType, String schemaName) {
    switch (kafkaType) {
      case INT8:
      case INT16:
        return "BIGINT";
      case INT32:
        if (Date.LOGICAL_NAME.equals(schemaName)) {
          return "DATE";
        } else if (Time.LOGICAL_NAME.equals(schemaName)) {
          return "TIME(6)";
        } else {
          return "BIGINT";
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
      default:
        // MAP and STRUCT will go here
        throw new IllegalStateException("Arrays, struct and map not supported!");
    }
  }
}
