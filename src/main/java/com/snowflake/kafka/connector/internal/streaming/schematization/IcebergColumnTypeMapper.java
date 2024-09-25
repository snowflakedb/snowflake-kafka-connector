package com.snowflake.kafka.connector.internal.streaming.schematization;

import org.apache.kafka.connect.data.Schema;

public class IcebergColumnTypeMapper extends ColumnTypeMapper {
  @Override
  public String mapToColumnType(Schema.Type kafkaType, String schemaName) {
    // TODO implement this in SNOW-1665417
    throw new IllegalStateException("Not implemented yet");
  }
}
