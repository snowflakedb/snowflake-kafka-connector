package com.snowflake.kafka.connector.internal.streaming.schemaevolution.iceberg;

import org.apache.iceberg.types.Type;

/** Wrapper class for Iceberg schema retrieved from channel. */
class ApacheIcebergColumnSchema {

  private final Type schema;

  private final String columnName;

  ApacheIcebergColumnSchema(Type schema, String columnName) {
    this.schema = schema;
    this.columnName = columnName;
  }

  Type getSchema() {
    return schema;
  }

  String getColumnName() {
    return columnName;
  }
}
