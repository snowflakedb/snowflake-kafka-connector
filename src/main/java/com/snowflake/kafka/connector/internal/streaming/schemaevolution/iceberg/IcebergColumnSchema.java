package com.snowflake.kafka.connector.internal.streaming.schemaevolution.iceberg;

import org.apache.iceberg.types.Type;

/** Wrapper class for Iceberg schema retrieved from channel. */
class IcebergColumnSchema {

  private final Type schema;

  private final String columnName;

  IcebergColumnSchema(Type schema, String columnName) {
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
