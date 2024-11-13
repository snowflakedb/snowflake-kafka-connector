package com.snowflake.kafka.connector.internal.streaming.schemaevolution.iceberg;

import org.apache.iceberg.types.Type;

/** Wrapper class for Iceberg schema retrieved from channel. */
public class ApacheIcebergColumnSchema {

  private final Type schema;

  private final String columnName;

  public ApacheIcebergColumnSchema(Type schema, String columnName) {
    this.schema = schema;
    this.columnName = columnName.toUpperCase();
  }

  public Type getSchema() {
    return schema;
  }

  public String getColumnName() {
    return columnName;
  }
}
