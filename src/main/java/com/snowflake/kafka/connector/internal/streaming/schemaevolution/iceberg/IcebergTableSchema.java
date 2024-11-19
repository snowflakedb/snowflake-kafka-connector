package com.snowflake.kafka.connector.internal.streaming.schemaevolution.iceberg;

import com.google.common.collect.ImmutableList;
import java.util.Collections;
import java.util.List;

/** Wrapper for multiple columns, not necessary all columns that are in the table */
class IcebergTableSchema {

  private final List<IcebergColumnTree> columns;

  public IcebergTableSchema(List<IcebergColumnTree> columns) {
    this.columns = Collections.unmodifiableList(columns);
  }

  public List<IcebergColumnTree> getColumns() {
    return columns;
  }

  public static IcebergTableSchema Empty() {
    return new IcebergTableSchema(ImmutableList.of());
  }
}
