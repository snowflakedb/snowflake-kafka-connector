package com.snowflake.kafka.connector.internal.streaming.schemaevolution.iceberg;

import com.google.common.collect.ImmutableList;
import java.util.List;

/** Wrapper for multiple columns, not necessary all columns that are in the table */
public class IcebergTableSchema {

  private List<IcebergColumnTree> columns;

  public IcebergTableSchema(List<IcebergColumnTree> columns) {
    this.columns = columns;
  }

  public static IcebergTableSchema Empty() {
    return new IcebergTableSchema(ImmutableList.of());
  }
}
