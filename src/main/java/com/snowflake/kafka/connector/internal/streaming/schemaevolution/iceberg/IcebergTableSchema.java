package com.snowflake.kafka.connector.internal.streaming.schemaevolution.iceberg;

import java.util.List;

/** Wrapper for multiple columns, not necessary all columns that are in the table */
public class IcebergTableSchema {

  private List<IcebergColumnTree> columns;
}
