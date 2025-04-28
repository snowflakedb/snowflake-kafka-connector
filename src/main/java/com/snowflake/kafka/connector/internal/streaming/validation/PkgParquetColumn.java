/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

package com.snowflake.kafka.connector.internal.streaming.validation;

import org.apache.parquet.schema.Type;

/** Represents a column in a Parquet file. */
class PkgParquetColumn {
  final ColumnMetadata columnMetadata;
  final int index;
  final Type type;

  PkgParquetColumn(ColumnMetadata columnMetadata, int index, Type type) {
    this.columnMetadata = columnMetadata;
    this.index = index;
    this.type = type;
  }
}
