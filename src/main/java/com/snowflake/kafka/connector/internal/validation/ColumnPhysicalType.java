/*
 * COPIED FROM SNOWFLAKE INGEST SDK V1
 * Source: snowflake-ingest-java/src/main/java/net/snowflake/ingest/streaming/internal/AbstractRowBuffer.java
 *
 * Modifications:
 * - Extracted ColumnPhysicalType enum from AbstractRowBuffer class
 * - Package changed to com.snowflake.kafka.connector.internal.validation
 *
 * Copyright (c) 2022-2024 Snowflake Computing Inc. All rights reserved.
 */

package com.snowflake.kafka.connector.internal.validation;

/** Snowflake table column physical type */
public enum ColumnPhysicalType {
  ROWINDEX(9),
  DOUBLE(7),
  SB1(1),
  SB2(2),
  SB4(3),
  SB8(4),
  SB16(5),
  LOB(8),
  BINARY,
  ROW(10),
  ;

  private static final int INVALID_SERVER_SIDE_DATA_TYPE_ORDINAL = -1;

  // ordinal should be in sync with the server side scanner
  private final int ordinal;

  ColumnPhysicalType() {
    // no valid server side ordinal by default
    this(INVALID_SERVER_SIDE_DATA_TYPE_ORDINAL);
  }

  ColumnPhysicalType(int ordinal) {
    this.ordinal = ordinal;
  }

  /**
   * Ordinal to encode the data type for the server side scanner
   *
   * <p>currently used for Parquet format
   */
  public int getOrdinal() {
    return ordinal;
  }
}
