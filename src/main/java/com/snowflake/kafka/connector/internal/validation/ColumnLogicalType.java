/*
 * Copyright (c) 2022-2024 Snowflake Computing Inc. All rights reserved.
 *
 * This file is extracted from the Snowflake Ingest SDK v1 (streaming/internal/AbstractRowBuffer.java)
 * to support client-side validation in Kafka Connector v4.
 */

package com.snowflake.kafka.connector.internal.validation;

/** Snowflake table column logical type */
public enum ColumnLogicalType {
  ANY,
  BOOLEAN(1),
  ROWINDEX,
  NULL(15),
  REAL(8),
  FIXED(2),
  TEXT(9),
  CHAR,
  BINARY(10),
  DATE(7),
  TIME(6),
  TIMESTAMP_LTZ(3),
  TIMESTAMP_NTZ(4),
  TIMESTAMP_TZ(5),
  INTERVAL,
  RAW,
  ARRAY(13, true),
  OBJECT(12, true),
  VARIANT(11, true),
  ROW,
  SEQUENCE,
  FUNCTION,
  USER_DEFINED_TYPE,
  ;

  private static final int INVALID_SERVER_SIDE_DATA_TYPE_ORDINAL = -1;

  // ordinal should be in sync with the server side scanner
  private final int ordinal;
  // whether it is a composite data type: array, object or variant
  private final boolean object;

  ColumnLogicalType() {
    // no valid server side ordinal by default
    this(INVALID_SERVER_SIDE_DATA_TYPE_ORDINAL);
  }

  ColumnLogicalType(int ordinal) {
    this(ordinal, false);
  }

  ColumnLogicalType(int ordinal, boolean object) {
    this.ordinal = ordinal;
    this.object = object;
  }

  /**
   * Ordinal to encode the data type for the server side scanner
   *
   * <p>currently used for Parquet format
   */
  public int getOrdinal() {
    return ordinal;
  }

  /** Whether the data type is a composite type: OBJECT, VARIANT, ARRAY. */
  public boolean isObject() {
    return object;
  }
}
