/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

package com.snowflake.kafka.connector.internal.streaming.validation;

/** Parquet internal value representation for buffering. */
class PkgParquetBufferValue {
  // Parquet uses BitPacking to encode boolean, hence 1 bit per value
  public static final float BIT_ENCODING_BYTE_LEN = 1.0f / 8;

  /**
   * On average parquet needs 2 bytes / 8 values for the RLE+bitpack encoded definition and
   * repetition level.
   *
   * <ul>
   *   There are two cases how definition and repetition level (0 for null values, 1 for non-null
   *   values) is encoded:
   *   <li>If there are at least 8 repeated values in a row, they are run-length encoded (length +
   *       value itself). E.g. 11111111 -> 8 1
   *   <li>If there are less than 8 repeated values, they are written in group as part of a
   *       bit-length encoded run, e.g. 1111 -> 15 A bit-length encoded run ends when either 64
   *       groups of 8 values have been written or if a new RLE run starts.
   *       <p>To distinguish between RLE and bitpack run, there is 1 extra bytes written as header
   *       when a bitpack run starts.
   * </ul>
   *
   * <ul>
   *   For more details see ColumnWriterV1#createDLWriter and {@link
   *   org.apache.parquet.column.values.rle.RunLengthBitPackingHybridEncoder#writeInt(int)}
   * </ul>
   */
  public static final float DEFINITION_LEVEL_ENCODING_BYTE_LEN = 2.0f / 8;

  public static final float REPETITION_LEVEL_ENCODING_BYTE_LEN = 2.0f / 8;

  // Parquet stores length in 4 bytes before the actual data bytes
  public static final int BYTE_ARRAY_LENGTH_ENCODING_BYTE_LEN = 4;
  private final Object value;
  private final float size;

  PkgParquetBufferValue(Object value, float size) {
    this.value = value;
    this.size = size;
  }

  Object getValue() {
    return value;
  }

  float getSize() {
    return size;
  }
}
