/*
 * Copyright (c) 2022-2024 Snowflake Computing Inc. All rights reserved.
 */

package com.snowflake.kafka.connector.internal.streaming.validation;

import net.snowflake.ingest.utils.Utils;
import org.apache.parquet.schema.PrimitiveType;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.time.ZoneId;
import java.util.Optional;

/** Parses a user Snowflake column value into Parquet internal representation for buffering. */
class PkgParquetValueParserSnowflake {

  /**
   * Parses a user column value into Parquet internal representation for buffering.
   *
   * @param value column value provided by user in a row
   * @param columnMetadata column metadata
   * @param typeName Parquet primitive type name
   * @param insertRowsCurrIndex Row index corresponding the row to parse (w.r.t input rows in
   *     insertRows API, and not buffered row)
   * @return parsed value and byte size of Parquet internal representation
   */
  static PkgParquetBufferValue parseColumnValueToParquet(
      Object value,
      ColumnMetadata columnMetadata,
      PrimitiveType.PrimitiveTypeName typeName,
      ZoneId defaultTimezone,
      long insertRowsCurrIndex,
      boolean enableNewJsonParsingLogic) {
    float estimatedParquetSize = 0F;
    estimatedParquetSize += PkgParquetBufferValue.DEFINITION_LEVEL_ENCODING_BYTE_LEN;
    if (value != null) {
      ColumnLogicalType logicalType =
          ColumnLogicalType.valueOf(columnMetadata.getLogicalType());
      ColumnPhysicalType physicalType =
          ColumnPhysicalType.valueOf(columnMetadata.getPhysicalType());
      switch (typeName) {
        case BOOLEAN:
          int intValue =
              PkgDataValidationUtil.validateAndParseBoolean(
                  columnMetadata.getName(), value, insertRowsCurrIndex);
          value = intValue > 0;
          estimatedParquetSize += PkgParquetBufferValue.BIT_ENCODING_BYTE_LEN;
          break;
        case INT32:
            value = getInt32Value(
                columnMetadata.getName(),
                value,
                columnMetadata.getScale(),
                Optional.ofNullable(columnMetadata.getPrecision()).orElse(0),
                logicalType,
                physicalType,
                insertRowsCurrIndex);
          estimatedParquetSize += 4;
          break;
        case INT64:
            value = getInt64Value(
                columnMetadata.getName(),
                value,
                columnMetadata.getScale(),
                Optional.ofNullable(columnMetadata.getPrecision()).orElse(0),
                logicalType,
                physicalType,
                defaultTimezone,
                insertRowsCurrIndex);
          estimatedParquetSize += 8;
          break;
        case DOUBLE:
            value = PkgDataValidationUtil.validateAndParseReal(
                columnMetadata.getName(), value, insertRowsCurrIndex);
          estimatedParquetSize += 8;
          break;
        case BINARY:
          int length = 0;
          if (logicalType == ColumnLogicalType.BINARY) {
            value =
                getBinaryValueForLogicalBinary(value, columnMetadata, insertRowsCurrIndex);
            length = ((byte[]) value).length;
          } else {
            String str =
                getBinaryValue(
                    value, columnMetadata, insertRowsCurrIndex, enableNewJsonParsingLogic);
            value = str;
            if (str != null) {
              length = str.getBytes().length;
            }
          }
          if (value != null) {
            estimatedParquetSize +=
                (PkgParquetBufferValue.BYTE_ARRAY_LENGTH_ENCODING_BYTE_LEN + length);
          }

          break;
        case FIXED_LEN_BYTE_ARRAY:
          BigInteger intRep =
              getSb16Value(
                  columnMetadata.getName(),
                  value,
                  columnMetadata.getScale(),
                  Optional.ofNullable(columnMetadata.getPrecision()).orElse(0),
                  logicalType,
                  physicalType,
                  defaultTimezone,
                  insertRowsCurrIndex);
          value = getSb16Bytes(intRep);
          estimatedParquetSize += 16;
          break;
        default:
          throw new PkgSFException(
              ErrorCode.UNKNOWN_DATA_TYPE, columnMetadata.getName(), logicalType, physicalType);
      }
    }

    if (value == null) {
      if (!columnMetadata.getNullable()) {
        throw new PkgSFException(
            ErrorCode.INVALID_FORMAT_ROW,
            columnMetadata.getName(),
            String.format("Passed null to non nullable field, rowIndex:%d", insertRowsCurrIndex));
      }
    }

    return new PkgParquetBufferValue(value, estimatedParquetSize);
  }

  /**
   * Parses an int32 value based on Snowflake logical type.
   *
   * @param value column value provided by user in a row
   * @param scale data type scale
   * @param precision data type precision
   * @param logicalType Snowflake logical type
   * @param physicalType Snowflake physical type
   * @param insertRowsCurrIndex Used for logging the row of index given in insertRows API
   * @return parsed int32 value
   */
  private static int getInt32Value(
      String columnName,
      Object value,
      Integer scale,
      Integer precision,
      ColumnLogicalType logicalType,
      ColumnPhysicalType physicalType,
      final long insertRowsCurrIndex) {
    int intVal;
    switch (logicalType) {
      case DATE:
        intVal = PkgDataValidationUtil.validateAndParseDate(columnName, value, insertRowsCurrIndex);
        break;
      case TIME:
        Utils.assertNotNull("Unexpected null scale for TIME data type", scale);
        intVal =
            PkgDataValidationUtil.validateAndParseTime(columnName, value, scale, insertRowsCurrIndex)
                .intValue();
        break;
      case FIXED:
        BigDecimal bigDecimalValue =
            PkgDataValidationUtil.validateAndParseBigDecimal(columnName, value, insertRowsCurrIndex);
        bigDecimalValue = bigDecimalValue.setScale(scale, RoundingMode.HALF_UP);
        PkgDataValidationUtil.checkValueInRange(
            columnName, bigDecimalValue, scale, precision, insertRowsCurrIndex);
        intVal = bigDecimalValue.intValue();
        break;
      default:
        throw new PkgSFException(ErrorCode.UNKNOWN_DATA_TYPE, columnName, logicalType, physicalType);
    }
    return intVal;
  }

  /**
   * Parses an int64 value based on Snowflake logical type.
   *
   * @param value column value provided by user in a row
   * @param scale data type scale
   * @param precision data type precision
   * @param logicalType Snowflake logical type
   * @param physicalType Snowflake physical type
   * @return parsed int64 value
   */
  private static long getInt64Value(
      String columnName,
      Object value,
      int scale,
      int precision,
      ColumnLogicalType logicalType,
      ColumnPhysicalType physicalType,
      ZoneId defaultTimezone,
      final long insertRowsCurrIndex) {
    long longValue;
    switch (logicalType) {
      case TIME:
        Utils.assertNotNull("Unexpected null scale for TIME data type", scale);
        longValue =
            PkgDataValidationUtil.validateAndParseTime(columnName, value, scale, insertRowsCurrIndex)
                .longValue();
        break;
      case TIMESTAMP_LTZ:
      case TIMESTAMP_NTZ:
        boolean trimTimezone = logicalType == ColumnLogicalType.TIMESTAMP_NTZ;
        longValue =
            PkgDataValidationUtil.validateAndParseTimestamp(
                    columnName, value, scale, defaultTimezone, trimTimezone, insertRowsCurrIndex)
                .toBinary(false)
                .longValue();
        break;
      case TIMESTAMP_TZ:
        longValue =
            PkgDataValidationUtil.validateAndParseTimestamp(
                    columnName, value, scale, defaultTimezone, false, insertRowsCurrIndex)
                .toBinary(true)
                .longValue();
        break;
      case FIXED:
        BigDecimal bigDecimalValue =
            PkgDataValidationUtil.validateAndParseBigDecimal(columnName, value, insertRowsCurrIndex);
        bigDecimalValue = bigDecimalValue.setScale(scale, RoundingMode.HALF_UP);
        PkgDataValidationUtil.checkValueInRange(
            columnName, bigDecimalValue, scale, precision, insertRowsCurrIndex);
        longValue = bigDecimalValue.longValue();
        break;
      default:
        throw new PkgSFException(ErrorCode.UNKNOWN_DATA_TYPE, columnName, logicalType, physicalType);
    }
    return longValue;
  }

  /**
   * Parses an int128 value based on Snowflake logical type.
   *
   * @param value column value provided by user in a row
   * @param scale data type scale
   * @param precision data type precision
   * @param logicalType Snowflake logical type
   * @param physicalType Snowflake physical type
   * @return parsed int64 value
   */
  private static BigInteger getSb16Value(
      String columnName,
      Object value,
      int scale,
      int precision,
      ColumnLogicalType logicalType,
      ColumnPhysicalType physicalType,
      ZoneId defaultTimezone,
      final long insertRowsCurrIndex) {
    switch (logicalType) {
      case TIMESTAMP_TZ:
        return PkgDataValidationUtil.validateAndParseTimestamp(
                columnName, value, scale, defaultTimezone, false, insertRowsCurrIndex)
            .toBinary(true);
      case TIMESTAMP_LTZ:
      case TIMESTAMP_NTZ:
        boolean trimTimezone = logicalType == ColumnLogicalType.TIMESTAMP_NTZ;
        return PkgDataValidationUtil.validateAndParseTimestamp(
                columnName, value, scale, defaultTimezone, trimTimezone, insertRowsCurrIndex)
            .toBinary(false);
      case FIXED:
        BigDecimal bigDecimalValue =
            PkgDataValidationUtil.validateAndParseBigDecimal(columnName, value, insertRowsCurrIndex);
        // explicitly match the BigDecimal input scale with the Snowflake data type scale
        bigDecimalValue = bigDecimalValue.setScale(scale, RoundingMode.HALF_UP);
        PkgDataValidationUtil.checkValueInRange(
            columnName, bigDecimalValue, scale, precision, insertRowsCurrIndex);
        return bigDecimalValue.unscaledValue();
      default:
        throw new PkgSFException(ErrorCode.UNKNOWN_DATA_TYPE, columnName, logicalType, physicalType);
    }
  }

  /**
   * Converts an int128 value to its byte array representation.
   *
   * @param intRep int128 value
   * @return byte array representation
   */
  static byte[] getSb16Bytes(BigInteger intRep) {
    byte[] bytes = intRep.toByteArray();
    byte padByte = (byte) (bytes[0] < 0 ? -1 : 0);
    byte[] bytesBE = new byte[16];
    for (int i = 0; i < 16 - bytes.length; i++) {
      bytesBE[i] = padByte;
    }
    System.arraycopy(bytes, 0, bytesBE, 16 - bytes.length, bytes.length);
    return bytesBE;
  }

  /**
   * Converts an object or string to its byte array representation.
   *
   * @param value value to parse
   * @param columnMetadata column metadata
   * @param insertRowsCurrIndex Used for logging the row of index given in insertRows API
   * @return string representation
   */
  private static String getBinaryValue(
      Object value,
      ColumnMetadata columnMetadata,
      final long insertRowsCurrIndex,
      boolean enableNewJsonParsingLogic) {
    ColumnLogicalType logicalType =
        ColumnLogicalType.valueOf(columnMetadata.getLogicalType());
    String str;
    if (logicalType.isObject()) {
        str = switch (logicalType) {
            case OBJECT -> enableNewJsonParsingLogic
                    ? PkgDataValidationUtil.validateAndParseObjectNew(
                    columnMetadata.getName(), value, insertRowsCurrIndex)
                    : PkgDataValidationUtil.validateAndParseObject(
                    columnMetadata.getName(), value, insertRowsCurrIndex);
            case VARIANT -> enableNewJsonParsingLogic
                    ? PkgDataValidationUtil.validateAndParseVariantNew(
                    columnMetadata.getName(), value, insertRowsCurrIndex)
                    : PkgDataValidationUtil.validateAndParseVariant(
                    columnMetadata.getName(), value, insertRowsCurrIndex);
            case ARRAY -> enableNewJsonParsingLogic
                    ? PkgDataValidationUtil.validateAndParseArrayNew(
                    columnMetadata.getName(), value, insertRowsCurrIndex)
                    : PkgDataValidationUtil.validateAndParseArray(
                    columnMetadata.getName(), value, insertRowsCurrIndex);
            default -> throw new PkgSFException(
                    ErrorCode.UNKNOWN_DATA_TYPE,
                    columnMetadata.getName(),
                    logicalType,
                    columnMetadata.getPhysicalType());
        };
    } else {
      String maxLengthString = columnMetadata.getLength().toString();
      str =
          PkgDataValidationUtil.validateAndParseString(
              columnMetadata.getName(),
              value,
              Optional.of(maxLengthString).map(Integer::parseInt),
              insertRowsCurrIndex);
    }
    return str;
  }

  /**
   * Converts a binary value to its byte array representation.
   *
   * @param value value to parse
   * @param columnMetadata column metadata
   * @return byte array representation
   */
  private static byte[] getBinaryValueForLogicalBinary(
      Object value,
      ColumnMetadata columnMetadata,
      final long insertRowsCurrIndex) {
    String maxLengthString = columnMetadata.getByteLength().toString();
      return PkgDataValidationUtil.validateAndParseBinary(
          columnMetadata.getName(),
          value,
          Optional.of(maxLengthString).map(Integer::parseInt),
          insertRowsCurrIndex);
  }

  // these types cannot be packed into the data chunk because they are not readable by the server
  // side scanner
  private static final int INVALID_SERVER_SIDE_DATA_TYPE_ORDINAL = -1;

  // Snowflake table column logical type
  enum ColumnLogicalType {
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

  // Snowflake table column physical type
  enum ColumnPhysicalType {
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
}
