/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

package com.snowflake.kafka.connector.internal.streaming.validation;

import static com.snowflake.kafka.connector.internal.streaming.validation.PkgDataValidationUtil.checkFixedLengthByteArray;
import static net.snowflake.ingest.utils.Utils.concatDotPath;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import net.snowflake.ingest.utils.Constants;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.DateLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.DecimalLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.StringLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimestampLogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Type.Repetition;

/** Parses a user Iceberg column value into Parquet internal representation for buffering. */
class PkgParquetValueParserIceberg {

  /**
   * Parses a user column value into Parquet internal representation for buffering.
   *
   * @param value column value provided by user in a row
   * @param type Parquet column type
   * @param subColumnFinder helper class to find stats of sub-columns
   * @param defaultTimezone default timezone to use for timestamp parsing
   * @param insertRowsCurrIndex Row index corresponding the row to parse (w.r.t input rows in
   *     insertRows API, and not buffered row)
   * @param error InsertError object to add errors
   * @return parsed value and byte size of Parquet internal representation
   */
  static PkgParquetBufferValue parseColumnValueToParquet(
      Object value,
      Type type,
      PkgSubColumnFinder subColumnFinder,
      ZoneId defaultTimezone,
      long insertRowsCurrIndex,
      PkgInsertError error) {
    return parseColumnValueToParquet(
        value,
        type,
        subColumnFinder,
        defaultTimezone,
        insertRowsCurrIndex,
        false /* isDescendantsOfRepeatingGroup */,
        error);
  }

  private static PkgParquetBufferValue parseColumnValueToParquet(
      Object value,
      Type type,
      PkgSubColumnFinder subColumnFinder,
      ZoneId defaultTimezone,
      long insertRowsCurrIndex,
      boolean isDescendantsOfRepeatingGroup,
      PkgInsertError error) {
    float estimatedParquetSize = 0F;

    if (type.getId() == null) {
      throw new PkgSFException(
          ErrorCode.INTERNAL_ERROR, String.format("Id not found for field: %s", type.getName()));
    }
    Type.ID id = type.getId();

    String path = subColumnFinder.getDotPath(id);
    if (value != null) {
      if (type.isPrimitive()) {
        estimatedParquetSize += PkgParquetBufferValue.DEFINITION_LEVEL_ENCODING_BYTE_LEN;
        estimatedParquetSize +=
            isDescendantsOfRepeatingGroup
                ? PkgParquetBufferValue.REPETITION_LEVEL_ENCODING_BYTE_LEN
                : 0;
        PrimitiveType primitiveType = type.asPrimitiveType();
        switch (primitiveType.getPrimitiveTypeName()) {
          case BOOLEAN:
            int intValue =
                PkgDataValidationUtil.validateAndParseBoolean(path, value, insertRowsCurrIndex);
            value = intValue > 0;
            estimatedParquetSize += PkgParquetBufferValue.BIT_ENCODING_BYTE_LEN;
            break;
          case INT32:
            value = getInt32Value(value, primitiveType, path, insertRowsCurrIndex);
            estimatedParquetSize += 4;
            break;
          case INT64:
            value = getInt64Value(value, primitiveType, defaultTimezone, path, insertRowsCurrIndex);
            estimatedParquetSize += 8;
            break;
          case FLOAT:
            value =
                (float)
                    PkgDataValidationUtil.validateAndParseReal(path, value, insertRowsCurrIndex);
            estimatedParquetSize += 4;
            break;
          case DOUBLE:
            value = PkgDataValidationUtil.validateAndParseReal(path, value, insertRowsCurrIndex);
            estimatedParquetSize += 8;
            break;
          case BINARY:
            byte[] byteVal = getBinaryValue(value, primitiveType, path, insertRowsCurrIndex);
            value = byteVal;
            estimatedParquetSize +=
                PkgParquetBufferValue.BYTE_ARRAY_LENGTH_ENCODING_BYTE_LEN + byteVal.length;
            break;
          case FIXED_LEN_BYTE_ARRAY:
            byte[] fixedLenByteArrayVal =
                getFixedLenByteArrayValue(value, primitiveType, path, insertRowsCurrIndex);
            value = fixedLenByteArrayVal;
            estimatedParquetSize +=
                PkgParquetBufferValue.BYTE_ARRAY_LENGTH_ENCODING_BYTE_LEN
                    + fixedLenByteArrayVal.length;
            break;
          default:
            throw new PkgSFException(
                ErrorCode.UNKNOWN_DATA_TYPE,
                path,
                type.getLogicalTypeAnnotation(),
                primitiveType.getPrimitiveTypeName());
        }
      } else {
        return getGroupValue(
            value,
            type.asGroupType(),
            subColumnFinder,
            defaultTimezone,
            insertRowsCurrIndex,
            isDescendantsOfRepeatingGroup,
            error);
      }
    }

    if (value == null) {
      if (type.isRepetition(Repetition.REQUIRED)) {
        error.addNullValueForNotNullColName(path);
      }
    }

    return new PkgParquetBufferValue(value, estimatedParquetSize);
  }

  /**
   * Parses an int32 value based on Parquet logical type.
   *
   * @param value column value provided by user in a row
   * @param type Parquet column type
   * @param path column path, used for logging
   * @param insertRowsCurrIndex Used for logging the row of index given in insertRows API
   * @return parsed int32 value
   */
  private static int getInt32Value(
      Object value, PrimitiveType type, String path, final long insertRowsCurrIndex) {
    LogicalTypeAnnotation logicalTypeAnnotation = type.getLogicalTypeAnnotation();
    if (logicalTypeAnnotation == null) {
      return PkgDataValidationUtil.validateAndParseIcebergInt(path, value, insertRowsCurrIndex);
    }
    if (logicalTypeAnnotation instanceof DecimalLogicalTypeAnnotation) {
      return getDecimalValue(value, type, path, insertRowsCurrIndex).unscaledValue().intValue();
    }
    if (logicalTypeAnnotation instanceof DateLogicalTypeAnnotation) {
      return PkgDataValidationUtil.validateAndParseDate(path, value, insertRowsCurrIndex);
    }
    throw new PkgSFException(
        ErrorCode.UNKNOWN_DATA_TYPE, path, logicalTypeAnnotation, type.getPrimitiveTypeName());
  }

  /**
   * Parses an int64 value based on Parquet logical type.
   *
   * @param value column value provided by user in a row
   * @param type Parquet column type
   * @param path column path, used for logging
   * @param insertRowsCurrIndex Used for logging the row of index given in insertRows API
   * @return parsed int64 value
   */
  private static long getInt64Value(
      Object value,
      PrimitiveType type,
      ZoneId defaultTimezone,
      String path,
      final long insertRowsCurrIndex) {
    LogicalTypeAnnotation logicalTypeAnnotation = type.getLogicalTypeAnnotation();
    if (logicalTypeAnnotation == null) {
      return PkgDataValidationUtil.validateAndParseIcebergLong(path, value, insertRowsCurrIndex);
    }
    if (logicalTypeAnnotation instanceof DecimalLogicalTypeAnnotation) {
      return getDecimalValue(value, type, path, insertRowsCurrIndex).unscaledValue().longValue();
    }
    if (logicalTypeAnnotation instanceof TimeLogicalTypeAnnotation) {
      return PkgDataValidationUtil.validateAndParseTime(
              path,
              value,
              timeUnitToScale(((TimeLogicalTypeAnnotation) logicalTypeAnnotation).getUnit()),
              insertRowsCurrIndex)
          .longValue();
    }
    if (logicalTypeAnnotation instanceof TimestampLogicalTypeAnnotation) {
      boolean includeTimeZone =
          ((TimestampLogicalTypeAnnotation) logicalTypeAnnotation).isAdjustedToUTC();
      return PkgDataValidationUtil.validateAndParseTimestamp(
              type.getName(),
              value,
              timeUnitToScale(((TimestampLogicalTypeAnnotation) logicalTypeAnnotation).getUnit()),
              defaultTimezone,
              !includeTimeZone,
              insertRowsCurrIndex)
          .toBinary(false)
          .longValue();
    }
    throw new PkgSFException(
        ErrorCode.UNKNOWN_DATA_TYPE,
        path,
        logicalTypeAnnotation,
        type.asPrimitiveType().getPrimitiveTypeName());
  }

  /**
   * Converts an Iceberg binary or string column to its byte array representation.
   *
   * @param value value to parse
   * @param type Parquet column type
   * @param path column path, used for logging
   * @param insertRowsCurrIndex Used for logging the row of index given in insertRows API
   * @return string representation
   */
  private static byte[] getBinaryValue(
      Object value, PrimitiveType type, String path, final long insertRowsCurrIndex) {
    LogicalTypeAnnotation logicalTypeAnnotation = type.getLogicalTypeAnnotation();
    if (logicalTypeAnnotation == null) {
      byte[] bytes =
          PkgDataValidationUtil.validateAndParseBinary(
              path, value, Optional.of(Constants.BINARY_COLUMN_MAX_SIZE), insertRowsCurrIndex);
      return bytes;
    }
    if (logicalTypeAnnotation instanceof StringLogicalTypeAnnotation) {
      String string =
          PkgDataValidationUtil.validateAndParseString(
              path, value, Optional.of(Constants.VARCHAR_COLUMN_MAX_SIZE), insertRowsCurrIndex);
      return string.getBytes(StandardCharsets.UTF_8);
    }
    throw new PkgSFException(
        ErrorCode.UNKNOWN_DATA_TYPE, path, logicalTypeAnnotation, type.getPrimitiveTypeName());
  }

  /**
   * Converts an Iceberg fixed length byte array column to its byte array representation.
   *
   * @param value value to parse
   * @param type Parquet column type
   * @param path column path, used for logging
   * @param insertRowsCurrIndex Used for logging the row of index given in insertRows API
   * @return string representation
   */
  private static byte[] getFixedLenByteArrayValue(
      Object value, PrimitiveType type, String path, final long insertRowsCurrIndex) {
    LogicalTypeAnnotation logicalTypeAnnotation = type.getLogicalTypeAnnotation();
    int length = type.getTypeLength();
    byte[] bytes = null;
    if (logicalTypeAnnotation == null) {
      PkgDataValidationUtil.validateAndParseBinary(
          path, value, Optional.of(length), insertRowsCurrIndex);
    }
    if (logicalTypeAnnotation instanceof DecimalLogicalTypeAnnotation) {
      BigInteger bigIntegerVal =
          getDecimalValue(value, type, path, insertRowsCurrIndex).unscaledValue();
      bytes = bigIntegerVal.toByteArray();
      if (bytes.length < length) {
        byte[] newBytes = new byte[length];
        Arrays.fill(newBytes, (byte) (bytes[0] < 0 ? -1 : 0));
        System.arraycopy(bytes, 0, newBytes, length - bytes.length, bytes.length);
        bytes = newBytes;
      }
    }
    if (bytes != null) {
      checkFixedLengthByteArray(path, bytes, length, insertRowsCurrIndex);
      return bytes;
    }
    throw new PkgSFException(
        ErrorCode.UNKNOWN_DATA_TYPE, path, logicalTypeAnnotation, type.getPrimitiveTypeName());
  }

  /**
   * Converts a decimal value to its BigDecimal representation.
   *
   * @param value value to parse
   * @param type Parquet column type
   * @param path column path, used for logging
   * @param insertRowsCurrIndex Used for logging the row of index given in insertRows API
   * @return BigDecimal representation
   */
  private static BigDecimal getDecimalValue(
      Object value, PrimitiveType type, String path, final long insertRowsCurrIndex) {
    int scale = ((DecimalLogicalTypeAnnotation) type.getLogicalTypeAnnotation()).getScale();
    int precision = ((DecimalLogicalTypeAnnotation) type.getLogicalTypeAnnotation()).getPrecision();
    BigDecimal bigDecimalValue =
        PkgDataValidationUtil.validateAndParseBigDecimal(path, value, insertRowsCurrIndex);
    bigDecimalValue = bigDecimalValue.setScale(scale, RoundingMode.HALF_UP);
    PkgDataValidationUtil.checkValueInRange(
        path, bigDecimalValue, scale, precision, insertRowsCurrIndex);
    return bigDecimalValue;
  }

  private static int timeUnitToScale(LogicalTypeAnnotation.TimeUnit timeUnit) {
    switch (timeUnit) {
      case MILLIS:
        return 3;
      case MICROS:
        return 6;
      case NANOS:
        return 9;
      default:
        throw new PkgSFException(
            ErrorCode.INTERNAL_ERROR, String.format("Unknown time unit: %s", timeUnit));
    }
  }

  /**
   * Parses a group value based on Parquet group logical type.
   *
   * @param value value to parse
   * @param type Parquet column type
   * @param subColumnFinder helper class to find stats of sub-columns
   * @param defaultTimezone default timezone to use for timestamp parsing
   * @param insertRowsCurrIndex Used for logging the row of index given in insertRows API
   * @param isDescendantsOfRepeatingGroup true if the column is a descendant of a repeating group,
   * @param error InsertError object to add errors
   * @return list of parsed values
   */
  private static PkgParquetBufferValue getGroupValue(
      Object value,
      GroupType type,
      PkgSubColumnFinder subColumnFinder,
      ZoneId defaultTimezone,
      final long insertRowsCurrIndex,
      boolean isDescendantsOfRepeatingGroup,
      PkgInsertError error) {
    LogicalTypeAnnotation logicalTypeAnnotation = type.getLogicalTypeAnnotation();
    if (logicalTypeAnnotation == null) {
      return getStructValue(
          value,
          type,
          subColumnFinder,
          defaultTimezone,
          insertRowsCurrIndex,
          isDescendantsOfRepeatingGroup,
          error);
    }
    if (logicalTypeAnnotation instanceof LogicalTypeAnnotation.ListLogicalTypeAnnotation) {
      return get3LevelListValue(
          value, type, subColumnFinder, defaultTimezone, insertRowsCurrIndex, error);
    }
    if (logicalTypeAnnotation instanceof LogicalTypeAnnotation.MapLogicalTypeAnnotation) {
      return get3LevelMapValue(
          value, type, subColumnFinder, defaultTimezone, insertRowsCurrIndex, error);
    }
    throw new PkgSFException(
        ErrorCode.UNKNOWN_DATA_TYPE,
        subColumnFinder.getDotPath(type.getId()),
        logicalTypeAnnotation,
        type.getClass().getSimpleName());
  }

  /**
   * Parses a struct value based on Parquet group logical type. The parsed value is a list of
   * values, where each element represents a field in the group. For example, an input {@code
   * {"field1": 1, "field2": 2}} will be parsed as {@code [1, 2]}.
   */
  private static PkgParquetBufferValue getStructValue(
      Object value,
      GroupType type,
      PkgSubColumnFinder subColumnFinder,
      ZoneId defaultTimezone,
      final long insertRowsCurrIndex,
      boolean isDescendantsOfRepeatingGroup,
      PkgInsertError error) {
    Map<String, ?> structVal =
        PkgDataValidationUtil.validateAndParseIcebergStruct(
            subColumnFinder.getDotPath(type.getId()), value, insertRowsCurrIndex);
    Set<String> extraFields = new HashSet<>(structVal.keySet());
    List<String> missingFields = new ArrayList<>();
    List<Object> listVal = new ArrayList<>(type.getFieldCount());
    float estimatedParquetSize = 0f;
    for (int i = 0; i < type.getFieldCount(); i++) {
      extraFields.remove(type.getFieldName(i));
      if (structVal.containsKey(type.getFieldName(i))) {
        PkgParquetBufferValue parsedValue =
            parseColumnValueToParquet(
                structVal.get(type.getFieldName(i)),
                type.getType(i),
                subColumnFinder,
                defaultTimezone,
                insertRowsCurrIndex,
                isDescendantsOfRepeatingGroup,
                error);
        listVal.add(parsedValue.getValue());
        estimatedParquetSize += parsedValue.getSize();
      } else {
        if (type.getType(i).isRepetition(Repetition.REQUIRED)) {
          missingFields.add(type.getFieldName(i));
        } else {
          listVal.add(null);
        }
      }
    }

    for (String missingField : missingFields) {
      List<String> missingFieldPath = new ArrayList<>(subColumnFinder.getPath(type.getId()));
      missingFieldPath.add(missingField);
      error.addMissingNotNullColName(concatDotPath(missingFieldPath.toArray(new String[0])));
    }
    for (String extraField : extraFields) {
      List<String> extraFieldPath = new ArrayList<>(subColumnFinder.getPath(type.getId()));
      extraFieldPath.add(extraField);
      error.addExtraColName(concatDotPath(extraFieldPath.toArray(new String[0])));
    }
    return new PkgParquetBufferValue(listVal, estimatedParquetSize);
  }

  /**
   * Parses an iterable value based on Parquet 3-level list logical type. Please check <a
   * href="https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#lists">Parquet
   * Logical Types#Lists</a> for more details. The parsed value is a list of lists, where each inner
   * list represents a list of elements in the group. For example, an input {@code [1, 2, 3, 4]}
   * will be parsed as {@code [[1], [2], [3], [4]]}.
   */
  private static PkgParquetBufferValue get3LevelListValue(
      Object value,
      GroupType type,
      PkgSubColumnFinder subColumnFinder,
      ZoneId defaultTimezone,
      final long insertRowsCurrIndex,
      PkgInsertError error) {
    Iterable<?> iterableVal =
        PkgDataValidationUtil.validateAndParseIcebergList(
            subColumnFinder.getDotPath(type.getId()), value, insertRowsCurrIndex);
    List<Object> listVal = new ArrayList<>();
    float estimatedParquetSize = 0;
    for (Object val : iterableVal) {
      PkgParquetBufferValue parsedValue =
          parseColumnValueToParquet(
              val,
              type.getType(0).asGroupType().getType(0),
              subColumnFinder,
              defaultTimezone,
              insertRowsCurrIndex,
              true /* isDecedentOfRepeatingGroup */,
              error);
      listVal.add(Collections.singletonList(parsedValue.getValue()));
      estimatedParquetSize += parsedValue.getSize();
    }
    return new PkgParquetBufferValue(listVal, estimatedParquetSize);
  }

  /**
   * Parses a map value based on Parquet 3-level map logical type. Please check <a
   * href="https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#maps">Parquet
   * Logical Types#Maps</a> for more details. The parsed value is a list of lists, where each inner
   * list represents a key-value pair in the group. For example, an input {@code {"a": 1, "b": 2}}
   * will be parsed as {@code [["a", 1], ["b", 2]]}.
   */
  private static PkgParquetBufferValue get3LevelMapValue(
      Object value,
      GroupType type,
      PkgSubColumnFinder subColumnFinder,
      ZoneId defaultTimezone,
      final long insertRowsCurrIndex,
      PkgInsertError error) {
    Map<?, ?> mapVal =
        PkgDataValidationUtil.validateAndParseIcebergMap(
            subColumnFinder.getDotPath(type.getId()), value, insertRowsCurrIndex);
    List<Object> listVal = new ArrayList<>();
    float estimatedParquetSize = 0;
    for (Map.Entry<?, ?> entry : mapVal.entrySet()) {
      PkgParquetBufferValue parsedKey =
          parseColumnValueToParquet(
              entry.getKey(),
              type.getType(0).asGroupType().getType(0),
              subColumnFinder,
              defaultTimezone,
              insertRowsCurrIndex,
              true /* isDecedentOfRepeatingGroup */,
              error);
      PkgParquetBufferValue parsedValue =
          parseColumnValueToParquet(
              entry.getValue(),
              type.getType(0).asGroupType().getType(1),
              subColumnFinder,
              defaultTimezone,
              insertRowsCurrIndex,
              true /* isDecedentOfRepeatingGroup */,
              error);
      listVal.add(Arrays.asList(parsedKey.getValue(), parsedValue.getValue()));
      estimatedParquetSize += parsedKey.getSize() + parsedValue.getSize();
    }
    return new PkgParquetBufferValue(listVal, estimatedParquetSize);
  }
}
