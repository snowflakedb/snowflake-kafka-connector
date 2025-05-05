/*
 * Copyright (c) 2022-2024 Snowflake Computing Inc. All rights reserved.
 */

package com.snowflake.kafka.connector.internal.streaming.validation;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;

/** Generates the Parquet types for the Snowflake's column types */
class PkgParquetTypeGenerator {

  private static final Set<PkgParquetValueParserSnowflake.ColumnPhysicalType>
      TIME_SUPPORTED_PHYSICAL_TYPES =
          new HashSet<>(
              Arrays.asList(
                  PkgParquetValueParserSnowflake.ColumnPhysicalType.SB4,
                  PkgParquetValueParserSnowflake.ColumnPhysicalType.SB8));
  private static final Set<PkgParquetValueParserSnowflake.ColumnPhysicalType>
      TIMESTAMP_SUPPORTED_PHYSICAL_TYPES =
          new HashSet<>(
              Arrays.asList(
                  PkgParquetValueParserSnowflake.ColumnPhysicalType.SB8,
                  PkgParquetValueParserSnowflake.ColumnPhysicalType.SB16));

  /**
   * Generate the column parquet type and metadata from the column metadata received from server
   * side.
   *
   * @param column column metadata as received from server side
   * @param id column id if column.getOrdinal() is not available
   * @return column parquet type
   */
  static PkgParquetTypeInfo generateColumnParquetTypeInfo(ColumnMetadata column, int id) {
    id = column.getOrdinal() == null ? id : column.getOrdinal();
    Type parquetType;
    Map<String, String> metadata = new HashMap<>();
    String name = column.getInternalName();

    // Parquet Type.Repetition in general supports repeated values for the same row column, like a
    // list of values.
    // This generator uses only either 0 or 1 value for nullable data type (OPTIONAL: 0 or none
    // value if it is null)
    // or exactly 1 value for non-nullable data type (REQUIRED)
    Type.Repetition repetition =
        column.getNullable() ? Type.Repetition.OPTIONAL : Type.Repetition.REQUIRED;

    if (column.getSourceIcebergDataType() != null) {
      parquetType =
          com.snowflake.kafka.connector.internal.streaming.schemaevolution.iceberg
              .IcebergDataTypeParser.parseIcebergDataTypeStringToParquetType(
              column.getSourceIcebergDataType(), repetition, id, name);
    } else {
      PkgParquetValueParserSnowflake.ColumnPhysicalType physicalType;
      PkgParquetValueParserSnowflake.ColumnLogicalType logicalType;
      try {
        physicalType =
            PkgParquetValueParserSnowflake.ColumnPhysicalType.valueOf(column.getPhysicalType());
        logicalType =
            PkgParquetValueParserSnowflake.ColumnLogicalType.valueOf(column.getLogicalType());
      } catch (IllegalArgumentException e) {
        throw new PkgSFException(
            ErrorCode.UNKNOWN_DATA_TYPE,
            column.getName(),
            column.getLogicalType(),
            column.getPhysicalType());
      }

      metadata.put(
          Integer.toString(id), logicalType.getOrdinal() + "," + physicalType.getOrdinal());

      // Handle differently depends on the column logical and physical types
      switch (logicalType) {
        case FIXED:
          parquetType = getFixedColumnParquetType(column, id, physicalType, repetition);
          break;
        case ARRAY:
        case OBJECT:
        case VARIANT:
          // mark the column metadata as being an object json for the server side scanner
          metadata.put(id + ":obj_enc", "1");
          // parquetType is same as the next one
        case ANY:
        case CHAR:
        case TEXT:
        case BINARY:
          parquetType =
              Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, repetition)
                  .as(LogicalTypeAnnotation.stringType())
                  .id(id)
                  .named(name);
          break;
        case TIMESTAMP_LTZ:
        case TIMESTAMP_NTZ:
        case TIMESTAMP_TZ:
          parquetType =
              getTimeColumnParquetType(
                  column.getScale(),
                  physicalType,
                  logicalType,
                  TIMESTAMP_SUPPORTED_PHYSICAL_TYPES,
                  repetition,
                  id,
                  name);
          break;
        case DATE:
          parquetType =
              Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, repetition)
                  .as(LogicalTypeAnnotation.dateType())
                  .id(id)
                  .named(name);
          break;
        case TIME:
          parquetType =
              getTimeColumnParquetType(
                  column.getScale(),
                  physicalType,
                  logicalType,
                  TIME_SUPPORTED_PHYSICAL_TYPES,
                  repetition,
                  id,
                  name);
          break;
        case BOOLEAN:
          parquetType =
              Types.primitive(PrimitiveType.PrimitiveTypeName.BOOLEAN, repetition)
                  .id(id)
                  .named(name);
          break;
        case REAL:
          parquetType =
              Types.primitive(PrimitiveType.PrimitiveTypeName.DOUBLE, repetition)
                  .id(id)
                  .named(name);
          break;
        default:
          throw new PkgSFException(
              ErrorCode.UNKNOWN_DATA_TYPE,
              column.getName(),
              column.getLogicalType(),
              column.getPhysicalType());
      }
    }
    return new PkgParquetTypeInfo(parquetType, metadata);
  }

  private static Type toUnshaded(
      net.snowflake.ingest.internal.apache.parquet.schema.Type shadedParquetType) {
    return null;
  }

  /**
   * Get the parquet type for column of Snowflake FIXED logical type.
   *
   * @param column column metadata
   * @param id column id in Snowflake table schema
   * @param physicalType Snowflake physical type of column
   * @param repetition parquet repetition type of column
   * @return column parquet type
   */
  private static Type getFixedColumnParquetType(
      ColumnMetadata column,
      int id,
      PkgParquetValueParserSnowflake.ColumnPhysicalType physicalType,
      Type.Repetition repetition) {
    String name = column.getInternalName();
    // the LogicalTypeAnnotation.DecimalLogicalTypeAnnotation is used by server side scanner
    // to discover data type scale and precision
    LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimalLogicalTypeAnnotation =
        column.getScale() != null && column.getPrecision() != null
            ? LogicalTypeAnnotation.DecimalLogicalTypeAnnotation.decimalType(
                column.getScale(), column.getPrecision())
            : null;
    Type parquetType;
    if ((column.getScale() != null && column.getScale() != 0)
        || physicalType == PkgParquetValueParserSnowflake.ColumnPhysicalType.SB16) {
      parquetType =
          Types.primitive(PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, repetition)
              .length(16)
              .as(decimalLogicalTypeAnnotation)
              .id(id)
              .named(name);
    } else {
      switch (physicalType) {
        case SB1:
        case SB2:
        case SB4:
          parquetType =
              Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, repetition)
                  .as(decimalLogicalTypeAnnotation)
                  .id(id)
                  .length(4)
                  .named(name);
          break;
        case SB8:
          parquetType =
              Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, repetition)
                  .as(decimalLogicalTypeAnnotation)
                  .id(id)
                  .length(8)
                  .named(name);
          break;
        default:
          throw new PkgSFException(
              ErrorCode.UNKNOWN_DATA_TYPE,
              column.getName(),
              column.getLogicalType(),
              column.getPhysicalType());
      }
    }
    return parquetType;
  }

  /**
   * Get the parquet type for column of a Snowflake time logical type.
   *
   * @param scale column scale
   * @param physicalType Snowflake physical type of column
   * @param logicalType Snowflake logical type of column
   * @param supportedPhysicalTypes supported Snowflake physical types for the given column
   * @param repetition parquet repetition type of column
   * @param id column id in Snowflake table schema
   * @param name column name
   * @return column parquet type
   */
  private static Type getTimeColumnParquetType(
      Integer scale,
      PkgParquetValueParserSnowflake.ColumnPhysicalType physicalType,
      PkgParquetValueParserSnowflake.ColumnLogicalType logicalType,
      Set<PkgParquetValueParserSnowflake.ColumnPhysicalType> supportedPhysicalTypes,
      Type.Repetition repetition,
      int id,
      String name) {
    if (scale == null || scale > 9 || scale < 0 || !supportedPhysicalTypes.contains(physicalType)) {
      throw new PkgSFException(
          ErrorCode.UNKNOWN_DATA_TYPE,
          name,
          String.format("%s(%d)", logicalType, scale),
          physicalType);
    }

    PrimitiveType.PrimitiveTypeName type = getTimePrimitiveType(physicalType);
    LogicalTypeAnnotation typeAnnotation;
    int length;
    switch (physicalType) {
      case SB4:
        typeAnnotation = LogicalTypeAnnotation.decimalType(scale, 9);
        length = 4;
        break;
      case SB8:
        typeAnnotation = LogicalTypeAnnotation.decimalType(scale, 18);
        length = 8;
        break;
      case SB16:
        typeAnnotation = LogicalTypeAnnotation.decimalType(scale, 38);
        length = 16;
        break;
      default:
        throw new PkgSFException(ErrorCode.UNKNOWN_DATA_TYPE, name, logicalType, physicalType);
    }
    return Types.primitive(type, repetition).as(typeAnnotation).length(length).id(id).named(name);
  }

  /**
   * Get the parquet primitive type name for column of a Snowflake time logical type.
   *
   * @param physicalType Snowflake physical type of column
   * @return column parquet primitive type name
   */
  private static PrimitiveType.PrimitiveTypeName getTimePrimitiveType(
      PkgParquetValueParserSnowflake.ColumnPhysicalType physicalType) {
    PrimitiveType.PrimitiveTypeName type;
    switch (physicalType) {
      case SB4:
        type = PrimitiveType.PrimitiveTypeName.INT32;
        break;
      case SB8:
        type = PrimitiveType.PrimitiveTypeName.INT64;
        break;
      case SB16:
        type = PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;
        break;
      default:
        throw new UnsupportedOperationException("Time physical type: " + physicalType);
    }
    return type;
  }
}
