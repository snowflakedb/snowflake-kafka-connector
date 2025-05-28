/*
 * Copyright (c) 2025 Snowflake Computing Inc. All rights reserved.
 */

package com.snowflake.kafka.connector.internal.streaming.common;

import com.snowflake.kafka.connector.internal.streaming.validation.ColumnMetadata;
import java.util.Objects;

/**
 * Class that encapsulates column properties. These are the same properties showed in the output of
 * <a href="https://docs.snowflake.com/en/sql-reference/sql/show-columns">SHOW COLUMNS</a>. Note
 * that this is slightly different than the internal column metadata used elsewhere in this SDK.
 */
public class ColumnProperties {
  private final String type;

  private final String logicalType;

  private final Integer precision;

  private final Integer scale;

  private final Long byteLength;

  private final Long length;

  private final boolean nullable;

  private final String icebergColumnSchema;

  public ColumnProperties(ColumnMetadata columnMetadata) {
    this.type = columnMetadata.getType();
    this.logicalType = columnMetadata.getLogicalType();
    this.precision = columnMetadata.getPrecision();
    this.scale = columnMetadata.getScale();
    this.byteLength = columnMetadata.getByteLength();
    this.length = columnMetadata.getLength();
    this.nullable = columnMetadata.getNullable();
    this.icebergColumnSchema = columnMetadata.getSourceIcebergDataType();
  }

  public ColumnProperties(net.snowflake.ingest.streaming.internal.ColumnProperties sdkProps) {
    this(
        sdkProps.getType(),
        sdkProps.getLogicalType(),
        sdkProps.getPrecision(),
        sdkProps.getScale(),
        sdkProps.getByteLength() == null ? null : Long.valueOf(sdkProps.getByteLength()),
        sdkProps.getLength() == null ? null : Long.valueOf(sdkProps.getLength()),
        sdkProps.isNullable(),
        sdkProps.getIcebergSchema());
  }

  public ColumnProperties(
      String type,
      String logicalType,
      Integer precision,
      Integer scale,
      Long byteLength,
      Long length,
      boolean nullable,
      String icebergColumnSchema) {
    this.type = type;
    this.logicalType = logicalType;
    this.precision = precision;
    this.scale = scale;
    this.byteLength = byteLength;
    this.length = length;
    this.nullable = nullable;
    this.icebergColumnSchema = icebergColumnSchema;
  }

  public String getType() {
    return type;
  }

  public String getLogicalType() {
    return logicalType;
  }

  public Integer getPrecision() {
    return precision;
  }

  public Integer getScale() {
    return scale;
  }

  public Long getByteLength() {
    return byteLength;
  }

  public Long getLength() {
    return length;
  }

  public boolean isNullable() {
    return nullable;
  }

  /**
   * Return the value of sourceIcebergDataType() as returned by the service. It is populated only
   * when this object represents an iceberg table's column, null otherwise. The String returned from
   * here is meant to conform to the json schema specified here:
   * https://iceberg.apache.org/spec/#appendix-c-json-serialization
   */
  public String getIcebergSchema() {
    return icebergColumnSchema;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ColumnProperties that = (ColumnProperties) o;
    return nullable == that.nullable
        && Objects.equals(type, that.type)
        && Objects.equals(logicalType, that.logicalType)
        && Objects.equals(precision, that.precision)
        && Objects.equals(scale, that.scale)
        && Objects.equals(byteLength, that.byteLength)
        && Objects.equals(length, that.length)
        && Objects.equals(icebergColumnSchema, that.icebergColumnSchema);
  }

  @Override
  public String toString() {
    return "ColumnProperties{"
        + "type='"
        + type
        + '\''
        + ", logicalType='"
        + logicalType
        + '\''
        + ", precision="
        + precision
        + ", scale="
        + scale
        + ", byteLength="
        + byteLength
        + ", length="
        + length
        + ", nullable="
        + nullable
        + ", icebergColumnSchema='"
        + icebergColumnSchema
        + '\''
        + '}';
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        type, logicalType, precision, scale, byteLength, length, nullable, icebergColumnSchema);
  }
}
