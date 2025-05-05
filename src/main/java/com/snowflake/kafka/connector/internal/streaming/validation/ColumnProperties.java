/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

package com.snowflake.kafka.connector.internal.streaming.validation;

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

  private final Integer byteLength;

  private final Integer length;

  private final boolean nullable;

  private final String icebergColumnSchema;

  ColumnProperties(ColumnMetadata columnMetadata) {
    this.type = columnMetadata.getType();
    this.logicalType = columnMetadata.getLogicalType();
    this.precision = columnMetadata.getPrecision();
    this.scale = columnMetadata.getScale();
    this.byteLength = columnMetadata.getByteLength();
    this.length = columnMetadata.getLength();
    this.nullable = columnMetadata.getNullable();
    this.icebergColumnSchema = columnMetadata.getSourceIcebergDataType();
  }

  public ColumnProperties(
      net.snowflake.ingest.streaming.internal.ColumnProperties ingestSdkColumnProperties) {
    this.type = ingestSdkColumnProperties.getType();
    this.logicalType = ingestSdkColumnProperties.getLogicalType();
    this.precision = ingestSdkColumnProperties.getPrecision();
    this.scale = ingestSdkColumnProperties.getScale();
    this.byteLength = ingestSdkColumnProperties.getByteLength();
    this.length = ingestSdkColumnProperties.getLength();
    this.nullable = ingestSdkColumnProperties.isNullable();
    this.icebergColumnSchema = ingestSdkColumnProperties.getIcebergSchema();
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

  public Integer getByteLength() {
    return byteLength;
  }

  public Integer getLength() {
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
}
