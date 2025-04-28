/*
 * Copyright (c) 2021-2024 Snowflake Computing Inc. All rights reserved.
 */

package com.snowflake.kafka.connector.internal.streaming.validation;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/** Column metadata for each column in a Snowflake table */
public class ColumnMetadata {
  private static ObjectMapper objectMapper = new ObjectMapper();

  private String name;
  private String internalName;
  private String type;
  private String logicalType;
  private String physicalType;
  private Integer precision;
  private Integer scale;
  private Integer byteLength;
  private Integer length;
  private boolean nullable;
  private String collation;
  private ObjectField[] objectFields;
  private Boolean fixed; // what is this?

  /**
   * The Json serialization of Iceberg data type of the column, see <a
   * href="https://iceberg.apache.org/spec/#appendix-c-json-serialization">JSON serialization</a>
   * for more details.
   */
  private String sourceIcebergDataType;

  /**
   * The column ordinal is an internal id of the column used by server scanner for the column
   * identification.
   */
  private Integer ordinal;

  @JsonProperty("name")
  public void setName(String name) {
    this.name = name;
    this.internalName = PkgLiteralQuoteUtils.unquoteColumnName(name);
  }

  String getName() {
    return this.name;
  }

  @JsonProperty("collation")
  public String getCollation() {
    return collation;
  }

  public void setCollation(String collation) {
    this.collation = collation;
  }

  @JsonProperty("type")
  public void setType(String type) {
    this.type = type;
  }

  public String getType() {
    return this.type;
  }

  @JsonProperty("logical_type")
  public void setLogicalType(String logicalType) {
    this.logicalType = logicalType.toUpperCase();
  }

  String getLogicalType() {
    return this.logicalType;
  }

  @JsonProperty("physical_type")
  public void setPhysicalType(String physicalType) {
    this.physicalType = physicalType;
  }

  String getPhysicalType() {
    return this.physicalType;
  }

  @JsonProperty("precision")
  public void setPrecision(Integer precision) {
    this.precision = precision;
  }

  Integer getPrecision() {
    return this.precision;
  }

  @JsonProperty("scale")
  public void setScale(Integer scale) {
    this.scale = scale;
  }

  Integer getScale() {
    return this.scale;
  }

  @JsonProperty("byteLength")
  public void setByteLength(Integer byteLength) {
    this.byteLength = byteLength;
  }

  Integer getByteLength() {
    return this.byteLength;
  }

  @JsonProperty("length")
  public void setLength(Integer length) {
    this.length = length;
  }

  Integer getLength() {
    return this.length;
  }

  @JsonProperty("nullable")
  public void setNullable(boolean nullable) {
    this.nullable = nullable;
  }

  boolean getNullable() {
    return this.nullable;
  }

  @JsonProperty("ordinal")
  public void setOrdinal(Integer ordinal) {
    this.ordinal = ordinal;
  }

  public Integer getOrdinal() {
    return ordinal;
  }

  @JsonProperty("source_iceberg_data_type")
  public void setSourceIcebergDataType(String sourceIcebergDataType) {
    this.sourceIcebergDataType = sourceIcebergDataType;
  }

  public String getSourceIcebergDataType() {
    return sourceIcebergDataType;
  }

  public ObjectField[] getObjectField() {
    return objectFields;
  }

  @JsonProperty("fields")
  public void setObjectField(ObjectField[] objectFields) {
    this.objectFields = objectFields;
  }

  public Boolean getFixed() {
    return fixed;
  }

  @JsonProperty("fixed")
  public void setFixed(Boolean fixed) {
    this.fixed = fixed;
  }

  String getInternalName() {
    return internalName;
  }

  @Override
  public String toString() {
    Map<String, Object> map = new HashMap<>();
    map.put("name", this.name);
    map.put("type", this.type);
    map.put("logical_type", this.logicalType);
    map.put("physical_type", this.physicalType);
    map.put("precision", this.precision);
    map.put("scale", this.scale);
    map.put("byte_length", this.byteLength);
    map.put("length", this.length);
    map.put("nullable", this.nullable);
    map.put("source_iceberg_data_type", this.sourceIcebergDataType);
    map.put("fields", Arrays.toString(this.objectFields));
    return map.toString();
  }

  public static class ObjectField {
    private String fieldName;
    private ObjectFieldType fieldType;

    public String getFieldName() {
      return fieldName;
    }

    @JsonProperty("fieldName")
    public void setFieldName(String fieldName) {
      this.fieldName = fieldName;
    }

    public ObjectFieldType getFieldType() {
      return fieldType;
    }

    @JsonProperty("fieldType")
    public void setFieldType(ObjectFieldType fieldType) {
      this.fieldType = fieldType;
    }

    @Override
    public String toString() {
      return "ObjectField{" +
              "fieldName='" + fieldName + '\'' +
              ", fieldType=" + fieldType +
              '}';
    }
  }

  public static class ObjectFieldType {
    private String type;
    private Integer length;
    private Integer byteLength;
    private Boolean nullable;
    private Boolean fixed;
    private Integer precision;
    private Integer scale;

    public String getType() {
      return type;
    }

    @JsonProperty("type")
    public void setType(String type) {
      this.type = type;
    }

    public Integer getLength() {
      return length;
    }

    @JsonProperty("length")
    public void setLength(Integer length) {
      this.length = length;
    }

    public Integer getByteLength() {
      return byteLength;
    }

    @JsonProperty("byteLength")
    public void setByteLength(Integer byteLength) {
      this.byteLength = byteLength;
    }

    public Boolean getNullable() {
      return nullable;
    }

    @JsonProperty("nullable")
    public void setNullable(Boolean nullable) {
      this.nullable = nullable;
    }

    public Boolean getFixed() {
      return fixed;
    }

    @JsonProperty("fixed")
    public void setFixed(Boolean fixed) {
      this.fixed = fixed;
    }

    public Integer getPrecision() {
      return precision;
    }

    @JsonProperty("precision")
    public void setPrecision(Integer precision) {
      this.precision = precision;
    }

    public Integer getScale() {
      return scale;
    }

    @JsonProperty("scale")
    public void setScale(Integer scale) {
      this.scale = scale;
    }

    @Override
    public String toString() {
      return "ObjectFieldType{" +
              "type='" + type + '\'' +
              ", length=" + length +
              ", byteLength=" + byteLength +
              ", nullable=" + nullable +
              ", fixed=" + fixed +
              '}';
    }
  }
}
