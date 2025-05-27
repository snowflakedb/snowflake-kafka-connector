package com.snowflake.kafka.connector.internal.streaming.validation;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

public class RowsetApiColumnsEndpointResponse {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private final List<TableColumn> tableColumns;
  private final String errorCode;

  public static RowsetApiColumnsEndpointResponse from(InputStream src) throws IOException {
    return OBJECT_MAPPER.readValue(src, RowsetApiColumnsEndpointResponse.class);
  }

  public RowsetApiColumnsEndpointResponse(
      @JsonProperty("table_columns") List<TableColumn> tableColumns,
      @JsonProperty("error_code") String errorCode) {
    this.tableColumns = tableColumns;
    this.errorCode = errorCode;
  }

  public List<TableColumn> tableColumns() {
    return tableColumns;
  }

  public String getErrorCode() {
    return errorCode;
  }

  public static class TableColumn {
    final String columnName;
    final DataType dataType;
    final String physicalType;
    final String sourceIcebergDataType;
    final boolean nullability;
    final String collation;
    final String defaultValue;

    public TableColumn(
        @JsonProperty("column_name") String columnName,
        @JsonProperty("data_type") String dataType,
        @JsonProperty("physical_type") String physicalType,
        @JsonProperty("source_iceberg_data_type") String sourceIcebergDataType,
        @JsonProperty("null?") boolean nullability,
        @JsonProperty("collation") String collation,
        @JsonProperty("default") String defaultValue)
        throws JsonProcessingException {
      this.columnName = columnName;
      this.dataType = OBJECT_MAPPER.readValue(dataType, DataType.class);
      this.physicalType = physicalType;
      this.sourceIcebergDataType = sourceIcebergDataType;
      this.nullability = nullability;
      this.collation = collation;
      this.defaultValue = defaultValue;
    }

    public String getColumnName() {
      return columnName;
    }

    public DataType getDataType() {
      return dataType;
    }

    public String getPhysicalType() {
      return physicalType;
    }

    public String getSourceIcebergDataType() {
      return sourceIcebergDataType;
    }

    public boolean isNullable() {
      return nullability;
    }

    public String getCollation() {
      return collation;
    }

    public String getDefaultValue() {
      return defaultValue;
    }
  }

  static class DataType {
    private final String type;
    private final boolean nullable;
    private final Long length;
    private final Long byteLength;
    private final Boolean fixed;
    private final Integer precision;
    private final Integer scale;
    private final List<Field> fields;
    private final DataType keyType;
    private final DataType valueType;
    private final DataType elementType;

    DataType(
        @JsonProperty("type") String type,
        @JsonProperty("nullable") boolean nullable,
        @JsonProperty("length") Long length,
        @JsonProperty("byteLength") Long byteLength,
        @JsonProperty("fixed") Boolean fixed,
        @JsonProperty("precision") Integer precision,
        @JsonProperty("scale") Integer scale,
        @JsonProperty("fields") List<Field> fields,
        @JsonProperty("keyType") DataType keyType,
        @JsonProperty("valueType") DataType valueType,
        @JsonProperty("elementType") DataType elementType) {
      this.type = type;
      this.nullable = nullable;
      this.length = length;
      this.byteLength = byteLength;
      this.fixed = fixed;
      this.precision = precision;
      this.scale = scale;
      this.fields = fields;
      this.keyType = keyType;
      this.valueType = valueType;
      this.elementType = elementType;
    }

    public String getType() {
      return type;
    }

    public boolean isNullable() {
      return nullable;
    }

    public Long getLength() {
      return length;
    }

    public Long getByteLength() {
      return byteLength;
    }

    public Boolean getFixed() {
      return fixed;
    }

    public Integer getPrecision() {
      return precision;
    }

    public Integer getScale() {
      return scale;
    }

    public List<Field> getFields() {
      return fields;
    }

    public DataType getKeyType() {
      return keyType;
    }

    public DataType getValueType() {
      return valueType;
    }

    public DataType getElementType() {
      return elementType;
    }
  }

  class Field {
    private final String fieldName;
    private final DataType fieldType;

    Field(
        @JsonProperty("fieldName") String fieldName,
        @JsonProperty("fieldType") DataType fieldType) {
      this.fieldName = fieldName;
      this.fieldType = fieldType;
    }
  }
}
