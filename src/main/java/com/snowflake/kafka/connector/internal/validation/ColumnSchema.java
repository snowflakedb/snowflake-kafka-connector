/*
 * Copyright (c) 2026 Snowflake Computing Inc. All rights reserved.
 *
 * This file provides integration between SSv1 validation code and KC v4.
 */

package com.snowflake.kafka.connector.internal.validation;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Represents the schema of a Snowflake table column for validation purposes. Constructed from JDBC
 * ResultSet (DESCRIBE TABLE or system function).
 */
public class ColumnSchema {
  /**
   * Maximum byte length for TEXT/VARCHAR columns, matching SSv1 SDK's BYTES_16_MB limit. SSv1 SDK
   * enforces that strings can never be larger than 16MB bytes, even if the VARCHAR character length
   * would theoretically allow more (e.g., VARCHAR(16777216) with 4-byte UTF-8 chars could be 64MB,
   * but is capped at 16MB).
   *
   * @see DataValidationUtil line 721 in SSv1 SDK
   */
  private static final int MAX_LOB_SIZE_BYTES = 16 * 1024 * 1024; // 16,777,216 bytes

  private final String name;
  private final ColumnLogicalType logicalType;
  private final ColumnPhysicalType physicalType;
  private final boolean nullable;
  private final Integer precision;
  private final Integer scale;
  private final Integer length;
  private final Integer byteLength;
  private final String collation;

  public ColumnSchema(
      String name,
      ColumnLogicalType logicalType,
      ColumnPhysicalType physicalType,
      boolean nullable,
      Integer precision,
      Integer scale,
      Integer length,
      Integer byteLength,
      String collation) {
    this.name = name;
    this.logicalType = logicalType;
    this.physicalType = physicalType;
    this.nullable = nullable;
    this.precision = precision;
    this.scale = scale;
    this.length = length;
    this.byteLength = byteLength;
    this.collation = collation;
  }

  /**
   * Construct ColumnSchema from DESCRIBE TABLE ResultSet row.
   *
   * <p>Thread-safety: This method is NOT thread-safe. Caller must synchronize if sharing ResultSet.
   *
   * <p>Resource management: Caller is responsible for closing the ResultSet.
   *
   * <p>ResultSet state: Must be positioned at a valid row before calling.
   *
   * @param rs ResultSet positioned at a DESCRIBE TABLE row (must not be closed)
   * @return ColumnSchema
   * @throws SQLException if column metadata cannot be read or ResultSet is closed/invalid
   * @throws IllegalArgumentException if ResultSet is null or closed
   */
  public static ColumnSchema fromDescribeTableRow(ResultSet rs) throws SQLException {
    if (rs == null || rs.isClosed()) {
      throw new IllegalArgumentException("ResultSet must be open and positioned at a row");
    }

    String name = rs.getString("name");
    String typeStr = rs.getString("type");
    String nullStr = rs.getString("null?");
    boolean nullable = "Y".equals(nullStr);

    // Parse type string to extract logical type and parameters
    TypeInfo typeInfo = parseTypeString(typeStr);

    return new ColumnSchema(
        name,
        typeInfo.logicalType,
        typeInfo.physicalType,
        nullable,
        typeInfo.precision,
        typeInfo.scale,
        typeInfo.length,
        typeInfo.byteLength,
        null); // DESCRIBE TABLE doesn't return collation
  }

  /**
   * Construct ColumnSchema from DESCRIBE TABLE field values.
   *
   * <p>This is a convenience method that takes the three key fields from DescribeTableRow.
   *
   * @param columnName column name from DESCRIBE TABLE
   * @param typeStr type string (e.g., "VARCHAR(100)", "NUMBER(38,0)")
   * @param nullableStr nullable indicator ("Y" or "N")
   * @return ColumnSchema
   * @throws IllegalArgumentException if parameters are invalid
   */
  public static ColumnSchema fromDescribeTableFields(
      String columnName, String typeStr, String nullableStr) {
    boolean nullable = "Y".equals(nullableStr);

    // Parse type string to extract logical type and parameters
    TypeInfo typeInfo = parseTypeString(typeStr);

    return new ColumnSchema(
        columnName,
        typeInfo.logicalType,
        typeInfo.physicalType,
        nullable,
        typeInfo.precision,
        typeInfo.scale,
        typeInfo.length,
        typeInfo.byteLength,
        null); // DESCRIBE TABLE doesn't return collation
  }

  private static class TypeInfo {
    ColumnLogicalType logicalType;
    ColumnPhysicalType physicalType;
    Integer precision;
    Integer scale;
    Integer length;
    Integer byteLength;
  }

  /** Parse Snowflake type string (e.g., "NUMBER(38,0)", "VARCHAR(16777216)") into TypeInfo. */
  private static TypeInfo parseTypeString(String typeStr) {
    // Input validation
    if (typeStr == null || typeStr.trim().isEmpty()) {
      throw new IllegalArgumentException("Type string cannot be null or empty");
    }

    TypeInfo info = new TypeInfo();

    // Extract base type and parameters
    String baseType;
    String params = null;
    String trimmedType = typeStr.trim();
    int parenIdx = trimmedType.indexOf('(');
    if (parenIdx > 0) {
      baseType = trimmedType.substring(0, parenIdx).toUpperCase();
      // Use lastIndexOf to handle nested types like OBJECT(a NUMBER(38,0), b VARCHAR)
      int closeParenIdx = trimmedType.lastIndexOf(')');
      if (closeParenIdx <= parenIdx) {
        throw new IllegalArgumentException(
            "Malformed type string (missing closing parenthesis): " + typeStr);
      }
      params = trimmedType.substring(parenIdx + 1, closeParenIdx).trim();
    } else {
      baseType = trimmedType.toUpperCase();
    }

    // Map to logical and physical types
    switch (baseType) {
      case "NUMBER":
      case "NUMERIC":
      case "DECIMAL":
      case "INT":
      case "INTEGER":
      case "BIGINT":
      case "SMALLINT":
      case "TINYINT":
      case "BYTEINT":
        info.logicalType = ColumnLogicalType.FIXED;
        info.physicalType = ColumnPhysicalType.SB16;
        if (params != null && params.contains(",")) {
          String[] parts = params.split(",");
          try {
            info.precision = Integer.parseInt(parts[0].trim());
            info.scale = Integer.parseInt(parts[1].trim());
          } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                "Invalid numeric parameter in type string: " + typeStr, e);
          }
        } else if (params != null) {
          try {
            info.precision = Integer.parseInt(params.trim());
          } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                "Invalid numeric parameter in type string: " + typeStr, e);
          }
          info.scale = 0;
        } else {
          info.precision = 38;
          info.scale = 0;
        }
        break;

      case "FLOAT":
      case "FLOAT4":
      case "FLOAT8":
      case "DOUBLE":
      case "DOUBLE PRECISION":
      case "REAL":
        info.logicalType = ColumnLogicalType.REAL;
        info.physicalType = ColumnPhysicalType.DOUBLE;
        break;

      case "VARCHAR":
      case "STRING":
      case "TEXT":
      case "CHAR":
      case "CHARACTER":
        info.logicalType = ColumnLogicalType.TEXT;
        info.physicalType = ColumnPhysicalType.LOB;
        if (params != null) {
          try {
            info.length = Integer.parseInt(params.trim());
          } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                "Invalid length parameter in type string: " + typeStr, e);
          }
          // Cap at MAX_LOB_SIZE_BYTES (SSv1 SDK limit: strings never exceed 16MB bytes)
          // Use long to prevent integer overflow if length is corrupted/malformed
          long byteLengthLong = (long) info.length * 4;
          info.byteLength = (int) Math.min(MAX_LOB_SIZE_BYTES, byteLengthLong);
        } else {
          info.length = 16777216; // Default VARCHAR max
          // Cap at MAX_LOB_SIZE_BYTES (SSv1 SDK limit: strings never exceed 16MB bytes)
          // Use long to prevent integer overflow if length is corrupted/malformed
          long byteLengthLong = (long) info.length * 4;
          info.byteLength = (int) Math.min(MAX_LOB_SIZE_BYTES, byteLengthLong);
        }
        break;

      case "BINARY":
      case "VARBINARY":
        info.logicalType = ColumnLogicalType.BINARY;
        info.physicalType = ColumnPhysicalType.BINARY;
        if (params != null) {
          try {
            info.byteLength = Integer.parseInt(params.trim());
          } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                "Invalid length parameter in type string: " + typeStr, e);
          }
        } else {
          info.byteLength = 8388608; // Default BINARY max
        }
        break;

      case "BOOLEAN":
        info.logicalType = ColumnLogicalType.BOOLEAN;
        info.physicalType = ColumnPhysicalType.SB1;
        break;

      case "DATE":
        info.logicalType = ColumnLogicalType.DATE;
        info.physicalType = ColumnPhysicalType.SB8;
        break;

      case "TIME":
        info.logicalType = ColumnLogicalType.TIME;
        info.physicalType = ColumnPhysicalType.SB8;
        if (params != null) {
          try {
            info.scale = Integer.parseInt(params.trim());
          } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                "Invalid scale parameter in type string: " + typeStr, e);
          }
        } else {
          info.scale = 9; // Default TIME scale
        }
        break;

      case "TIMESTAMP":
      case "DATETIME":
        info.logicalType = ColumnLogicalType.TIMESTAMP_NTZ;
        info.physicalType = ColumnPhysicalType.SB8;
        if (params != null) {
          try {
            info.scale = Integer.parseInt(params.trim());
          } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                "Invalid scale parameter in type string: " + typeStr, e);
          }
        } else {
          info.scale = 9; // Default TIMESTAMP scale
        }
        break;

      case "TIMESTAMP_LTZ":
        info.logicalType = ColumnLogicalType.TIMESTAMP_LTZ;
        info.physicalType = ColumnPhysicalType.SB8;
        if (params != null) {
          try {
            info.scale = Integer.parseInt(params.trim());
          } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                "Invalid scale parameter in type string: " + typeStr, e);
          }
        } else {
          info.scale = 9;
        }
        break;

      case "TIMESTAMP_NTZ":
        info.logicalType = ColumnLogicalType.TIMESTAMP_NTZ;
        info.physicalType = ColumnPhysicalType.SB8;
        if (params != null) {
          try {
            info.scale = Integer.parseInt(params.trim());
          } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                "Invalid scale parameter in type string: " + typeStr, e);
          }
        } else {
          info.scale = 9;
        }
        break;

      case "TIMESTAMP_TZ":
        info.logicalType = ColumnLogicalType.TIMESTAMP_TZ;
        info.physicalType = ColumnPhysicalType.SB8;
        if (params != null) {
          try {
            info.scale = Integer.parseInt(params.trim());
          } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                "Invalid scale parameter in type string: " + typeStr, e);
          }
        } else {
          info.scale = 9;
        }
        break;

      case "VARIANT":
        info.logicalType = ColumnLogicalType.VARIANT;
        info.physicalType = ColumnPhysicalType.LOB;
        break;

      case "OBJECT":
        // Reject structured OBJECT types like OBJECT(a INT, b TEXT)
        // SSv1 SDK only supports unstructured OBJECT
        if (params != null && !params.trim().isEmpty()) {
          throw new IllegalArgumentException(
              "Structured OBJECT types are not supported by Snowpipe Streaming. "
                  + "Use unstructured OBJECT instead. Type: "
                  + typeStr);
        }
        info.logicalType = ColumnLogicalType.OBJECT;
        info.physicalType = ColumnPhysicalType.LOB;
        break;

      case "ARRAY":
        // Reject structured ARRAY types like ARRAY(INT)
        // SSv1 SDK only supports unstructured ARRAY
        if (params != null && !params.trim().isEmpty()) {
          throw new IllegalArgumentException(
              "Structured ARRAY types are not supported by Snowpipe Streaming. "
                  + "Use unstructured ARRAY instead. Type: "
                  + typeStr);
        }
        info.logicalType = ColumnLogicalType.ARRAY;
        info.physicalType = ColumnPhysicalType.LOB;
        break;

      default:
        // Unknown type - will be caught by validateSchema
        info.logicalType = null;
        info.physicalType = null;
    }

    return info;
  }

  public String getName() {
    return name;
  }

  public ColumnLogicalType getLogicalType() {
    return logicalType;
  }

  public ColumnPhysicalType getPhysicalType() {
    return physicalType;
  }

  public boolean isNullable() {
    return nullable;
  }

  public Integer getPrecision() {
    return precision;
  }

  public Integer getScale() {
    return scale;
  }

  public Integer getLength() {
    return length;
  }

  public Integer getByteLength() {
    return byteLength;
  }

  public String getCollation() {
    return collation;
  }
}
