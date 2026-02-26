/*
 * Copyright (c) 2026 Snowflake Computing Inc. All rights reserved.
 *
 * This file provides integration between SSv1 validation code and KC v4.
 */

package com.snowflake.kafka.connector.internal.validation;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Represents the schema of a Snowflake table column for validation purposes.
 * Constructed from JDBC ResultSet (DESCRIBE TABLE or system function).
 */
public class ColumnSchema {
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
   * Temporary workaround until system function is available.
   *
   * @param rs ResultSet positioned at a DESCRIBE TABLE row
   * @return ColumnSchema
   * @throws SQLException if column metadata cannot be read
   */
  public static ColumnSchema fromDescribeTableRow(ResultSet rs) throws SQLException {
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

  private static class TypeInfo {
    ColumnLogicalType logicalType;
    ColumnPhysicalType physicalType;
    Integer precision;
    Integer scale;
    Integer length;
    Integer byteLength;
  }

  /**
   * Parse Snowflake type string (e.g., "NUMBER(38,0)", "VARCHAR(16777216)") into TypeInfo.
   */
  private static TypeInfo parseTypeString(String typeStr) {
    TypeInfo info = new TypeInfo();

    // Extract base type and parameters
    String baseType;
    String params = null;
    int parenIdx = typeStr.indexOf('(');
    if (parenIdx > 0) {
      baseType = typeStr.substring(0, parenIdx).trim().toUpperCase();
      int closeParenIdx = typeStr.indexOf(')', parenIdx);
      if (closeParenIdx > 0) {
        params = typeStr.substring(parenIdx + 1, closeParenIdx).trim();
      }
    } else {
      baseType = typeStr.trim().toUpperCase();
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
          info.precision = Integer.parseInt(parts[0].trim());
          info.scale = Integer.parseInt(parts[1].trim());
        } else if (params != null) {
          info.precision = Integer.parseInt(params.trim());
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
          info.length = Integer.parseInt(params.trim());
          info.byteLength = info.length * 4; // Max 4 bytes per UTF-8 character
        } else {
          info.length = 16777216; // Default VARCHAR max
          info.byteLength = info.length * 4;
        }
        break;

      case "BINARY":
      case "VARBINARY":
        info.logicalType = ColumnLogicalType.BINARY;
        info.physicalType = ColumnPhysicalType.BINARY;
        if (params != null) {
          info.byteLength = Integer.parseInt(params.trim());
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
          info.scale = Integer.parseInt(params.trim());
        } else {
          info.scale = 9; // Default TIME scale
        }
        break;

      case "TIMESTAMP":
      case "DATETIME":
        info.logicalType = ColumnLogicalType.TIMESTAMP_NTZ;
        info.physicalType = ColumnPhysicalType.SB8;
        if (params != null) {
          info.scale = Integer.parseInt(params.trim());
        } else {
          info.scale = 9; // Default TIMESTAMP scale
        }
        break;

      case "TIMESTAMP_LTZ":
        info.logicalType = ColumnLogicalType.TIMESTAMP_LTZ;
        info.physicalType = ColumnPhysicalType.SB8;
        if (params != null) {
          info.scale = Integer.parseInt(params.trim());
        } else {
          info.scale = 9;
        }
        break;

      case "TIMESTAMP_NTZ":
        info.logicalType = ColumnLogicalType.TIMESTAMP_NTZ;
        info.physicalType = ColumnPhysicalType.SB8;
        if (params != null) {
          info.scale = Integer.parseInt(params.trim());
        } else {
          info.scale = 9;
        }
        break;

      case "TIMESTAMP_TZ":
        info.logicalType = ColumnLogicalType.TIMESTAMP_TZ;
        info.physicalType = ColumnPhysicalType.SB8;
        if (params != null) {
          info.scale = Integer.parseInt(params.trim());
        } else {
          info.scale = 9;
        }
        break;

      case "VARIANT":
        info.logicalType = ColumnLogicalType.VARIANT;
        info.physicalType = ColumnPhysicalType.LOB;
        break;

      case "OBJECT":
        info.logicalType = ColumnLogicalType.OBJECT;
        info.physicalType = ColumnPhysicalType.LOB;
        break;

      case "ARRAY":
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
