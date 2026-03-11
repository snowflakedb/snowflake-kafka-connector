/*
 * Copyright (c) 2026 Snowflake Computing Inc. All rights reserved.
 *
 * This file provides integration between SSv1 validation code and KC v4.
 */

package com.snowflake.kafka.connector.internal.validation;

import java.math.BigDecimal;
import java.time.ZoneId;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Validates rows against a table schema using SSv1 validation logic.
 * This is the main facade that integrates DataValidationUtil with KC v4.
 */
public class RowValidator {
  private final Map<String, ColumnSchema> columnSchemaMap;
  private final ZoneId defaultTimezone = ZoneId.of("UTC");

  public RowValidator(Map<String, ColumnSchema> columnSchemaMap) {
    this.columnSchemaMap = columnSchemaMap;
  }

  /**
   * Normalize column name using the same logic as Snowflake (unquoted names are uppercased).
   * This is a public wrapper around LiteralQuoteUtils.unquoteColumnName().
   *
   * @param columnName column name to normalize
   * @return normalized column name
   */
  public static String normalizeColumnName(String columnName) {
    return LiteralQuoteUtils.unquoteColumnName(columnName);
  }

  /**
   * Validate a row against the table schema.
   * Performs both structural validation (column presence, NOT NULL checks)
   * and type/value validation.
   *
   * @param row Map of column name to value
   * @return ValidationResult indicating success or failure with error details
   */
  public ValidationResult validateRow(Map<String, Object> row) {
    // Pre-compute unquoted row column names once for efficiency
    Set<String> unquotedRowCols = new HashSet<>();
    for (String colName : row.keySet()) {
      unquotedRowCols.add(LiteralQuoteUtils.unquoteColumnName(colName));
    }

    // Step 1: Structural validation (matching AbstractRowBuffer.verifyInputColumns)
    Set<String> extraCols = detectExtraColumns(unquotedRowCols);
    Set<String> missingNotNullCols = detectMissingNotNullColumns(unquotedRowCols);
    Set<String> nullNotNullCols = detectNullValuesInNotNullColumns(row);

    if (!extraCols.isEmpty() || !missingNotNullCols.isEmpty() || !nullNotNullCols.isEmpty()) {
      return ValidationResult.structuralError(extraCols, missingNotNullCols, nullNotNullCols);
    }

    // Step 2: Type/value validation (dispatch to DataValidationUtil)
    for (Map.Entry<String, Object> entry : row.entrySet()) {
      String colName = LiteralQuoteUtils.unquoteColumnName(entry.getKey());
      Object value = entry.getValue();
      ColumnSchema col = columnSchemaMap.get(colName);

      if (col == null) {
        continue; // Already caught as extra column in structural validation
      }
      if (value == null) {
        continue; // Null handling done in structural validation
      }

      try {
        validateColumnValue(col, value);
      } catch (SFExceptionValidation e) {
        return ValidationResult.typeError(colName, e.getMessage());
      }
    }

    return ValidationResult.valid();
  }

  /**
   * Validate a single column value using DataValidationUtil.
   */
  private void validateColumnValue(ColumnSchema col, Object value) throws SFExceptionValidation {
    // insertRowIndex parameter is used for error messages - use 0 for now
    final long insertRowIndex = 0;

    switch (col.getLogicalType()) {
      case BOOLEAN:
        // Boolean doesn't have a dedicated validation method in DataValidationUtil
        // Simple type coercion will be handled by the SDK
        if (!(value instanceof Boolean) && !(value instanceof String) && !(value instanceof Number)) {
          throw new SFExceptionValidation(
              ErrorCode.INVALID_FORMAT_ROW,
              String.format("Invalid boolean value for column %s", col.getName()));
        }
        break;

      case FIXED:
        // Parse and validate NUMBER(precision, scale)
        // Following SSv1 pattern: parse -> setScale -> checkValueInRange
        BigDecimal bigDecimalValue =
            DataValidationUtil.validateAndParseBigDecimal(col.getName(), value, insertRowIndex);

        int scale = col.getScale() != null ? col.getScale() : 0;
        int precision = col.getPrecision() != null ? col.getPrecision() : 38;

        // Match scale to column definition
        bigDecimalValue = bigDecimalValue.setScale(scale, java.math.RoundingMode.HALF_UP);

        // Validate value fits within precision/scale bounds
        DataValidationUtil.checkValueInRange(
            col.getName(), bigDecimalValue, scale, precision, insertRowIndex);
        break;

      case REAL:
        DataValidationUtil.validateAndParseReal(col.getName(), value, insertRowIndex);
        break;

      case TEXT:
      case CHAR:
        DataValidationUtil.validateAndParseString(
            col.getName(),
            value,
            java.util.Optional.ofNullable(col.getLength()),
            insertRowIndex);
        break;

      case BINARY:
        DataValidationUtil.validateAndParseBinary(
            col.getName(),
            value,
            java.util.Optional.ofNullable(col.getByteLength()),
            insertRowIndex);
        break;

      case DATE:
        DataValidationUtil.validateAndParseDate(col.getName(), value, insertRowIndex);
        break;

      case TIME:
        DataValidationUtil.validateAndParseTime(
            col.getName(), value, col.getScale() != null ? col.getScale() : 9, insertRowIndex);
        break;

      case TIMESTAMP_NTZ:
        // trimTimezone=true for NTZ columns
        DataValidationUtil.validateAndParseTimestamp(
            col.getName(),
            value,
            col.getScale() != null ? col.getScale() : 9,
            defaultTimezone,
            true,
            insertRowIndex);
        break;

      case TIMESTAMP_LTZ:
        // trimTimezone=false for LTZ columns
        DataValidationUtil.validateAndParseTimestamp(
            col.getName(),
            value,
            col.getScale() != null ? col.getScale() : 9,
            defaultTimezone,
            false,
            insertRowIndex);
        break;

      case TIMESTAMP_TZ:
        // trimTimezone=false for TZ columns
        DataValidationUtil.validateAndParseTimestamp(
            col.getName(),
            value,
            col.getScale() != null ? col.getScale() : 9,
            defaultTimezone,
            false,
            insertRowIndex);
        break;

      case VARIANT:
        DataValidationUtil.validateAndParseVariant(col.getName(), value, insertRowIndex);
        break;

      case ARRAY:
        DataValidationUtil.validateAndParseArray(col.getName(), value, insertRowIndex);
        break;

      case OBJECT:
        DataValidationUtil.validateAndParseObject(col.getName(), value, insertRowIndex);
        break;

      default:
        throw new SFExceptionValidation(
            ErrorCode.UNKNOWN_DATA_TYPE,
            col.getName(),
            col.getLogicalType());
    }
  }

  /**
   * Detect columns in the row that don't exist in the table schema.
   */
  private Set<String> detectExtraColumns(Set<String> unquotedRowCols) {
    Set<String> extraCols = new HashSet<>();
    for (String unquotedName : unquotedRowCols) {
      if (!columnSchemaMap.containsKey(unquotedName)) {
        extraCols.add(unquotedName);
      }
    }
    return extraCols;
  }

  /**
   * Detect NOT NULL columns that are missing from the row.
   */
  private Set<String> detectMissingNotNullColumns(Set<String> unquotedRowCols) {
    Set<String> missingNotNullCols = new HashSet<>();
    for (Map.Entry<String, ColumnSchema> entry : columnSchemaMap.entrySet()) {
      String colName = entry.getKey();
      ColumnSchema col = entry.getValue();

      if (!col.isNullable() && !unquotedRowCols.contains(colName)) {
        missingNotNullCols.add(colName);
      }
    }
    return missingNotNullCols;
  }

  /**
   * Detect NOT NULL columns that have null values in the row.
   */
  private Set<String> detectNullValuesInNotNullColumns(Map<String, Object> row) {
    Set<String> nullNotNullCols = new HashSet<>();
    for (Map.Entry<String, Object> entry : row.entrySet()) {
      String colName = LiteralQuoteUtils.unquoteColumnName(entry.getKey());
      Object value = entry.getValue();

      ColumnSchema col = columnSchemaMap.get(colName);
      if (col != null && !col.isNullable() && value == null) {
        nullNotNullCols.add(colName);
      }
    }
    return nullNotNullCols;
  }

  /**
   * Static validator for unsupported types at channel open time.
   * Throws SFException if the schema contains unsupported types.
   *
   * @param schema Map of column name to ColumnSchema
   * @throws SFExceptionValidation if unsupported types are found
   */
  public static void validateSchema(Map<String, ColumnSchema> schema) throws SFExceptionValidation {
    for (ColumnSchema col : schema.values()) {
      if (col.getLogicalType() == null) {
        throw new SFExceptionValidation(ErrorCode.UNKNOWN_DATA_TYPE, col.getName());
      }

      // Reject collated columns (not supported in SSv1 validation)
      if (col.getCollation() != null && !col.getCollation().isEmpty()) {
        throw new SFExceptionValidation(
            ErrorCode.UNSUPPORTED_DATA_TYPE,
            "Collated columns not supported",
            col.getName());
      }

      // GEOGRAPHY and GEOMETRY are not in ColumnLogicalType enum
      // They would show up as null logicalType and be caught above
    }
  }
}
