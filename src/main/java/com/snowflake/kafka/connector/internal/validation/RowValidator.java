/*
 * Copyright (c) 2026 Snowflake Computing Inc. All rights reserved.
 *
 * This file provides integration between SSv1 validation code and KC v4.
 */

package com.snowflake.kafka.connector.internal.validation;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.snowflake.kafka.connector.Utils;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.ZoneId;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Validates rows against a table schema using SSv1 validation logic. This is the main facade that
 * integrates DataValidationUtil with KC v4.
 *
 * <p>Thread-safety: This class is thread-safe. The schema map is immutably captured at construction
 * time. Multiple threads can safely call validateRow() on the same RowValidator instance
 * concurrently.
 */
public class RowValidator {
  private static final Logger logger = LoggerFactory.getLogger(RowValidator.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private final Map<String, ColumnSchema> columnSchemaMap;

  /**
   * Default timezone for timestamp parsing, matching SSv1 SDK behavior.
   *
   * <p>When parsing timestamps without timezone information (e.g., "2024-03-06 10:00:00"), this
   * timezone determines how the timestamp is interpreted. Must match SSv1 SDK's
   * OpenChannelRequest.DEFAULT_DEFAULT_TIMEZONE to ensure identical validation behavior.
   *
   * <p>SSv1 SDK uses America/Los_Angeles, not UTC.
   */
  private final ZoneId defaultTimezone = ZoneId.of("America/Los_Angeles");

  public RowValidator(Map<String, ColumnSchema> columnSchemaMap) {
    // Input validation
    Objects.requireNonNull(columnSchemaMap, "columnSchemaMap cannot be null");
    if (columnSchemaMap.isEmpty()) {
      throw new IllegalArgumentException("columnSchemaMap cannot be empty");
    }

    // Defensive copy for thread safety
    this.columnSchemaMap = Collections.unmodifiableMap(new HashMap<>(columnSchemaMap));
  }

  /**
   * Validate a row against the table schema. Performs both structural validation (column presence,
   * NOT NULL checks) and type/value validation.
   *
   * <p><b>Side effect:</b> For BINARY columns, hex string values in the row are replaced in-place
   * with their {@code byte[]} equivalents so the Ingest SDK receives an unambiguous type.
   *
   * @param row Map of column name to value (may be mutated for BINARY normalization)
   * @return ValidationResult indicating success or failure with error details
   */
  public ValidationResult validateRow(Map<String, Object> row) {
    // Input validation
    Objects.requireNonNull(row, "row cannot be null");

    // Column names are expected to be already normalized (raw internal names) by the caller.
    // When column identifier normalization is enabled, SnowflakeSinkRecord sanitizes keys
    // at record creation time. DESCRIBE TABLE results are already raw names.

    // Step 1: Structural validation (matching AbstractRowBuffer.verifyInputColumns)
    Set<String> colNames = row.keySet();
    Set<String> extraCols = detectExtraColumns(colNames);
    Set<String> missingNotNullCols = detectMissingNotNullColumns(colNames);
    Set<String> nullNotNullCols = detectNullValuesInNotNullColumns(row);

    if (!extraCols.isEmpty() || !missingNotNullCols.isEmpty() || !nullNotNullCols.isEmpty()) {
      return ValidationResult.structuralError(extraCols, missingNotNullCols, nullNotNullCols);
    }

    // Step 2: Type/value validation (dispatch to DataValidationUtil)
    for (Map.Entry<String, Object> entry : row.entrySet()) {
      String colName = entry.getKey();
      Object value = entry.getValue();
      ColumnSchema col = columnSchemaMap.get(colName);

      // These conditions should have been caught by structural validation above.
      // If we reach here, it indicates a bug in structural validation logic.
      if (col == null) {
        throw new IllegalStateException(
            "Column "
                + colName
                + " not found in schema but was not caught by structural validation");
      }
      if (value == null) {
        // Null values are valid for nullable columns, skip type validation
        if (col.isNullable()) {
          continue; // Valid null for nullable column
        }
        // Null value in NOT NULL column should have been caught by structural validation
        throw new IllegalStateException(
            "Null value for NOT NULL column "
                + colName
                + " but was not caught by structural validation");
      }

      // Skip type validation for the legacy RECORD_CONTENT wrapper column.
      // In non-schematized mode, this column contains the raw payload (e.g. a plain string, bytes,
      // or object) and should accept any value the connector places there.
      // Otherwise, VARIANT client-side validation requires that the payload is a complex object.
      if (Utils.TABLE_COLUMN_CONTENT.equals(colName)) {
        continue;
      }

      try {
        Object normalized = validateAndNormalizeColumnValue(col, value);
        // Reference equality: same object returned for types that don't need normalization
        if (normalized != value) {
          entry.setValue(normalized);
        }
      } catch (SFExceptionValidation e) {
        return ValidationResult.typeError(colName, e.getMessage());
      }
    }

    return ValidationResult.valid();
  }

  /**
   * Validate a single column value using DataValidationUtil, and return the canonical form to
   * ingest.
   */
  private Object validateAndNormalizeColumnValue(ColumnSchema col, Object value)
      throws SFExceptionValidation {
    // insertRowIndex parameter is used for error messages - use 0 for now
    final long insertRowIndex = 0;

    switch (col.getLogicalType()) {
      case BOOLEAN:
        // SSv2 SDK only accepts Boolean — normalize to avoid silent drops.
        // Pre-reject non-0/1 Numbers for KC v3 parity: KC v3's StreamingRecordMapper stringified
        // all values, and SSv1's convertStringToBoolean rejects e.g. "42".
        if (value instanceof Number && !(value instanceof Boolean)) {
          BigDecimal bd = new BigDecimal(value.toString());
          if (bd.compareTo(BigDecimal.ZERO) != 0 && bd.compareTo(BigDecimal.ONE) != 0) {
            throw DataValidationUtil.valueFormatNotAllowedException(
                col.getName(),
                "BOOLEAN",
                "Only 0 and 1 are accepted for numeric boolean values",
                insertRowIndex);
          }
        }
        return DataValidationUtil.validateAndParseBoolean(col.getName(), value, insertRowIndex) == 1
            ? Boolean.TRUE
            : Boolean.FALSE;

      case FIXED:
        // DataValidationUtil.validateAndParseBigDecimal doesn't check precision/scale
        // It just parses the value.
        BigDecimal parsedNumber =
            DataValidationUtil.validateAndParseBigDecimal(col.getName(), value, insertRowIndex);
        // SSv1 enforced this during Parquet/SB16 serialization.
        // We must range-check the value here.
        int columnScale = col.getScale() != null ? col.getScale() : 0;
        int columnPrecision = col.getPrecision() != null ? col.getPrecision() : 38;
        // Round excess fractional digits to the column scale first, mirroring the server: it rounds
        // the fraction and only errors when the *rounded* integer part exceeds (precision - scale)
        // digits. The original value is still passed to the SDK unchanged; this scaled copy is used
        // only for the range check.
        BigDecimal scaledNumber = parsedNumber.setScale(columnScale, RoundingMode.HALF_UP);
        DataValidationUtil.checkValueInRange(
            col.getName(), scaledNumber, columnScale, columnPrecision, insertRowIndex);
        break;

      case REAL:
        DataValidationUtil.validateAndParseReal(col.getName(), value, insertRowIndex);
        break;

      case TEXT:
      case CHAR:
        // DVU.validateAndParseString only accepts String, Number, boolean, char — it rejects
        // Map/Collection.  However, KC v3's StreamingRecordMapper serialized all non-textual
        // JsonNodes to JSON strings via Jackson before the SDK saw them.  We replicate that
        // pipeline-level serialization so v4-compat handles Map/Collection inputs the same way.
        if (value instanceof Map || value instanceof Collection) {
          try {
            String json = OBJECT_MAPPER.writeValueAsString(value);
            DataValidationUtil.validateAndParseString(
                col.getName(), json, Optional.ofNullable(col.getLength()), insertRowIndex);
            return json;
          } catch (JsonProcessingException e) {
            throw DataValidationUtil.valueFormatNotAllowedException(
                col.getName(),
                "STRING",
                "Cannot serialize " + value.getClass().getSimpleName() + " to JSON",
                insertRowIndex);
          }
        }
        DataValidationUtil.validateAndParseString(
            col.getName(), value, Optional.ofNullable(col.getLength()), insertRowIndex);
        break;

      case BINARY:
        // The SSv2 interprets String values for BINARY columns as either hex or base64
        // depending on the server-side parameter ENABLE_SSV2_DEFAULT_BINARY_FORMAT_BASE64.
        // Returning byte[] sidesteps this ambiguity: byte[] is accepted uniformly regardless of
        // how that parameter is set.
        return DataValidationUtil.validateAndParseBinary(
            col.getName(), value, Optional.ofNullable(col.getByteLength()), insertRowIndex);

      case DATE:
        DataValidationUtil.validateAndParseDate(col.getName(), value, insertRowIndex);
        break;

      case TIME:
        DataValidationUtil.validateAndParseTime(
            col.getName(), value, col.getScale() != null ? col.getScale() : 9, insertRowIndex);
        break;

      case TIMESTAMP_NTZ:
        return validateAndNormalizeTimestamp(col, value, /* trimTimezone= */ true, insertRowIndex);

      case TIMESTAMP_LTZ:
      case TIMESTAMP_TZ:
        return validateAndNormalizeTimestamp(col, value, /* trimTimezone= */ false, insertRowIndex);

      case VARIANT:
        // When input is a String, the SSv2 SDK stores it as a JSON-quoted string (e.g.
        // '{"a":1}' → '"{\\"a\\":1}"'), whereas SSv1 stored the parsed native object.
        // validateAndParseVariantAsObject returns a native Java object (Map/List/primitive)
        // so the SDK receives the right type.
        if (value instanceof String) {
          return DataValidationUtil.validateAndParseVariantAsObject(
              col.getName(), value, insertRowIndex);
        }
        DataValidationUtil.validateAndParseVariant(col.getName(), value, insertRowIndex);
        break;

      case ARRAY:
        // SSv2 SDK wraps a String value for an ARRAY column as a single-element array (e.g.
        // "[1,2,3]" → ["[1,2,3]"]), while SSv1 parsed the string into a proper array.
        // validateAndParseArrayAsList returns a native List so the SDK gets the right type.
        if (value instanceof String) {
          return DataValidationUtil.validateAndParseArrayAsList(
              col.getName(), value, insertRowIndex);
        }
        DataValidationUtil.validateAndParseArray(col.getName(), value, insertRowIndex);
        break;

      case OBJECT:
        // No normalization needed: SSv2 SDK correctly parses JSON strings for OBJECT columns
        // (unlike VARIANT/ARRAY). Passing the original String value through is safe.
        DataValidationUtil.validateAndParseObject(col.getName(), value, insertRowIndex);
        break;

      default:
        throw new SFExceptionValidation(
            ErrorCode.UNKNOWN_DATA_TYPE, col.getName(), col.getLogicalType());
    }
    return value;
  }

  /**
   * Validate and optionally normalize a timestamp value. Integer/Long epoch values are converted to
   * ISO strings so the SSv2 SDK interprets them correctly; other types are validated in place.
   */
  private Object validateAndNormalizeTimestamp(
      ColumnSchema col, Object value, boolean trimTimezone, long insertRowIndex)
      throws SFExceptionValidation {
    if (value instanceof Integer || value instanceof Long) {
      return DataValidationUtil.validateAndFormatTimestamp(
          col.getName(), value, defaultTimezone, trimTimezone, insertRowIndex);
    }
    DataValidationUtil.validateAndParseTimestamp(
        col.getName(),
        value,
        col.getScale() != null ? col.getScale() : 9,
        defaultTimezone,
        trimTimezone,
        insertRowIndex);
    return value;
  }

  /** Detect columns in the row that don't exist in the table schema. */
  private Set<String> detectExtraColumns(Set<String> unquotedRowCols) {
    Set<String> extraCols = new HashSet<>();
    for (String unquotedName : unquotedRowCols) {
      if (!columnSchemaMap.containsKey(unquotedName)) {
        extraCols.add(unquotedName);
      }
    }
    return extraCols;
  }

  /** Detect NOT NULL columns that are missing from the row, excluding server-filled columns. */
  private Set<String> detectMissingNotNullColumns(Set<String> unquotedRowCols) {
    Set<String> missingNotNullCols = new HashSet<>();
    for (Map.Entry<String, ColumnSchema> entry : columnSchemaMap.entrySet()) {
      String colName = entry.getKey();
      ColumnSchema col = entry.getValue();

      if (!col.isNullable() && !col.isServerFilled() && !unquotedRowCols.contains(colName)) {
        missingNotNullCols.add(colName);
      }
    }
    return missingNotNullCols;
  }

  /** Detect NOT NULL columns that have null values in the row. */
  private Set<String> detectNullValuesInNotNullColumns(Map<String, Object> normalizedRow) {
    Set<String> nullNotNullCols = new HashSet<>();
    for (Map.Entry<String, Object> entry : normalizedRow.entrySet()) {
      String colName = entry.getKey(); // Already normalized

      // Validate column name is not empty
      if (colName == null || colName.trim().isEmpty()) {
        logger.warn("Skipping validation for empty column name");
        continue;
      }

      Object value = entry.getValue();

      ColumnSchema col = columnSchemaMap.get(colName);
      if (col != null && !col.isNullable() && value == null) {
        nullNotNullCols.add(colName);
      }
    }
    return nullNotNullCols;
  }

  /**
   * Static validator for unsupported types at channel open time. Throws SFExceptionValidation if
   * the schema contains unsupported types.
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
            ErrorCode.UNSUPPORTED_DATA_TYPE, "Collated columns not supported", col.getName());
      }

      // GEOGRAPHY and GEOMETRY are not in ColumnLogicalType enum
      // They would show up as null logicalType and be caught above
    }
  }
}
