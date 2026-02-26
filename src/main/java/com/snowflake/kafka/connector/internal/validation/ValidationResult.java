/*
 * Copyright (c) 2026 Snowflake Computing Inc. All rights reserved.
 *
 * This file provides integration between SSv1 validation code and KC v4.
 */

package com.snowflake.kafka.connector.internal.validation;

import java.util.Collections;
import java.util.Set;

/**
 * Result of row validation containing validation status and error details.
 */
public class ValidationResult {
  private final boolean valid;
  private final boolean hasTypeError;
  private final boolean hasStructuralError;
  private final String valueError;
  private final String columnName;
  private final Set<String> extraColNames;
  private final Set<String> missingNotNullColNames;
  private final Set<String> nullValueForNotNullColNames;

  private ValidationResult(
      boolean valid,
      boolean hasTypeError,
      boolean hasStructuralError,
      String valueError,
      String columnName,
      Set<String> extraColNames,
      Set<String> missingNotNullColNames,
      Set<String> nullValueForNotNullColNames) {
    this.valid = valid;
    this.hasTypeError = hasTypeError;
    this.hasStructuralError = hasStructuralError;
    this.valueError = valueError;
    this.columnName = columnName;
    this.extraColNames = extraColNames != null ? extraColNames : Collections.emptySet();
    this.missingNotNullColNames = missingNotNullColNames != null ? missingNotNullColNames : Collections.emptySet();
    this.nullValueForNotNullColNames = nullValueForNotNullColNames != null ? nullValueForNotNullColNames : Collections.emptySet();
  }

  /** Create a valid result */
  public static ValidationResult valid() {
    return new ValidationResult(true, false, false, null, null, null, null, null);
  }

  /** Create a type/value error result */
  public static ValidationResult typeError(String columnName, String errorMessage) {
    return new ValidationResult(false, true, false, errorMessage, columnName, null, null, null);
  }

  /** Create a structural error result */
  public static ValidationResult structuralError(
      Set<String> extraColNames,
      Set<String> missingNotNullColNames,
      Set<String> nullValueForNotNullColNames) {
    return new ValidationResult(
        false,
        false,
        true,
        null,
        null,
        extraColNames,
        missingNotNullColNames,
        nullValueForNotNullColNames);
  }

  public boolean isValid() {
    return valid;
  }

  public boolean hasTypeError() {
    return hasTypeError;
  }

  public boolean hasStructuralError() {
    return hasStructuralError;
  }

  public String getValueError() {
    return valueError;
  }

  public String getColumnName() {
    return columnName;
  }

  public Set<String> getExtraColNames() {
    return extraColNames;
  }

  public Set<String> getMissingNotNullColNames() {
    return missingNotNullColNames;
  }

  public Set<String> getNullValueForNotNullColNames() {
    return nullValueForNotNullColNames;
  }

  /**
   * Check if this structural error indicates a need for schema evolution.
   * True if there are extra columns or NOT NULL constraint violations.
   */
  public boolean needsSchemaEvolution() {
    return hasStructuralError
        && (!extraColNames.isEmpty()
            || !missingNotNullColNames.isEmpty()
            || !nullValueForNotNullColNames.isEmpty());
  }

  public String getErrorType() {
    if (hasTypeError) {
      return "type_error";
    } else if (hasStructuralError) {
      return "structural_error";
    } else {
      return "unknown";
    }
  }
}
