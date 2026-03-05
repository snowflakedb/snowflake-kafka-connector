/*
 * Copyright (c) 2026 Snowflake Computing Inc. All rights reserved.
 *
 * This file provides integration between SSv1 validation code and KC v4.
 */

package com.snowflake.kafka.connector.internal.validation;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/** Result of row validation containing validation status and error details. */
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
    // Create defensive immutable copies of all sets for thread safety
    this.extraColNames = Collections.unmodifiableSet(new HashSet<>(extraColNames));
    this.missingNotNullColNames =
        Collections.unmodifiableSet(new HashSet<>(missingNotNullColNames));
    this.nullValueForNotNullColNames =
        Collections.unmodifiableSet(new HashSet<>(nullValueForNotNullColNames));
  }

  /** Create a valid result */
  public static ValidationResult valid() {
    return new ValidationResult(
        true,
        false,
        false,
        null,
        null,
        Collections.emptySet(),
        Collections.emptySet(),
        Collections.emptySet());
  }

  /** Create a type/value error result */
  public static ValidationResult typeError(String columnName, String errorMessage) {
    return new ValidationResult(
        false,
        true,
        false,
        errorMessage,
        columnName,
        Collections.emptySet(),
        Collections.emptySet(),
        Collections.emptySet());
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
   * Check if this structural error can be resolved with schema evolution. - Extra columns: YES -
   * can add via ALTER TABLE ADD COLUMN - Null in NOT NULL: YES - can drop constraint via ALTER
   * TABLE DROP NOT NULL - Missing NOT NULL columns: NO - cannot add data for missing columns
   *
   * @return true if the error can be resolved with schema evolution
   */
  public boolean needsSchemaEvolution() {
    return hasStructuralError
        && (!extraColNames.isEmpty() || !nullValueForNotNullColNames.isEmpty());
  }

  /**
   * Check if this structural error cannot be resolved with schema evolution. This is true when data
   * is missing required NOT NULL columns.
   *
   * @return true if the error is unresolvable
   */
  public boolean hasUnresolvableError() {
    return hasStructuralError && !missingNotNullColNames.isEmpty();
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
