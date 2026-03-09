/*
 * Copyright (c) 2026 Snowflake Computing Inc. All rights reserved.
 *
 * NEW file for KC v4 - adapted from KC v3.2's InsertErrorMapper.
 * Maps ValidationResult to SchemaEvolutionTargetItems for client-side schema evolution.
 */

package com.snowflake.kafka.connector.internal.schemaevolution;

import com.snowflake.kafka.connector.internal.validation.ValidationResult;
import java.util.HashSet;
import java.util.Set;

/**
 * Maps ValidationResult structural errors to SchemaEvolutionTargetItems.
 *
 * <p>Combines missing NOT NULL columns and null values in NOT NULL columns into a single set of
 * columns that need to drop their NOT NULL constraint.
 */
public class ValidationResultMapper {

  /**
   * Convert ValidationResult to SchemaEvolutionTargetItems.
   *
   * @param result ValidationResult with structural error details
   * @param tableName Target table name
   * @return SchemaEvolutionTargetItems with columns to add and columns to drop NOT NULL
   */
  public static SchemaEvolutionTargetItems mapToSchemaEvolutionItems(
      ValidationResult result, String tableName) {
    Set<String> extraColNames = result.getExtraColNames();
    Set<String> columnsToDropNonNull = new HashSet<>();

    // Combine both NOT NULL violations into a single set
    columnsToDropNonNull.addAll(result.getMissingNotNullColNames());
    columnsToDropNonNull.addAll(result.getNullValueForNotNullColNames());

    return new SchemaEvolutionTargetItems(tableName, columnsToDropNonNull, extraColNames);
  }
}
