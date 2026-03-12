/*
 * Copyright (c) 2026 Snowflake Computing Inc. All rights reserved.
 */

package com.snowflake.kafka.connector.internal.schemaevolution;

import com.snowflake.kafka.connector.internal.validation.ValidationResult;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Maps {@link ValidationResult} to {@link SchemaEvolutionTargetItems}, re-quoting column names so
 * they match the format expected by {@link TableSchemaResolver} and DDL operations.
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
    Set<String> extraColNames = quoteAll(result.getExtraColNames());
    Set<String> columnsToDropNonNull =
        quoteAll(
            Stream.concat(
                    result.getMissingNotNullColNames().stream(),
                    result.getNullValueForNotNullColNames().stream())
                .collect(Collectors.toSet()));

    return new SchemaEvolutionTargetItems(tableName, columnsToDropNonNull, extraColNames);
  }

  /** Wrap each name in double quotes to match the format used by {@link TableSchemaResolver}. */
  private static Set<String> quoteAll(Set<String> names) {
    return names.stream().map(name -> "\"" + name + "\"").collect(Collectors.toSet());
  }
}
