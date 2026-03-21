/*
 * Copyright (c) 2026 Snowflake Computing Inc. All rights reserved.
 */

package com.snowflake.kafka.connector.internal.schemaevolution;

import com.snowflake.kafka.connector.internal.validation.ValidationResult;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Maps {@link ValidationResult} to {@link SchemaEvolutionTargetItems}. Column names are raw
 * internal names (as returned by DESCRIBE TABLE / as normalized at record creation time). Quoting
 * for DDL is handled downstream in {@link
 * com.snowflake.kafka.connector.internal.StandardSnowflakeConnectionService}.
 */
public class ValidationResultMapper {

  /**
   * Convert ValidationResult to SchemaEvolutionTargetItems.
   *
   * @param result ValidationResult with structural error details (raw column names)
   * @param tableName Target table name
   * @return SchemaEvolutionTargetItems with raw column names to add and columns to drop NOT NULL
   */
  public static SchemaEvolutionTargetItems mapToSchemaEvolutionItems(
      ValidationResult result, String tableName) {
    Set<String> extraColNames = result.getExtraColNames();
    Set<String> columnsToDropNonNull =
        Stream.concat(
                result.getMissingNotNullColNames().stream(),
                result.getNullValueForNotNullColNames().stream())
            .collect(Collectors.toSet());

    return new SchemaEvolutionTargetItems(tableName, columnsToDropNonNull, extraColNames);
  }
}
