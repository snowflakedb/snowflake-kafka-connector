/*
 * Copyright (c) 2026 Snowflake Computing Inc. All rights reserved.
 *
 * Ported from KC v3.2 for client-side schema evolution in KC v4.
 * Adapted to use Sets instead of Lists for KC v4 ValidationResult compatibility.
 */

package com.snowflake.kafka.connector.internal.schemaevolution;

import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nonnull;

/**
 * Contains target items for schema evolution: table name, columns to drop non-nullability, and
 * columns to add to the table.
 *
 * <p>Note: Uses Sets instead of Lists (KC v3.2 used Lists) to match ValidationResult structure.
 */
public class SchemaEvolutionTargetItems {
  private final String tableName;

  @Nonnull private final Set<String> columnsToDropNonNullability;
  @Nonnull private final Set<String> columnsToAdd;

  public SchemaEvolutionTargetItems(
      String tableName, Set<String> columnsToDropNonNullability, Set<String> columnsToAdd) {
    this.tableName = tableName;
    this.columnsToDropNonNullability =
        columnsToDropNonNullability != null
            ? new HashSet<>(columnsToDropNonNullability)
            : Collections.emptySet();
    this.columnsToAdd = columnsToAdd != null ? new HashSet<>(columnsToAdd) : Collections.emptySet();
  }

  public boolean hasDataForSchemaEvolution() {
    return !columnsToDropNonNullability.isEmpty() || !columnsToAdd.isEmpty();
  }

  public SchemaEvolutionTargetItems(String tableName, Set<String> columnsToAdd) {
    this(tableName, null, columnsToAdd);
  }

  public String getTableName() {
    return tableName;
  }

  @Nonnull
  public Set<String> getColumnsToDropNonNullability() {
    return Collections.unmodifiableSet(columnsToDropNonNullability);
  }

  @Nonnull
  public Set<String> getColumnsToAdd() {
    return Collections.unmodifiableSet(columnsToAdd);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    SchemaEvolutionTargetItems that = (SchemaEvolutionTargetItems) o;
    return Objects.equals(tableName, that.tableName)
        && Objects.equals(columnsToDropNonNullability, that.columnsToDropNonNullability)
        && Objects.equals(columnsToAdd, that.columnsToAdd);
  }

  @Override
  public int hashCode() {
    return Objects.hash(tableName, columnsToDropNonNullability, columnsToAdd);
  }

  @Override
  public String toString() {
    return "SchemaEvolutionTargetItems{"
        + "tableName='"
        + tableName
        + '\''
        + ", nonNullableColumns="
        + columnsToDropNonNullability
        + ", extraColNames="
        + columnsToAdd
        + '}';
  }
}
