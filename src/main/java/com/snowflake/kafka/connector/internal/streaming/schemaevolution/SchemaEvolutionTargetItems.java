package com.snowflake.kafka.connector.internal.streaming.schemaevolution;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nonnull;

/**
 * This class contains the target items for schema evolution. It contains the target table name,
 * columns to drop non-nullability, and columns to add to the table.
 */
public class SchemaEvolutionTargetItems {
  private final String tableName;

  @Nonnull private final List<String> columnsToDropNonNullability;
  @Nonnull private final List<String> columnsToAdd;

  public SchemaEvolutionTargetItems(
      String tableName, List<String> columnsToDropNonNullability, List<String> columnsToAdd) {
    this.tableName = tableName;
    this.columnsToDropNonNullability =
        columnsToDropNonNullability != null ? columnsToDropNonNullability : Collections.emptyList();
    this.columnsToAdd = columnsToAdd != null ? columnsToAdd : Collections.emptyList();
  }

  public boolean hasDataForSchemaEvolution() {
    return !columnsToDropNonNullability.isEmpty() || !columnsToAdd.isEmpty();
  }

  public SchemaEvolutionTargetItems(String tableName, List<String> columnsToAdd) {
    this(tableName, null, columnsToAdd);
  }

  public String getTableName() {
    return tableName;
  }

  @Nonnull
  public List<String> getColumnsToDropNonNullability() {
    return columnsToDropNonNullability;
  }

  @Nonnull
  public List<String> getColumnsToAdd() {
    return columnsToAdd;
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
