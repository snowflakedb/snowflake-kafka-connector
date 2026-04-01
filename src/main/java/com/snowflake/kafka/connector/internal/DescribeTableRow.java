package com.snowflake.kafka.connector.internal;

import java.util.Objects;

/** Class representing a single row returned by describe table statement. */
public class DescribeTableRow {
  private final String column;
  private final String type;
  private final String comment;
  private final String nullable;
  private final String defaultValue;
  private final String autoincrement;

  /** Full constructor with default and autoincrement metadata. */
  public DescribeTableRow(
      String column,
      String type,
      String comment,
      String nullable,
      String defaultValue,
      String autoincrement) {
    this.column = column;
    this.type = type;
    this.comment = comment;
    this.nullable = nullable;
    this.defaultValue = defaultValue;
    this.autoincrement = autoincrement;
  }

  /** Backward-compatible constructor (no default/autoincrement metadata). */
  public DescribeTableRow(String column, String type, String comment, String nullable) {
    this(column, type, comment, nullable, null, null);
  }

  public String getColumn() {
    return column;
  }

  public String getType() {
    return type;
  }

  public String getComment() {
    return comment;
  }

  public String getNullable() {
    return nullable;
  }

  public String getDefaultValue() {
    return defaultValue;
  }

  public String getAutoincrement() {
    return autoincrement;
  }

  /** True when the column has a server-assigned default value. */
  public boolean hasDefault() {
    return defaultValue != null && !defaultValue.isEmpty();
  }

  /** True when the column is an autoincrement/identity column. */
  public boolean isAutoincrement() {
    return autoincrement != null && !autoincrement.isEmpty();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    DescribeTableRow that = (DescribeTableRow) o;
    return Objects.equals(column, that.column) && Objects.equals(type, that.type);
  }

  @Override
  public int hashCode() {
    return Objects.hash(column, type);
  }

  @Override
  public String toString() {
    return " " + column + " " + type;
  }
}
