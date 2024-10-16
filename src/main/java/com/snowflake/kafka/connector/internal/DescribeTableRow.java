package com.snowflake.kafka.connector.internal;

import java.util.Objects;

/** Class representing a single row returned by describe table statement. */
public class DescribeTableRow {
  private final String column;
  private final String type;

  public DescribeTableRow(String column, String type) {
    this.column = column;
    this.type = type;
  }

  public String getColumn() {
    return column;
  }

  public String getType() {
    return type;
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
    return "DescribeTableRow{" + "column='" + column + '\'' + ", type='" + type + '\'' + '}';
  }
}
