package com.snowflake.kafka.connector.internal;

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
}
