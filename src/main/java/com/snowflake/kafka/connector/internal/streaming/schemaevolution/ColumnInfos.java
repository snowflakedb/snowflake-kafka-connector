package com.snowflake.kafka.connector.internal.streaming.schemaevolution;

import java.util.Objects;
import java.util.Optional;

public class ColumnInfos {
  private final String columnType;
  private final String comments;

  public ColumnInfos(String columnType, String comments) {
    this.columnType = columnType;
    this.comments = comments;
  }

  public String getColumnType() {
    return columnType;
  }

  public String getComments() {
    return comments;
  }

  public String getDdlComments() {
    return Optional.ofNullable(comments)
        .map(comment -> String.format(" comment '%s' ", comment))
        .orElse(" comment 'column created by schema evolution from Snowflake Kafka Connector' ");
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ColumnInfos that = (ColumnInfos) o;
    return Objects.equals(columnType, that.columnType) && Objects.equals(comments, that.comments);
  }

  @Override
  public int hashCode() {
    return Objects.hash(columnType, comments);
  }
}
