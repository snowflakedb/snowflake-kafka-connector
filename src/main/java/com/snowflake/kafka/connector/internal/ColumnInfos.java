package com.snowflake.kafka.connector.internal;

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
}
