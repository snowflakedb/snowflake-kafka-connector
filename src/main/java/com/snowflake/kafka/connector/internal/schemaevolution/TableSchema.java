/*
 * Copyright (c) 2026 Snowflake Computing Inc. All rights reserved.
 *
 * Ported from KC v3.2 for client-side schema evolution in KC v4.
 */

package com.snowflake.kafka.connector.internal.schemaevolution;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * Wrapper around Map of column name to ColumnInfos.
 */
public class TableSchema {
  private final Map<String, ColumnInfos> columnInfos;

  public TableSchema(Map<String, ColumnInfos> columnInfos) {
    this.columnInfos = columnInfos;
  }

  public Map<String, ColumnInfos> getColumnInfos() {
    return Collections.unmodifiableMap(columnInfos);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    TableSchema that = (TableSchema) o;
    return Objects.equals(columnInfos, that.columnInfos);
  }

  @Override
  public int hashCode() {
    return Objects.hash(columnInfos);
  }

  @Override
  public String toString() {
    return "TableSchema{" + "columnInfos=" + columnInfos + '}';
  }
}
