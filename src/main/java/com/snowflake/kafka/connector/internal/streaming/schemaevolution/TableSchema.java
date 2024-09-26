package com.snowflake.kafka.connector.internal.streaming.schemaevolution;

import java.util.Map;
import java.util.Objects;

public class TableSchema {
  private final Map<String, ColumnInfos> columnInfos;

  public TableSchema(Map<String, ColumnInfos> columnInfos) {
    this.columnInfos = columnInfos;
  }

  public Map<String, ColumnInfos> getColumnInfos() {
    return columnInfos;
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
