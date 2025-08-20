package com.snowflake.kafka.connector.internal.streaming.validation;

import java.util.HashMap;
import java.util.Map;

/** Manage state of RowSchema objects across tables */
public class RowSchemaManager {

  private final RowSchemaProvider provider;
  private final Map<String, RowSchema> schemaMap = new HashMap<>();

  public RowSchemaManager(RowSchemaProvider provider) {
    this.provider = provider;
  }

  /** Get existing schema or create if absent */
  public RowSchema get(String tableName, Map<String, String> connectorConfig) {
    return schemaMap.computeIfAbsent(
        tableName, k -> provider.getRowSchema(tableName, connectorConfig));
  }

  /** Remove schema for given table */
  public void invalidate(String tableName) {
    schemaMap.remove(tableName);
  }
}
