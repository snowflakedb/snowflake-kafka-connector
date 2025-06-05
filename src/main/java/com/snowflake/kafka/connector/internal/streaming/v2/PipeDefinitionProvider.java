package com.snowflake.kafka.connector.internal.streaming.v2;

/** Construct CREATE PIPE sql statement */
public interface PipeDefinitionProvider {
  String getPipeDefinition(String tableName, boolean recreate);
}
