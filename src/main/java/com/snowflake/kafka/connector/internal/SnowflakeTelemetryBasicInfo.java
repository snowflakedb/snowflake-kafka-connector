package com.snowflake.kafka.connector.internal;

import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.node.ObjectNode;

public abstract class SnowflakeTelemetryBasicInfo {
  final String tableName;
  final String stageName;
  final String pipeName;
  static final String TABLE_NAME = "table_name";
  static final String STAGE_NAME = "stage_name";
  static final String PIPE_NAME  = "pipe_name";

  SnowflakeTelemetryBasicInfo(final String tableName, final String stageName, final String pipeName)
  {
    this.tableName = tableName;
    this.stageName = stageName;
    this.pipeName = pipeName;
  }

  abstract void dumpTo(ObjectNode msg);
}
