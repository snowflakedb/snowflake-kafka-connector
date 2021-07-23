package com.snowflake.kafka.connector.internal;

import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class SnowflakeTelemetryBasicInfo {
  final String tableName;
  final String stageName;
  final String pipeName;
  static final String TABLE_NAME = "table_name";
  static final String STAGE_NAME = "stage_name";
  static final String PIPE_NAME = "pipe_name";

  static final Logger LOGGER = LoggerFactory.getLogger(SnowflakeTelemetryBasicInfo.class);

  SnowflakeTelemetryBasicInfo(
      final String tableName, final String stageName, final String pipeName) {
    this.tableName = tableName;
    this.stageName = stageName;
    this.pipeName = pipeName;
  }

  abstract void dumpTo(ObjectNode msg);
}
