package com.snowflake.kafka.connector.internal;

public class SnowflakeTelemetryBasicInfo {
  String tableName;
  String stageName;
  String pipeName;
  static final String TABLE_NAME = "table_name";
  static final String STAGE_NAME = "stage_name";
  static final String PIPE_NAME  = "pipe_name";
}
