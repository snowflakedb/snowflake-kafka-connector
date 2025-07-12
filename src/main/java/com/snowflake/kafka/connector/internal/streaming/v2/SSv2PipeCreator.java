package com.snowflake.kafka.connector.internal.streaming.v2;

import com.snowflake.kafka.connector.internal.SnowflakeConnectionService;

/** Creates pipe without any transformations */
public class SSv2PipeCreator {

  private static final String CREATE_PIPE_IF_NOT_EXISTS_STATEMENT =
      "CREATE PIPE IF NOT EXISTS identifier(?) AS COPY INTO %s FROM TABLE (DATA_SOURCE(TYPE =>"
          + " 'STREAMING')) MATCH_BY_COLUMN_NAME=CASE_SENSITIVE";

  private final SnowflakeConnectionService conn;
  private final String pipeName;
  private final String tableName;

  public SSv2PipeCreator(SnowflakeConnectionService conn, String pipeName, String tableName) {
    this.conn = conn;
    this.pipeName = pipeName;
    this.tableName = tableName;
  }

  public void createPipeIfNotExists() {
    String createPipeSql = String.format(CREATE_PIPE_IF_NOT_EXISTS_STATEMENT, tableName);
    conn.executeQueryWithParameters(createPipeSql, pipeName);
  }
}
