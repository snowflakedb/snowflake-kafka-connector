package com.snowflake.kafka.connector.internal.streaming.v2;

import com.snowflake.kafka.connector.internal.SnowflakeConnectionService;

/** Default implementation does not perform any transformations on pipe level */
class DefaultSSv2PipeCreator implements SSv2PipeCreator {

  private static final String CREATE_PIPE_IF_NOT_EXISTS_STATEMENT =
      "CREATE PIPE IF NOT EXISTS identifier(?) AS COPY INTO %s FROM TABLE (DATA_SOURCE(TYPE =>"
          + " 'STREAMING')) MATCH_BY_COLUMN_NAME=CASE_SENSITIVE";

  private static final String CREATE_OR_REPLACE_PIPE_STATEMENT =
      "CREATE OR REPLACE PIPE identifier(?) AS COPY INTO %s FROM TABLE (DATA_SOURCE(TYPE =>"
          + " 'STREAMING')) MATCH_BY_COLUMN_NAME=CASE_SENSITIVE";

  private final SnowflakeConnectionService conn;
  private final String pipeName;
  private final String tableName;

  DefaultSSv2PipeCreator(SnowflakeConnectionService conn, String pipeName, String tableName) {
    this.conn = conn;
    this.pipeName = pipeName;
    this.tableName = tableName;
  }

  @Override
  public void createPipe(boolean recreate) {
    String sqlTemplate =
        recreate ? CREATE_OR_REPLACE_PIPE_STATEMENT : CREATE_PIPE_IF_NOT_EXISTS_STATEMENT;
    String createPipeSql = String.format(sqlTemplate, tableName);
    conn.executeQueryWithParameter(createPipeSql, pipeName);
  }
}
