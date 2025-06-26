package com.snowflake.kafka.connector.internal.streaming.v2;

import com.snowflake.kafka.connector.internal.SnowflakeConnectionService;

/** Creates pipe without any transformations */
public class SSv2PipeCreator {

  private static final String CREATE_PIPE_IF_NOT_EXISTS_STATEMENT =
      "CREATE PIPE IF NOT EXISTS identifier(?) AS COPY INTO %s FROM TABLE (DATA_SOURCE(TYPE =>"
          + " 'STREAMING')) MATCH_BY_COLUMN_NAME=CASE_SENSITIVE";

  private static final String CREATE_OR_REPLACE_PIPE_STATEMENT =
      "CREATE OR REPLACE PIPE identifier(?) AS COPY INTO %s FROM TABLE (DATA_SOURCE(TYPE =>"
          + " 'STREAMING')) MATCH_BY_COLUMN_NAME=CASE_SENSITIVE";

  private final SnowflakeConnectionService conn;
  private final String pipeName;
  private final String tableName;

  public SSv2PipeCreator(SnowflakeConnectionService conn, String pipeName, String tableName) {
    this.conn = conn;
    this.pipeName = pipeName;
    this.tableName = tableName;
  }

  public void createPipe(CreatePipeMode mode) {
    String sqlTemplate = getTemplate(mode);
    String createPipeSql = String.format(sqlTemplate, tableName);
    conn.executeQueryWithParameters(createPipeSql, pipeName);
  }

  private String getTemplate(CreatePipeMode mode) {
    switch (mode) {
      case CREATE_OR_REPLACE:
        return CREATE_OR_REPLACE_PIPE_STATEMENT;
      case CREATE_IF_NOT_EXIST:
        return CREATE_PIPE_IF_NOT_EXISTS_STATEMENT;
    }
    throw new IllegalStateException("Unexpected mode " + mode);
  }
}
