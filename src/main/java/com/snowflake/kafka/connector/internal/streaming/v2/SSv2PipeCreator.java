package com.snowflake.kafka.connector.internal.streaming.v2;

import com.snowflake.kafka.connector.internal.SnowflakeConnectionService;

/** Creates pipe without any transformations */
public class SSv2PipeCreator {

  // the second parameter has to be string substituted because of the bug in the Snowflake
  // when second parameter is added as a bind variable it fails with SQL compilation error
  private static final String DEFAULT_CREATE_PIPE_IF_NOT_EXISTS_STATEMENT =
      "CREATE PIPE IF NOT EXISTS identifier(?) AS COPY INTO %s FROM TABLE (DATA_SOURCE(TYPE =>"
          + " 'STREAMING')) MATCH_BY_COLUMN_NAME=CASE_SENSITIVE";

  private final SnowflakeConnectionService conn;
  private final String pipeName;
  private final String tableName;
  private final String pipeDdlSql;

  public SSv2PipeCreator(SnowflakeConnectionService conn, String pipeName, String tableName) {
    this(conn, pipeName, tableName, DEFAULT_CREATE_PIPE_IF_NOT_EXISTS_STATEMENT);
  }

  public SSv2PipeCreator(
      SnowflakeConnectionService conn, String pipeName, String tableName, String pipeDdlSql) {
    this.conn = conn;
    this.pipeName = pipeName;
    this.tableName = tableName;
    this.pipeDdlSql = pipeDdlSql;
  }

  public void createPipeIfNotExists() {
    String createPipeSql = String.format(pipeDdlSql, tableName);
    conn.executeQueryWithParameters(createPipeSql, pipeName);
  }
}
