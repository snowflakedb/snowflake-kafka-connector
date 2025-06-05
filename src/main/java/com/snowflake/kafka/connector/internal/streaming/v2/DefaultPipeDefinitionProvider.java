package com.snowflake.kafka.connector.internal.streaming.v2;

/** Default implementation does not perform any transformations on pipe level */
public class DefaultPipeDefinitionProvider implements PipeDefinitionProvider {

  private static final String CREATE_PIPE_IF_NOT_EXISTS_STATEMENT =
      "CREATE PIPE IF NOT EXISTS identifier(?) AS COPY INTO %s FROM TABLE (DATA_SOURCE(TYPE =>"
          + " 'STREAMING')) MATCH_BY_COLUMN_NAME=CASE_SENSITIVE";

  private static final String CREATE_OR_REPLACE_PIPE_STATEMENT =
      "CREATE OR REPLACE PIPE identifier(?) AS COPY INTO %s FROM TABLE (DATA_SOURCE(TYPE =>"
          + " 'STREAMING')) MATCH_BY_COLUMN_NAME=CASE_SENSITIVE";

  @Override
  public String getPipeDefinition(String tableName, boolean recreate) {
    String sqlTemplate =
        recreate ? CREATE_OR_REPLACE_PIPE_STATEMENT : CREATE_PIPE_IF_NOT_EXISTS_STATEMENT;
    return String.format(sqlTemplate, tableName);
  }
}
