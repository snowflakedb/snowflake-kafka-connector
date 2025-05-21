package com.snowflake.kafka.connector.internal.streaming.v2;

import com.snowflake.kafka.connector.internal.DescribeTableRow;
import java.util.List;
import java.util.stream.Collectors;

public class SnowflakePipeDefinitionProvider implements PipeDefinitionProvider {

  private static final String CREATE_OR_REPLACE_PIPE_STATEMENT =
      "CREATE OR REPLACE PIPE identifier(?) AS COPY INTO %s FROM (SELECT %s FROM TABLE"
          + " (DATA_SOURCE(TYPE => 'STREAMING')))";

  private static final String CREATE_PIPE_IF_NOT_EXISTS_STATEMENT =
      "CREATE PIPE IF NOT EXISTS identifier(?) AS COPY INTO %s FROM (SELECT %s FROM TABLE"
          + " (DATA_SOURCE(TYPE => 'STREAMING')))";

  @Override
  public String getPipeDefinition(
      String tableName, List<DescribeTableRow> tableRows, boolean recreate) {
    String columnsDefinition = getColumnsDefinition(tableRows);
    String sqlTemplate =
        recreate ? CREATE_OR_REPLACE_PIPE_STATEMENT : CREATE_PIPE_IF_NOT_EXISTS_STATEMENT;
    return String.format(sqlTemplate, tableName, columnsDefinition);
  }

  private String getColumnsDefinition(List<DescribeTableRow> tableRows) {
    return tableRows.stream().map(this::columnDefinition).collect(Collectors.joining(", "));
  }

  private String columnDefinition(DescribeTableRow row) {
    switch (row.getType()) {
      case "VARIANT":
      case "ARRAY":
        return String.format("PARSE_JSON($1:%s)", row.getColumn());
      default:
        return String.format("$1:%s AS %s", row.getColumn(), row.getColumn());
    }
  }
}
