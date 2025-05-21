package com.snowflake.kafka.connector.internal.streaming.v2;

import com.snowflake.kafka.connector.internal.DescribeTableRow;
import java.util.List;

/** Construct CREATE PIPE sql statement based on the results of DESCRIBE TABLE statement */
public interface PipeDefinitionProvider {
  String getPipeDefinition(String tableName, List<DescribeTableRow> tableRows, boolean recreate);
}
