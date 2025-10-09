package com.snowflake.kafka.connector.internal.streaming.v2;

import com.snowflake.kafka.connector.Utils;
import java.util.Map;

import static com.snowflake.kafka.connector.Utils.isEnableAlteringPipesTables;

/** Class that generates pipe name for Snowpipe Streaming v2 */
public class PipeNameProvider {

  public static String pipeName(Map<String, String> connectorConfig, String table) {
      // this is special case senario
      // when the user has this option configured the user is expected to create the pipe and table before starting connector. When this property is set pipe name and table name are expected
      // to have the same name. This allows to use interactive tables as destination tables because interactive tables create the pipe with the same name as the table when user executes
      // create interactive table statement in SQL
    if (isEnableAlteringPipesTables(connectorConfig)) {
      return table;
    } else {
      return connectorConfig.get(Utils.NAME) + "_SSV2_PIPE_" + table;
    }
  }
}
