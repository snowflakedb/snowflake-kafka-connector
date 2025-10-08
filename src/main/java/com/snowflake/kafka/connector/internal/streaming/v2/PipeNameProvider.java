package com.snowflake.kafka.connector.internal.streaming.v2;

import com.snowflake.kafka.connector.Utils;
import java.util.Map;

/** Class that generates pipe name for Snowpipe Streaming v2 */
public class PipeNameProvider {

  public static String pipeName(Map<String, String> connectorConfig, String table) {
    if (Utils.useInteractiveTable(connectorConfig)) {
      return table;
    } else {
      return connectorConfig.get(Utils.NAME) + "_SSV2_PIPE_" + table;
    }
  }
}
