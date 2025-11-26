package com.snowflake.kafka.connector.internal.streaming.v2;

import static com.snowflake.kafka.connector.Constants.DEFAULT_PIPE_NAME_SUFFIX;

/** Class that generates pipe name for Snowpipe Streaming v2 */
public final class PipeNameProvider {

  public static String buildPipeName(String table) {
    return table;
  }

  public static String buildDefaultPipeName(String table) {
    return table + DEFAULT_PIPE_NAME_SUFFIX;
  }
}
