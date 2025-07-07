package com.snowflake.kafka.connector.internal.streaming.v2;

/** Class that generates pipe name for Snowpipe Streaming v2 */
public class PipeNameProvider {

  public static String pipeName(String appName, String table) {
    return appName + "_SSV2_PIPE_" + table;
  }
}
