package com.snowflake.kafka.connector.internal.streaming.v2;

class PipeNameProvider {

  // TODO - in Snowpipe we create a separate pipe for each partition... why?
  static String pipeName(String appName, String table) {
    return appName + "_SSV2_PIPE_" + table;
  }
}
