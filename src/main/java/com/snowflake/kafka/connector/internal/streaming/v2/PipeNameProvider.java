package com.snowflake.kafka.connector.internal.streaming.v2;

class PipeNameProvider {

  static String pipeName(String appName, String table) {
    return appName + "_SSV2_PIPE_" + table;
  }
}
