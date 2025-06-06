package com.snowflake.kafka.connector.internal.streaming.v2;

/** (Re)creates the pipe */
public interface SSv2PipeCreator {
  void createPipe(boolean recreate);
}
