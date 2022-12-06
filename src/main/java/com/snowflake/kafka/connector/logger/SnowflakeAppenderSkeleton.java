package com.snowflake.kafka.connector.logger;

import net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.spi.LoggingEvent;

import java.io.IOException;
import java.util.HashMap;

public class SnowflakeAppenderSkeleton extends AppenderSkeleton {

  @Override
  protected void append(LoggingEvent loggingEvent) {
    try {
      SnowflakeStreamingIngestChannel channel = SingletonStreamingClient.getChannel();
      String logLevel = loggingEvent.getLevel().toString();
      Object msg = loggingEvent.getMessage();
      HashMap<String, Object> logRow = new HashMap<>();
      logRow.put("LOG_LEVEL", logLevel);
      logRow.put("MESSAGE", msg);
      channel.insertRow(logRow, null);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() {
    // do nothing for now
  }

  @Override
  public boolean requiresLayout() {
    return false;
  }
}
