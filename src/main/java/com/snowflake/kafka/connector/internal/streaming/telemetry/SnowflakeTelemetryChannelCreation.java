package com.snowflake.kafka.connector.internal.streaming.telemetry;

import static com.snowflake.kafka.connector.internal.telemetry.TelemetryConstants.CHANNEL_NAME;
import static com.snowflake.kafka.connector.internal.telemetry.TelemetryConstants.FILE_COUNT_REPROCESS_PURGE;
import static com.snowflake.kafka.connector.internal.telemetry.TelemetryConstants.FILE_COUNT_RESTART;
import static com.snowflake.kafka.connector.internal.telemetry.TelemetryConstants.IS_REUSE_PIPE;
import static com.snowflake.kafka.connector.internal.telemetry.TelemetryConstants.IS_REUSE_STAGE;
import static com.snowflake.kafka.connector.internal.telemetry.TelemetryConstants.IS_REUSE_TABLE;
import static com.snowflake.kafka.connector.internal.telemetry.TelemetryConstants.PIPE_NAME;
import static com.snowflake.kafka.connector.internal.telemetry.TelemetryConstants.STAGE_NAME;
import static com.snowflake.kafka.connector.internal.telemetry.TelemetryConstants.START_TIME;
import static com.snowflake.kafka.connector.internal.telemetry.TelemetryConstants.TABLE_NAME;

import com.snowflake.kafka.connector.internal.telemetry.SnowflakeTelemetryBasicInfo;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.node.ObjectNode;

/**
 * This object is send only once when pipe starts No concurrent modification is made on this object,
 * thus no lock is required.
 */
public class SnowflakeTelemetryChannelCreation extends SnowflakeTelemetryBasicInfo {
  boolean isReuseTable = false; // is the create reusing existing table
  long startTime; // start time of the pipe
  private final String channelName;

  public SnowflakeTelemetryChannelCreation(
      final String tableName, String channelName) {
    super(tableName);
    this.channelName = channelName;
    this.startTime = System.currentTimeMillis();
  }

  @Override
  public void dumpTo(ObjectNode msg) {
    msg.put(TABLE_NAME, this.tableName);
    msg.put(CHANNEL_NAME, this.channelName);

    msg.put(IS_REUSE_TABLE, this.isReuseTable);
    msg.put(START_TIME, startTime);
  }

  @Override
  public boolean isEmpty() {
    throw new IllegalStateException(
        "Empty function doesnt apply to:" + this.getClass().getSimpleName());
  }

  public void setReuseTable(boolean reuseTable) {
    isReuseTable = reuseTable;
  }

  public void setStartTime(long startTime) {
    this.startTime = startTime;
  }
}
