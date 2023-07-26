package com.snowflake.kafka.connector.internal.streaming;

import static com.snowflake.kafka.connector.internal.telemetry.TelemetryConstants.CHANNEL_NAME;
import static com.snowflake.kafka.connector.internal.telemetry.TelemetryConstants.IS_REUSE_TABLE;
import static com.snowflake.kafka.connector.internal.telemetry.TelemetryConstants.PARTITION;
import static com.snowflake.kafka.connector.internal.telemetry.TelemetryConstants.START_TIME;
import static com.snowflake.kafka.connector.internal.telemetry.TelemetryConstants.TOPIC_NAME;

import com.snowflake.kafka.connector.internal.telemetry.SnowflakeTelemetryBasicInfo;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.node.ObjectNode;

/**
 * This object is send only once when pipe starts No concurrent modification is made on this object,
 * thus no lock is required.
 */
public class SnowflakeTelemetryChannelCreation extends SnowflakeTelemetryBasicInfo {
  private final String channelName;
  private final String topicName;
  private final int partition;
  long startTime; // start time of the pipe
  boolean isReuseTable = false; // is the create reusing existing table

  public SnowflakeTelemetryChannelCreation(
      final String tableName,
      final String topicName,
      final int partition,
      final String channelName) {
    super(tableName);
    this.topicName = topicName;
    this.partition = partition;
    this.channelName = channelName;

    this.startTime = System.currentTimeMillis();
  }

  @Override
  public void dumpTo(ObjectNode msg) {
    msg.put(TOPIC_NAME, this.topicName);
    msg.put(PARTITION, this.partition);
    msg.put(CHANNEL_NAME, this.channelName);

    msg.put(IS_REUSE_TABLE, isReuseTable);
    msg.put(START_TIME, startTime);
  }

  @Override
  public boolean isEmpty() {
    throw new IllegalStateException(
        "Empty function doesnt apply to:" + this.getClass().getSimpleName());
  }
}
