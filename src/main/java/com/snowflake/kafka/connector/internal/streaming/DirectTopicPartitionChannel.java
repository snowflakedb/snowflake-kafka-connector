package com.snowflake.kafka.connector.internal.streaming;

import com.snowflake.kafka.connector.internal.streaming.channel.TopicPartitionChannel;
import com.snowflake.kafka.connector.internal.streaming.telemetry.SnowflakeTelemetryChannelStatus;
import com.snowflake.kafka.connector.internal.telemetry.SnowflakeTelemetryService;
import java.util.concurrent.CompletableFuture;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import org.apache.kafka.connect.sink.SinkRecord;

public class DirectTopicPartitionChannel implements TopicPartitionChannel {
  @Override
  public long getOffsetPersistedInSnowflake() {
    return 0;
  }

  @Override
  public long getProcessedOffset() {
    return 0;
  }

  @Override
  public long getLatestConsumerOffset() {
    return 0;
  }

  @Override
  public boolean isPartitionBufferEmpty() {
    return false;
  }

  @Override
  public SnowflakeStreamingIngestChannel getChannel() {
    return null;
  }

  @Override
  public SnowflakeTelemetryService getTelemetryServiceV2() {
    return null;
  }

  @Override
  public SnowflakeTelemetryChannelStatus getSnowflakeTelemetryChannelStatus() {
    return null;
  }

  @Override
  public long fetchOffsetTokenWithRetry() {
    return 0;
  }

  @Override
  public void insertRecord(SinkRecord kafkaSinkRecord, boolean isFirstRowPerPartitionInBatch) {}

  @Override
  public long getOffsetSafeToCommitToKafka() {
    return 0;
  }

  @Override
  public void closeChannel() {}

  @Override
  public CompletableFuture<Void> closeChannelAsync() {
    return null;
  }

  @Override
  public boolean isChannelClosed() {
    return false;
  }

  @Override
  public String getChannelNameFormatV1() {
    return null;
  }

  @Override
  public void insertBufferedRecordsIfFlushTimeThresholdReached() {}

  @Override
  public void setLatestConsumerOffset(long consumerOffset) {}
}
