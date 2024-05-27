package com.snowflake.kafka.connector.internal.streaming.channel;

import com.google.common.annotations.VisibleForTesting;
import com.snowflake.kafka.connector.internal.streaming.telemetry.SnowflakeTelemetryChannelStatus;
import com.snowflake.kafka.connector.internal.telemetry.SnowflakeTelemetryService;
import dev.failsafe.Fallback;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;

public interface ExposingInternalsTopicPartitionChannel {

  @VisibleForTesting
  long getOffsetPersistedInSnowflake();

  @VisibleForTesting
  long getProcessedOffset();

  @VisibleForTesting
  long getLatestConsumerOffset();

  @VisibleForTesting
  boolean isPartitionBufferEmpty();

  @VisibleForTesting
  SnowflakeStreamingIngestChannel getChannel();

  @VisibleForTesting
  SnowflakeTelemetryService getTelemetryServiceV2();

  @VisibleForTesting
  SnowflakeTelemetryChannelStatus getSnowflakeTelemetryChannelStatus();

  /**
   * Fetches the offset token from Snowflake.
   *
   * <p>It uses <a href="https://github.com/failsafe-lib/failsafe">Failsafe library </a> which
   * implements retries, fallbacks and circuit breaker.
   *
   * <p>Here is how Failsafe is implemented.
   *
   * <p>Fetches offsetToken from Snowflake (Streaming API)
   *
   * <p>If it returns a valid offset number, that number is returned back to caller.
   *
   * <p>If {@link net.snowflake.ingest.utils.SFException} is thrown, we will retry for max 3 times.
   * (Including the original try)
   *
   * <p>Upon reaching the limit of maxRetries, we will {@link Fallback} to opening a channel and
   * fetching offsetToken again.
   *
   * <p>Please note, upon executing fallback, we might throw an exception too. However, in that case
   * we will not retry.
   *
   * @return long offset token present in snowflake for this channel/partition.
   */
  @VisibleForTesting
  long fetchOffsetTokenWithRetry();
}
