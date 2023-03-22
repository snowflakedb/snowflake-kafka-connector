package com.snowflake.kafka.connector.internal.streaming;

import com.snowflake.kafka.connector.internal.telemetry.SnowflakeTelemetryBasicInfo;
import com.snowflake.kafka.connector.internal.telemetry.SnowflakeTelemetryService;

/**
 * This is the implementation of Telemetry Service for Snowpipe Streaming. Sends data related to
 * Snowpipe Streaming to Snowflake for additional debugging purposes.
 */
public class SnowflakeTelemetryServiceV2 extends SnowflakeTelemetryService {
  @Override
  public void reportKafkaPartitionUsage(
      SnowflakeTelemetryBasicInfo partitionStatus, boolean isClosing) {}
}
