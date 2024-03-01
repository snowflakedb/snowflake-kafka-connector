package com.snowflake.kafka.connector.internal.streaming;

import com.snowflake.kafka.connector.internal.BufferThreshold;

/**
 * Helper class Associated to Streaming Snowpipe runtime of Kafka Connect which can help to identify
 * if there is a need to flush the buffered records.
 *
 * <p>Please note: Flush entails invoking {@link
 * net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel#insertRows(Iterable, String,
 * String)} API with suitable buffered rows.
 */
public class StreamingBufferThreshold extends BufferThreshold {
  public StreamingBufferThreshold(
      long flushTimeThresholdSeconds,
      long bufferSizeThresholdBytes,
      long bufferKafkaRecordCountThreshold) {
    super(
        IngestionMethodConfig.SNOWPIPE_STREAMING,
        flushTimeThresholdSeconds,
        bufferSizeThresholdBytes,
        bufferKafkaRecordCountThreshold);
  }
}
