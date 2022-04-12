package com.snowflake.kafka.connector.internal.streaming;

/**
 * Class for exceptions that occur while interacting with Snowflake through Streaming Snowpipe.
 *
 * <p>Use this exception when a particular channel (Topic Partition) fails to insert Rows into
 * Snowflake Table.
 *
 * <p>(Note: This exception is not when Streaming Snowpipe API returns error in its response)
 */
public class TopicPartitionChannelInsertionException extends RuntimeException {
  public TopicPartitionChannelInsertionException(String s) {
    super(s);
  }

  public TopicPartitionChannelInsertionException(String msg, Throwable t) {
    super(msg, t);
  }
}
