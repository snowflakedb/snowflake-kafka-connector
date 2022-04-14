package com.snowflake.kafka.connector.internal.streaming;

/**
 * Class for exceptions that occur while interacting with Snowflake through Snowpipe Streaming.
 *
 * <p>Please note: This exception is translated from SFException when Client SDK determines this is
 * an invalid insert Operation. (For instance, clientSequencer is bumped up, but we are still
 * calling from older clientSequencer number)
 *
 * <p>Use this exception when a particular channel (Topic Partition) fails to insert Rows into
 * Snowflake Table, in this case we will reopen the channel and try to insert same rows again.
 *
 * <p>(Note: This exception is not when Streaming Snowpipe API returns error in its response)
 */
public class TopicPartitionChannelInsertionException extends RuntimeException {
  public TopicPartitionChannelInsertionException(String msg, Throwable t) {
    super(msg, t);
  }
}
