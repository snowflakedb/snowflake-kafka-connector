package com.snowflake.kafka.connector.internal.streaming;

import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;

/**
 * Interface for managing the streaming ingestion client ({@link
 * #createClient(StreamingClientProperties)}, {@link #closeClient(SnowflakeStreamingIngestClient)})
 * as well as validating the previously created client ({@link
 * #isClientValid(SnowflakeStreamingIngestClient)})
 */
public interface StreamingClientHandler {
  /**
   * Creates a streaming client from the given properties
   *
   * @param streamingClientProperties The properties to create the client
   * @return A newly created client
   */
  SnowflakeStreamingIngestClient createClient(StreamingClientProperties streamingClientProperties);

  /**
   * Closes the given client. Swallows any exceptions
   *
   * @param client The client to be closed
   */
  void closeClient(SnowflakeStreamingIngestClient client);

  /**
   * Checks if a given client is valid (not null, open and has a name)
   *
   * @param client The client to validate
   * @return If the client is valid
   */
  static boolean isClientValid(SnowflakeStreamingIngestClient client) {
    return client != null && !client.isClosed() && client.getName() != null;
  }
}
