package com.snowflake.kafka.connector.internal.streaming.v2;

import com.snowflake.ingest.streaming.OpenChannelResult;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import java.util.Objects;

/**
 * Wrapper class for {@link SnowflakeStreamingIngestClient} that provides a controlled interface for
 * streaming operations while hiding lifecycle management from users.
 *
 * <p>The primary purpose of this wrapper is to prevent direct access to client lifecycle methods
 * such as {@code close()} and {@code isClosed()}. This ensures that the underlying client's
 * lifecycle is managed exclusively by the {@link StreamingIngestClientV2Provider}, preventing
 * premature closure or inconsistent state that could affect other consumers of the same client
 * instance.
 *
 * @see StreamingIngestClientV2Provider
 * @see SnowflakeStreamingIngestClient
 */
class StreamingIngestClientV2Wrapper {
  private final SnowflakeStreamingIngestClient client;

  /**
   * Constructs a new wrapper around the provided streaming client.
   *
   * @param client the underlying {@link SnowflakeStreamingIngestClient} to wrap. Must not be null.
   * @throws IllegalArgumentException if client is null
   */
  StreamingIngestClientV2Wrapper(SnowflakeStreamingIngestClient client) {
    if (client == null) {
      throw new IllegalArgumentException("Client cannot be null");
    }
    this.client = client;
  }

  /**
   * Opens a new streaming channel with the specified channel name and offset token.
   *
   * @param channelName the name of the channel to open
   * @param offsetToken the offset token for the channel
   * @return {@link OpenChannelResult} containing the result of the channel opening operation
   */
  OpenChannelResult openChannel(String channelName, String offsetToken) {
    return client.openChannel(channelName, offsetToken);
  }

  void initiateFlush() {
    client.initiateFlush();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    StreamingIngestClientV2Wrapper that = (StreamingIngestClientV2Wrapper) obj;
    return Objects.equals(client, that.client);
  }

  @Override
  public int hashCode() {
    return Objects.hash(client);
  }
}
