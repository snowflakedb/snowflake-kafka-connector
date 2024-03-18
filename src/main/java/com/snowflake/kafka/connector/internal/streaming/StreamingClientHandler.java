package com.snowflake.kafka.connector.internal.streaming;

import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;

public interface StreamingClientHandler {
    SnowflakeStreamingIngestClient createClient(
            StreamingClientProperties streamingClientProperties);

    void closeClient(SnowflakeStreamingIngestClient client);

    /**
     * Checks if a given client is valid (not null, open and has a name)
     *
     * @param client The client to validate
     * @return If the client is valid
     */
    public static boolean isClientValid(SnowflakeStreamingIngestClient client) {
        return client != null && !client.isClosed() && client.getName() != null;
    }
}
