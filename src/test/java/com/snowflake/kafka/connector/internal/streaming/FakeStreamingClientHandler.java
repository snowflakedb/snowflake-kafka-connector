package com.snowflake.kafka.connector.internal.streaming;

import net.snowflake.ingest.streaming.FakeSnowflakeStreamingIngestClient;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;

import java.util.UUID;

public class FakeStreamingClientHandler implements StreamingClientHandler {

    @Override
    public SnowflakeStreamingIngestClient createClient(StreamingClientProperties streamingClientProperties) {
        return new FakeSnowflakeStreamingIngestClient(streamingClientProperties.clientName+"_"+UUID.randomUUID());
    }

    @Override
    public void closeClient(SnowflakeStreamingIngestClient client) {
        try {
            client.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}