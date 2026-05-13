package com.snowflake.kafka.connector.internal.streaming.v2.client;

import com.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestClientFactory;
import com.snowflake.kafka.connector.config.SinkTaskConfig;
import com.snowflake.kafka.connector.internal.streaming.StreamingClientProperties;
import java.util.concurrent.atomic.AtomicInteger;

/** Factory for creating Snowpipe Streaming clients. Shared by all connectors. */
public class StreamingClientFactory {

  // Supplier reference is here so that we can swap it to mocked one in the tests
  private static volatile StreamingClientSupplier ingestClientSupplier =
      new StreamingClientSupplierImpl();

  private static final AtomicInteger createdClientId = new AtomicInteger(0);

  /** Sets a custom ingest client supplier. This method is used in tests only. */
  public static void setStreamingClientSupplier(final StreamingClientSupplier supplier) {
    ingestClientSupplier = supplier;
  }

  /** Resets the ingest client supplier to default. This method is used in tests only. */
  public static void resetStreamingClientSupplier() {
    ingestClientSupplier = new StreamingClientSupplierImpl();
  }

  static SnowflakeStreamingIngestClient createClient(
      final String pipeName,
      final SinkTaskConfig config,
      final StreamingClientProperties streamingClientProperties) {

    String clientName = clientName(streamingClientProperties);
    String dbName = config.getSnowflakeDatabase();
    String schemaName = config.getSnowflakeSchema();

    return ingestClientSupplier.get(
        clientName, dbName, schemaName, pipeName, streamingClientProperties);
  }

  private static String clientName(final StreamingClientProperties streamingClientProperties) {
    return streamingClientProperties.clientNamePrefix + createdClientId.incrementAndGet();
  }

  static final class StreamingClientSupplierImpl implements StreamingClientSupplier {
    @Override
    public SnowflakeStreamingIngestClient get(
        final String clientName,
        final String dbName,
        final String schemaName,
        final String pipeName,
        final StreamingClientProperties streamingClientProperties) {

      // Quote the pipe name to handle lowercase / special characters in the name.
      return SnowflakeStreamingIngestClientFactory.builder(
              clientName, dbName, schemaName, '"' + pipeName + '"')
          .setProperties(streamingClientProperties.clientProperties)
          .setParameterOverrides(streamingClientProperties.parameterOverrides)
          .build();
    }
  }
}
