package com.snowflake.kafka.connector.internal.streaming;

import java.util.Locale;

/**
 * Enum representing the ingestion method for Snowflake Kafka Connector.
 *
 * <p>Only SNOWPIPE_STREAMING is supported (SSv2). Legacy SNOWPIPE and SSv1 have been removed.
 */
public enum IngestionMethodConfig {

  /* Snowpipe streaming (SSv2) - the only supported ingestion method */
  SNOWPIPE_STREAMING;

  @Override
  public String toString() {
    return name().toLowerCase(Locale.ROOT);
  }
}
