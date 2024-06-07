package com.snowflake.kafka.connector.internal.parameters;

import java.util.Map;
import java.util.Optional;

import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.SNOWPIPE_STREAMING_ENABLE_SINGLE_BUFFER;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.SNOWPIPE_STREAMING_ENABLE_SINGLE_BUFFER_DEFAULT;

/***
 * The helper class for checking parameters related to a internal (double) buffer.
 */
public class InternalBufferParameters {
    public static Boolean isSingleBufferEnabled(Map<String, String> connectorConfig) {
      return Optional.ofNullable(connectorConfig.get(SNOWPIPE_STREAMING_ENABLE_SINGLE_BUFFER))
              .map(Boolean::parseBoolean)
              .orElse(SNOWPIPE_STREAMING_ENABLE_SINGLE_BUFFER_DEFAULT);
    }
}
