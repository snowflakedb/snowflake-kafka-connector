package com.snowflake.kafka.connector.internal.streaming;

import com.google.common.collect.ImmutableMap;
import java.util.Map;

/** Validates connector config for Snowpipe Streaming */
public interface StreamingConfigValidator {

  /**
   * @param inputConfig connector provided by user
   * @return map of invalid parameters
   */
  ImmutableMap<String, String> validate(final Map<String, String> inputConfig);
}
