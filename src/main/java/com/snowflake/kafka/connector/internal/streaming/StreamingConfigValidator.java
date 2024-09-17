package com.snowflake.kafka.connector.internal.streaming;

import com.google.common.collect.ImmutableMap;
import java.util.Map;

/** Validates connector config for Snowpipe Streaming */
// TODO (separate PR) - rename to ConfigValidator and return an ordinary Map
public interface StreamingConfigValidator {

  /**
   * @param inputConfig connector provided by user
   * @return map of invalid parameters
   */
  ImmutableMap<String, String> validate(final Map<String, String> inputConfig);
}
