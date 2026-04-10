package com.snowflake.kafka.connector.internal.streaming;

import com.google.common.collect.ImmutableMap;
import com.snowflake.kafka.connector.config.SinkTaskConfig;
import java.util.Map;

/** Validates connector config for Snowpipe Streaming */
// TODO (separate PR) - rename to ConfigValidator and return an ordinary Map
public interface StreamingConfigValidator {

  /**
   * @param parsedConfig typed config parsed from the raw map
   * @param rawConfig raw connector config, needed for format-level checks that can't use the typed
   *     config (e.g. strict boolean validation, converter class names)
   * @return map of invalid parameters
   */
  ImmutableMap<String, String> validate(SinkTaskConfig parsedConfig, Map<String, String> rawConfig);
}
