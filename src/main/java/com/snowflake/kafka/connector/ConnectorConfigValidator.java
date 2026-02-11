package com.snowflake.kafka.connector;

import java.util.Map;

public interface ConnectorConfigValidator {

  /**
   * Validate input configuration
   *
   * @param config configuration Map
   */
  void validateConfig(Map<String, String> config);
}
